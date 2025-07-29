ForL0 State Backend设计说明书

# 1.  整体架构设计

ForL0 StateBackend设计的目标是充分利用鲲鹏CPU服务器提供的L0 Cache特性，优化Flink状态访问的性能与效率。整体采用两级索引结构，其中顶层索引存放于L0 Cache中，实现高频访问的快速响应；主索引位于传统DRAM中，负责管理全量状态数据与解决冲突。此架构配备专门的Checkpoint接口模块，确保状态数据的一致性与快速恢复能力。此外，还设计了专门的容错机制适配层，使得本方案能对接Flink原生的Checkpoint和Savepoint机制，达到高兼容性与易部署性。

系统架构采用分层设计，分为 L0 Table（Cache加速区）和 Main Table（主状态表）两大核心部分，并配合内存分配管理与负载管理等辅助组件。其结构如图1所示：


## 1.1 L0 Table

L0 Table用于缓存热点key的索引信息，其常驻于L0 Cache，大小受用户可配置的L0容量预算限制，使热点状态能以极低的延迟访问。L0 Table由多个哈希桶组成，每个桶为一个Cache行大小（64B）并包含4个slot，每个slot存储以下字段：

**Tag**（2B）：对应key的摘要，用于快速比对；

**Valid**（1B）：用于标记该slot信息是否有效；

**Extension**（5B）：用于对齐16B，也可留作扩展字段使用（如LRU位等）；

**Pointer**（8B）：指向实际键值对的地址；

L0 Table 所占用的内存由专用的 L0 Memory Manager 管理，负载和热点感知由 L0 Load Manager 实现，动态提升热点Key。

## 1.2 Main Table

Main Table是经过缓存友好设计的哈希结构，其存储全量状态数据的索引，支持大容量、高负载因子的键值存储。其每个桶为64B对齐的Pointer Set，支持局部子桶分裂。键值分离设计，索引指向堆外的Entry数据块。Main Table的每个哈希桶包含6个slot，拥有以下字段：

**Pointer**（8B）：指向实际键值对的地址；

**Tag**（2B）：对应key的摘要，用于快速比对；

**Extension Pointer**（4B）：4个用于树型扩展的指针，每个指针为扩展桶的下标，最多支持扩展255个桶；

Main Table所使用的堆外内存由运行时注入的Flink Memory Manager进行分配。

## 1.3内存负载与管理

L0 Table和 Main Table分别有独立的内存管理组件，其中Main Table使用的普通堆外内存采用Flink Memory Manager进行分配和资源约束，并使用Unsafe进行读写；而L0 Table使用的L0映射区内存采用JNI封装后的系统调用接口进行分配和管理。Entry Arena 管理所有key-value对的物理存储，采用大块分配与空闲列表等机制高效利用堆外内存。

## 1.4组件解偶与集成

L0 Table作为Main Table的缓存和主表逻辑解耦，负载和cache管理由独立Manager负责，因此L0 Table可以作为一个可选的性能优化配置项向用户提供。而整个ForL0 State Backend则以插件方式对接Flink，导入Jar包后只需在配置文件中修改配置即可，无需修改用户代码。

 

# 2.  架构实现细节

ForL0 State Backend基于Flink原生HashMapStateBackend架构，在保留原有核心接口和部分可复用组件的基础上，引入了多个新增组件，以支持L0 Table热点缓存与分层哈希索引结构。这些新增组件与原有Flink StateBackend紧密协作，既保持了API兼容性，又实现了存储结构和缓存策略的深度优化。其组件关系如图2所示。


## 2.1 可复用组件与须修改组件

在ForL0 State Backend中，以下 Flink 原生组件可直接复用。

### 2.1.2 StateTable / StateMap

提供 StateBackend 对外的统一接口，封装底层索引实现，支持 Checkpoint/Restore 流程。

### 2.1.3 PriorityQueueManager

用于管理定时器状态，保持原有功能，不影响 L0 Table 设计。

### 2.1.4 Snapshot & Restore

负责状态快照与恢复，ForL0 在此流程中接入自定义的 StateMap 数据结构进行持久化。

### 2.1.5 ForL0KeyedStateBackend

在 Flink 原有 KeyedStateBackend 基础上扩展，负责初始化 ForL0StateTable、分配 L0 Table 内存、启动 L0 管理器等。同时在 Snapshot/Restore 流程中对接 ForL0 特有的索引结构序列化逻辑。

### 2.1.5 ForL0State Implementations

在原有 State 接口基础上提供 ForL0 版本的实现类，使得 Flink State API 能调用到 L0 Table 加速的底层实现。

以上这些组件无需大幅修改，只需在调用链中接入ForL0特有的相关实现。

## 2.2 新增组件

### 2.2.3 ForL0StateMap

ForL0StateMap是ForL0 State Backend的核心数据结构，替代了原有的 CopyOnWriteStateMap，实现了双层索引表结构。上层L0 Table，用于缓存热点key 的索引信息，保证 CPU Cache 命中率；下层是基于 缓存友好性设计的主表（分层哈希 + 树型扩展），存储所有状态的权威索引。所有数据均存储于堆外内存，索引部分使用固定大小的bucket结构，KV数据使用变长布局存储。具体实现见第三章

该组件负责所有状态的增删改查逻辑，并与 L0 Table 保持最终一致性。

### 2.3 Memory Allocator

专用的堆外内存分配器，负责为主表、扩展bucket 池以及 KV 存储区分配和释放内存块。所有内存均通过Flink的MemoryManager进行大块分配，并保证64B对齐。该组件为 ForL0StateMap提供统一的内存管理接口，在扩容、回收以及写时复制阶段协调不同内存区域的生命周期。

### 2.4 Level Hash Table

ForL0StateMap所使用的索引结构与KV存储所对应的具体实现，该模块直接服务于 ForL0StateMap 的查找、插入和删除等操作。

### 2.5 L0 Manager

L0 Table 内存分配与管理模块，负责根据总的 L0 内存配额（由配置文件指定）为 L0 Table 分配内存。在运行中，L0 Manager负责控制L0 Table 的容量、桶数等，确保热点索引能够在分配的内存限额内高效存储，并与State Estimator进行协作。

### 2.6 State Estimator

状态大小预测模块，主要用于辅助L0 Manager制定内存分配策略。

该模块会结合key的访问频率、状态大小等信息，预估未来一段时间内L0 Table的容量需求，并向L0 Manager提供参考。这样可以在不超出总内存限额的前提下，使更多的热点状态驻留于L0 Table，从而提升缓存命中率。

### 2.7 L0 Table Evictor

L0 Table淘汰策略执行模块，实现具体的slot替换算法。

该模块根据 L0 Manager 的指令，选择要淘汰的缓存条目，并在 O(1) 或近似常数时间内完成替换。可支持 LRU、LFU 或混合等替换策略，确保热点数据优先保留，减少性能波动。

# 3.  State Map设计

ForL0 State Backend 的核心在于其分层索引结构——ForL0StateMap。ForL0StateMap以极致 cache 友好为目标，分为 L0 Table（热点缓存层）和 Main Table（主表）两层。所有索引结构和 key-value 数据均存储于堆外内存，并以 64 字节 cache line 对齐设计，实现极低延迟和高吞吐。

## 3.1 结构概述

State Map由 L0 Table、 Main Table以及Entry Arena组成：

**L0 Table**：负责缓存最活跃的热点 key，保证极低延迟访问。其容量、对齐和结构可动态调优，全部存储于一块连续的堆外内存中。

**Main Table**：负责存储全部 key 的权威索引，采用 Cavast 提出的多 slot + 局部树型扩展结构，实现高负载、高冲突情况下的稳定高性能。

**KV****存储区（Entry Arena****）**：所有实际的 key-value 数据存储于专门的堆外内存池中，支持变长/定长 KV 对。

## 3.2 L0 Table 设计


如图3所示，L0 Table 是热点 key 的高速缓存层。其实现和内存布局如下：

l **桶结构**：每个bucket为 64 字节，包含4个slot，每个slot包含tag、有效位、主表指针、以及预留扩展字段（如 LRU/LFU/版本等）；

l **物理分布**：所有 L0 Table 内存块一次性分配，64B 对齐，便于批量加载；

l **管理策略**：热点key会被动态提升至L0 Table。插入、更新、查询均会刷新L0 Table，对应slot的选择和替换由可配置的替换算法决定；

l **一致性维护**：L0 Table 仅作缓存，主表持有全量状态索引，写入或扩容等变更后，相关 L0 条目会自动失效或被刷新。

 

表 1 L0 Table Slot数据结构

| 字段      | 说明                     | 字节数(Byte) |
| --------- | ------------------------ | ------------ |
| Tag       | Key的哈希摘要            | 2            |
| Valid     | 有效位                   | 1            |
| Extension | 扩展位/LRU/版本/字节对齐 | 5            |
| Pointer   | 指向KV项的指针           | 8            |

L0 Table为L0区域的桶数组，每个L0桶为64B，包含4个slot，扩展字段部分可灵活利用。

## 3.3 Main Table 设计


如图4所示，Main Table采用缓存友好设计的分层扩展哈希表：

l 每个主表桶（bucket）为64字节，包含6个slot和4个子桶指针（subPtr）。

l slot保存 key 的 tag 及指向 KV 区的指针，所有 slot 均 cache line 对齐。

l 当主表桶满时，根据 tag 低两位选择一个扩展指针（subPtr），分配新的子 bucket 并递归插入，实现局部树型扩展，有效避免全表链表冲突问题。

l 扩展池管理每主表最多支持255个子 bucket，下标存储于4B的 subPtr 字段，最大化空间利用；

其具体数据结构由表2和表3给出：

 

表 2 Main Table Slot数据结构

| 字段    | 说明           | 字节数(Byte) |
| ------- | -------------- | ------------ |
| Tag     | Key的哈希摘要  | 2            |
| Pointer | 指向KV项的指针 | 8            |

 

表 3 Main Table Bucket数据结构

| 字段     | 说明                          | 字节数(Byte) |
| -------- | ----------------------------- | ------------ |
| slot[]   | Main Table Slot数组，共6个    | 60           |
| subPtr[] | 用于解决冲突的扩展指针，共4个 | 4            |

 

## 3.4 KV 存储区（Entry Arena）

Entry Arena负责所有key-value对的实际存储。采用slab大块分配与空闲链表管理，既保证大吞吐分配，也降低碎片化。

l 结构设计：每个 KVNode 包含 keyLen、valueLen、key数据、value数据等元信息。支持变长 key/value；

l 分配策略：按大块分配，写入新 KVNode 时顺序分配；删除时，KVNode 空间回收到 freelist，支持快速重用；

l 对齐和效率：分配、释放均采用 8B 或16B 对齐，提升内存访问效率；

l 内存池：所有数据均在 Flink MemoryManager 分配的大块堆外内存中，无需频繁native调用。

KVNode结构由表4所示。

表 4 KVNode数据结构

| 字段         | 说明                | 字节数(Byte) |
| ------------ | ------------------- | ------------ |
| keyLen       | key长度             | 4B           |
| namespaceLen | namespace长度       | 4B           |
| valueLen     | value长度           | 4B           |
| key          | 序列化后的key       | keyLen       |
| namespace    | 序列化后的namespace | namespaceLen |
| value        | 序列化后的value     | valueLen     |

 

## 3.5 主要操作流程

**插入（Put）**

写入流程总是先写主表，再同步到L0 Table（由可配置替换算法决定）。其流程如下：

1. 计算key的tag和主表bucket下标。
2. 读取主表bucket，遍历每个slot：

a)    若slot有效且tag匹配，取指针指向的KVNode，比较key数据是否一致；

​               i.      若一致，直接覆盖value数据，提升该slot到L0 Table，流程结束；

​               ii.      若不一致，继续遍历。

b)    若slot空闲，则：

​               i.      分配新的KVNode内存，写入key/value；

​               ii.      将slot的tag、指针等信息设置为新值；

​              iii.      将该key/tag/ptr插入到L0 Table，流程结束。

3. 若所有slot已占用，计算扩展bucket下标（tag & 0x3）：

a)    若当前subPtr未分配扩展bucket，则在扩展池中分配新bucket，并写入subPtr；

b)    递归步骤2对该扩展bucket进行插入操作。

4. 插入/更新后可按策略将key提升为 L0 热点。

 

 

**查询（Get）**

查询流程总是优先查 L0 Table，未命中则回落主表。其流程如下：

1. 计算key的tag（hash 高位）和L0 bucket下标。
2. 读取L0 Table中对应bucket，遍历其中每个slot：

a)    若slot有效且tag匹配，则取出slot指针指向的KVNode；

b)    比较KVNode的key数据是否与查询key完全一致；

​               i.      若一致，直接返回该KVNode的value数据，流程结束；

​               ii.      若不一致，继续检查下一个slot。

3. 若L0 Table未命中，则计算Main Table的bucket下标。
4. 读取主表对应bucket，遍历bucket的每个slot：

a)    若slot有效且tag匹配，则取出slot指针指向的KVNode，比较key数据是否一致；

​               i.      若一致，将该项目插入或提升到L0 Table，并返回value数据，流程结束；

​               ii.      若不一致，继续遍历。

5. 若主表bucket所有slot均未命中，计算扩展bucket下标（tag & 0x3），

a)    如果对应subPtr存在扩展bucket，则递归步骤4对扩展bucket重复上述操作。

6. 若所有主表与扩展bucket均未命中，返回未找到。

 

**删除（Delete）**

删除流程同样以主表为准，其流程如下：

1. 计算key的tag和主表bucket下标。
2. 读取主表bucket，遍历每个slot：

a)    若slot有效且tag匹配，取指针指向的KVNode，比较key数据是否一致；

​               i.      若一致，将slot有效位和指针清零（置为无效），并将对应KVNode地址加入freelist回收，流程继续；

​               ii.      若不一致，继续遍历。

3. 若主表bucket所有slot均未命中，计算扩展bucket下标（tag & 0x3），如有扩展bucket，递归步骤2。
4. 最后，找到L0 Table中slot指针指向该KVNode地址的条目，将其有效位清零。
5. 删除操作结束。

 

**全局扩容（Resize）**

1. 检查当前主表全局负载因子或单 bucket 扩展池数量，若超过阈值，则触发扩容。
2. 分配新主表 bucket 数组（容量为原表 2 倍），分配新的扩展 bucket 池和内存区域。
3. 递归遍历当前主表所有 bucket 及扩展 bucket：

a)    对于每个有效 slot，重新计算在新表中的 hash 下标和 tag，并插入新表。

b)    迁移过程中，所有 KVNode 物理地址保持不变，仅重建索引结构。

4. 完成后，用新表和新扩展池替换旧主表和旧扩展池，释放旧内存。
5. L0 Table 整体清空，待后续访问自动重建热点 key 缓存。
6. 扩容过程可与 Checkpoint 一致性机制配合，采用写时复制等手段保证数据一致性。

 

## 3.6 一致性与容错保障

 

l L0 Table 与 Main Table 保持最终一致性，所有写入、删除和扩容操作均以主表为准，L0 仅为 cache，可随时失效重建。

l 内存分配、回收和生命周期受 Flink MemoryManager 和 StateBackend 管控，保证资源安全和系统容错能力。

l 所有操作流程兼容 Flink 的 Checkpoint/快照，支持高可用和任务自动恢复。