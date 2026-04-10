# ForL0 State Backend设计说明书

## 1. 整体架构设计

ForL0 StateBackend 面向 Apache Flink 的 Keyed State 场景设计，目标是在保持 Flink State API 兼容性的前提下，将状态存储、索引组织、快照导出与恢复重建统一纳入一套面向原生执行路径优化的状态引擎中。系统采用 Java 状态后端壳层、JNI 桥接层与 C++ 原生状态引擎分层协同的结构：Flink 运行时通过标准 `StateBackend` / `KeyedStateBackend` 接口访问状态；Java 层负责状态注册、序列化器分析、状态对象封装与用户函数执行；JNI 层负责跨语言调用与本地库装载；C++ 层负责实际状态表的组织、读写、命名空间管理以及 checkpoint 数据的导出与导入。

图 1整体架构图

整个系统围绕 `StateEngine -> StateTable -> KeyGroup -> Namespace -> SwissTable` 的层级组织展开。一个作业实例对应一个 native `StateEngine`；每个状态描述符在引擎中注册为一个 typed `StateTable<K,V>`；每个 `StateTable` 再按 key-group 划分内部状态；在 VoidNamespace 模式下直接访问 key-group 对应的 `SwissTable`，在一般 namespace 模式下通过 namespace 映射到各自独立的 `SwissTable`。这种组织方式保证了状态分区、命名空间隔离、快照迭代与恢复写回都可以围绕统一的数据结构完成。

系统的主要特点如下：
- 统一的原生状态引擎：Keyed State 的实际数据全部保存在 C++ 侧，Java 层只承担 API 适配和控制逻辑。
- 缓存友好的哈希组织：底层采用 SwissTable 结构，以 ctrl byte、分组匹配与连续 slot 布局降低查找成本。
- 多层热路径优化：针对 long key、int key、TimeWindow namespace、RowData、MapState typed inner map 等场景提供专用访问路径。
- 与 Flink 运行时兼容：保留 Flink 的 StateDescriptor、Snapshot 语义、PriorityQueue 语义与 Operator State 接入方式。
- 可恢复的快照机制：通过 native Copy-On-Write 跟踪与 key-group 级别导出实现一致性 checkpoint 与 savepoint 支持。

### 1.1 状态访问优化路径

系统专门构建了一层状态访问优化路径，用于缩短热路径上的分支判断、对象封装与序列化链路。该优化路径不是一个独立的表结构，而是由以下机制共同构成：
- Java 状态对象在初始化阶段预计算访问策略，例如 `KeyNsStrategy`、`ListStrategy`、`MapStrategy`，将 key、namespace 与 value 的组合映射为固定的热路径分支。
- `NativeEngine` 针对高频类型组合提供专用 JNI 接口，例如 long key / long value、int key / double value、TimeWindow namespace、MapState long-long 内层映射等。
- 对 `BinaryRowData`、RowData accumulator 和部分 bytes value 提供零拷贝读取能力，通过 native 指针与长度直接构建 Flink 侧可访问的内存视图。
- 对 RowData key 提供单字段 primitive 解包和固定长度多字段 `FixedRow` 编码，降低通用序列化路径的开销。

因此，该优化路径层的目标是在不改变状态语义的前提下，让热点场景优先使用最窄的类型表示、最短的 JNI 路由和最少的数据复制。

### 1.2 原生状态表组织

系统中负责权威状态存储的核心结构由 native 侧的 `StateTable<K,V>` 与其内部的 `SwissTable<K,V>` 共同构成。每个 `StateTable` 对应一个状态描述符，负责该状态在所有 key-group 和 namespace 范围内的组织与访问；而每个 `SwissTable` 负责一个具体分区下的实际哈希存储。

原生状态表的组织方式如下：
- 状态维度：每个已注册状态在 `StateEngine` 中有唯一 handle，并映射到一个 typed `StateTable<K,V>`。
- key-group 维度：每个 `StateTable` 内部按 key-group 划分，保证与 Flink 的 key-group 分区模型一致。
- namespace 维度：VoidNamespace 模式下直接使用 `tables[keyGroup]`；一般 namespace 模式下使用 `namespace_map[keyGroup] -> SwissTable` 的方式组织。
- 数据维度：每个 `SwissTable` 直接以 typed slot 保存键和值，不再通过额外的主索引指针层间接寻址。

这种设计使得主状态表同时具备分区清晰、访问路径稳定、恢复粒度明确和迭代导出方便的特性，是整个后端一致性和持久化语义的基础。

状态表的建立和使用遵循一条固定链路：`ForL0KeyedStateBackendBuilder` 负责创建 native `StateEngine`；`ForL0KeyedStateBackend` 在状态首次访问时，根据 `StateDescriptor`、serializer 和 namespace 信息向 native 层注册状态；JNI `registerState` 根据 key/value/namespace type id 与 descriptor 字节流确定具体的 `StateTable<K,V>` 类型；后续所有状态读写都通过 state handle 回到该 `StateTable`。因此，文档中的“状态表”不是概念性的中间层，而是 checkpoint、restore、增删改查共同依赖的权威存储结构。

### 1.3 状态存储与内存布局

系统中的 Keyed State 数据全部落在 native 引擎内部，Java 层只保留 engine handle、state handle 和少量序列化辅助对象。真正的状态布局由 `StateEngine`、`StateTable` 和 `SwissTable` 三层共同决定。

从存储组织上看，系统划分为四层：
- 状态层：每个 Flink 状态描述符在 native 侧对应一个 typed `StateTable<K,V>`。
- 分区层：每个 `StateTable` 按 key-group 划分，保证状态数据与 Flink key-group 路由一致。
- 命名空间层：VoidNamespace 直接落到 key-group 对应表；一般 namespace 则先定位到 namespace，再进入该 namespace 下独立的 `SwissTable`。
- 槽位层：每个 `SwissTable` 内部用 ctrl 区和 slot 区组织条目，slot 直接保存 key 与 value。

从单表布局上看，`SwissTable` 的核心内存结构由两部分组成：
- ctrl 区：每个槽位对应 1 字节控制信息，用于表示 EMPTY、DELETED 或保存 H2 指纹；表尾还保留镜像 ctrl 区，便于 group 扫描越界时连续读取。
- slot 区：按槽位顺序连续保存实际条目，每个槽位直接持有 `std::pair<K,V>`，因此键和值与索引元数据之间没有额外的主索引层或 entry 指针层。

这种布局有两个直接结果：
- 查找阶段先扫一小段连续 ctrl 字节，快速筛掉大部分不可能命中的槽位，再对少量候选槽位做键比较。
- 命中后可以直接在对应 slot 上读取 value，不需要再跟随额外指针跳到另一个 value 存储区。

复杂 value 不再单独拆成外部 value store，而是跟随 slot value 一起组织：
- ValueState 的 primitive 值直接内联在 slot value 中。
- bytes/string 值使用 `std::string` 持有连续字节。
- ListState 使用 `ElementList` 保存元素序列。
- MapState 使用 typed `InnerMap` 保存内层键值映射。
- ReducingState 与 AggregatingState 直接把中间值或 accumulator 放在 slot value 中。

在快照期间，为了保证异步写出与前台状态更新之间的一致性，`StateTable` 会为每个 key-group 维护 Copy-On-Write 视图，使异步线程读取稳定表内容，而前台线程仍可继续执行更新。

Copy-On-Write 机制的核心思想不是复制整张状态表，而是只记录“自快照开始之后发生变化的条目”。因此，系统在每个 key-group 上维护一组快照辅助结构：
- 活动标记：表示该 key-group 是否处于快照窗口内。
- 覆盖记录：保存某个 key 在快照时刻对应的旧值；若某个 key 是在快照开始后新插入的，则只记录“该条目属于快照之后新增”。
- 删除记录：保存快照开始时存在、但在快照窗口内被删除的条目旧值。
- namespace 级跟踪表：对一般 namespace 状态，按 namespace 分别维护同样的覆盖记录和删除记录。

这种设计的直接效果是：
- 前台写入只在某个 key 第一次被改动时保存一次旧值，而不是复制整张表。
- 快照线程读取时可以在当前表内容之上叠加覆盖记录和删除记录，恢复出“快照发起时刻”的一致性视图。
- 对于快照开始后新增的条目，快照遍历阶段直接跳过，不把它们写入本次 checkpoint。

### 1.4组件解偶与集成

ForL0 StateBackend 的组件划分清晰，能够在保持 Flink 兼容性的同时让存储实现独立优化。

各层职责如下：
- `ForL0StateBackend` 与 `ForL0StateBackendFactory` 负责 Flink 侧后端注册、配置解析与后端创建。
- `ForL0KeyedStateBackendBuilder` 负责 native 库装载、引擎创建、checkpoint/restore 初始化以及后端实例构建。
- `ForL0KeyedStateBackend` 负责 native state handle 管理、状态对象缓存、元信息注册以及快照发起。
- `ForL0ValueState`、`ForL0ListState`、`ForL0MapState`、`ForL0ReducingState`、`ForL0AggregatingState` 负责 Flink 内部状态接口到 JNI 调用的映射。
- `TypeAnalyzer` 和 `RowDataKeyAccessor` 负责类型识别、布局描述生成和 RowData 快路径支持。
- `NativeEngine` 负责所有 JNI 声明以及 native 共享库的动态加载。
- `StateEngine`、`StateTable`、`SwissTable` 和 checkpoint 读写器共同构成 C++ 状态引擎的数据面与持久化能力。

这种解耦方式保证了 Flink 接口层、Java 状态适配层和 native 数据面之间的职责边界明确，便于持续优化底层存储结构而不影响上层状态 API 语义。

## 2. 架构设计细节

ForL0 State Backend 遵循 Flink `AbstractStateBackend` / `AbstractKeyedStateBackend` 的扩展方式，并通过 native 引擎承载具体状态存储。Java 侧围绕状态描述符与 serializer 选择访问路径，native 侧围绕 `StateTable<K,V>` 与 `SwissTable<K,V>` 组织读写、迭代、快照导出和恢复重建。其组件关系如图 4所示。

图 4 ForL0StateBackend组件图

### 2.1 可复用组件与扩展组件

ForL0 State Backend 复用了 Flink 中与状态后端集成、优先队列管理、快照输出与元信息表示相关的一系列组件，并在 Keyed State 数据面上引入专用扩展。

#### 2.1.2 状态注册与原生状态表

ForL0 通过 Flink 的 `StateDescriptor`、状态元信息和内部状态接口维持与 Flink 状态抽象的兼容关系，而具体的数据结构由 native `StateTable<K,V>` 承载。Java 层通过状态名称注册 native 状态表，并将状态读写映射到对应 handle。

native `StateTable<K,V>` 支持以下核心能力：
- 按 key-group 组织状态数据。
- 支持 VoidNamespace 与一般 namespace 两种模式。
- 提供 `get`、`put`、`remove`、`for_each`、`merge namespace` 等状态访问能力。
- 在快照期间提供 Copy-On-Write 保护，支持一致性导出。

状态注册的具体过程包括以下几个步骤：
- Java 侧从 `StateDescriptor` 提取状态名、状态类型、key serializer、namespace serializer、value serializer。
- `TypeAnalyzer` 将 serializer 转换为 native 可识别的 `typeId`，并在需要时生成 `TypeLayout` descriptor 字节流。
- `NativeEngine.registerState(...)` 将这些类型信息传入 JNI。
- JNI `registerState` 根据状态种类和值布局，选择具体的 native 模板实例，例如 `StateTable<long, long>`、`StateTable<long, std::string>`、`StateTable<FixedRow, InnerMapLongLong>` 等。
- native 层返回 state handle，Java 层将其缓存到 `nativeStateHandles`，同时把对应 `RegisteredKeyValueStateBackendMetaInfo` 缓存到 registry，供后续 snapshot/restore 使用。

这意味着状态注册不仅是名字登记，还同时决定了后续访问走哪条 JNI 快路径、native 表中采用哪种键值表示以及 checkpoint 导出时如何编码。

#### 2.1.3 PriorityQueueManager / HeapPriorityQueue

定时器与优先队列部分直接复用 Flink 原生实现。`ForL0KeyedStateBackend` 内部通过 `HeapPriorityQueuesManager` 管理优先队列状态，并使用 `HeapPriorityQueueSnapshotRestoreWrapper` 完成优先队列的快照与恢复。因此，ForL0 不改变现有定时器语义、触发时机和优先队列内部行为，只替换 Keyed State 的主存储路径。

#### 2.1.4 ForL0KeyedStateBackend

`ForL0KeyedStateBackend` 是 ForL0 状态后端的核心控制组件，承担以下职责：
- 持有 native engine handle 和各状态的 native state handle。
- 维护已创建状态对象缓存与 state meta info registry。
- 在状态首次创建时完成 serializer 兼容性校验与 native 状态注册。
- 为五类 Flink 内部状态创建对应包装对象。
- 对接快照策略、savepoint 资源生成以及 backend dispose 生命周期。

它在语义上是 Flink `AbstractKeyedStateBackend` 的具体实现，在结构上是 Java 控制层与 native 数据层的衔接点。

#### 2.1.5 NativeEngine

`NativeEngine` 是 Java 层访问原生状态引擎的 JNI 桥接入口，负责 native 库加载和 Java 到 C++ 的函数映射。其主要职责如下：
- 提供 `createEngine`、`destroyEngine`、`registerState` 等生命周期管理接口。
- 提供 ValueState、ListState、MapState、ReducingState、AggregatingState 的各类 JNI 访问函数。
- 提供 checkpoint / restore 相关的 native 导出与导入接口。
- 在启动时优先通过 `System.loadLibrary("forl0_engine")` 加载共享库，失败后再从打包资源中提取本地库文件加载。

`NativeEngine` 不保存状态本身，但它定义了 Java 控制层访问 native 数据面的全部入口。

### 2.2 关键设计组件

#### 2.2.3 StateEngine

`StateEngine` 是系统的统一原生状态引擎，负责管理所有已注册状态的句柄和状态表。其职责如下：
- Java 层负责将 Flink 状态操作翻译为 native 引擎可以执行的访问请求。
- native 层负责真正的 KeyGroup、Namespace 与哈希表组织。
- 快照、恢复、遍历和命名空间合并都围绕同一套数据面结构完成。

`StateEngine` 位于 native 数据面的最顶层，是 Java backend 与具体状态表之间的统一路由中心。

`StateEngine` 内部维护 handle 到状态表实例的映射，并记录 task 负责的 key-group 范围。对于每个已注册状态，`StateEngine` 保存该状态的 key layout、value layout、namespace 形态以及快照所需的辅助信息。这样一来，JNI 层接收到一次状态访问调用时，只需通过 handle 找到对应 `StateTable`，再结合 key-group 和 namespace 即可定位到最终 `SwissTable`。快照阶段，`StateEngine.prepare_snapshot()` 和 `release_snapshot()` 统一作用于所有状态表，保证整套状态在同一个 checkpoint 周期内进入和退出一致性视图。

#### 2.2.4 状态布局与命名空间组织

这里更关键的不是“如何分配”，而是状态条目如何在 native 侧组织和索引。ForL0 的状态布局围绕 `StateTable -> key-group -> namespace -> SwissTable` 展开。

具体来说：
- 对于 VoidNamespace 状态，每个 key-group 直接对应一个 `SwissTable<K,V>`，因此查找路径是 `stateHandle -> keyGroup -> SwissTable -> slot`。
- 对于一般 namespace 状态，每个 key-group 下先维护一个 namespace 到 `SwissTable<K,V>` 的映射；不同 namespace 的条目不混放在同一张表里，而是各自独立持有哈希表。
- namespace 为空时直接移除对应表，因此删除完最后一个条目后不长期保留空 namespace 容器。

这种组织方式对恢复和快照非常重要：
- checkpoint 导出时可以按 key-group 和 namespace 稳定遍历。
- 恢复阶段可以根据 namespace 类型直接把条目重建到目标 `SwissTable`。
- namespace merge 时不需要扫描整张全局大表，只需要在源 namespace 表和目标 namespace 表之间搬移或归并条目。

#### 2.2.5 SwissTable

`SwissTable<K,V>` 是状态系统的底层哈希存储结构，负责维护键到值的高效映射。其设计要点如下：
- 采用 ctrl byte + slot 连续布局。
- 采用 H1/H2 哈希位分配方式，H2 存入 ctrl，H1 用于探测起点。
- 采用按 group 的并行匹配方式查找候选槽位。
- 使用 2 的幂容量、7/8 负载因子、tombstone 删除和 grow/rehash 机制。
- slot 中直接以 `std::pair<K,V>` 形式持有键和值。

因此，`SwissTable` 既承担哈希索引职责，也直接承载实际 value，不需要额外的间接层才能访问状态。

`SwissTable` 的访问路径围绕 ctrl 区和 slot 区协同工作：表初始化后，ctrl 区负责记录每个槽位的状态和 H2 指纹；查找时先扫描 ctrl group，再对命中的候选槽位做键比较；插入时优先复用 deleted 槽位，其次使用 empty 槽位；删除时只更新 ctrl 为 tombstone，不立即搬移元素；当 deleted 槽位积累过多或增长预算耗尽时，通过 `rehash()` 或 `grow()` 重建表布局并迁移有效元素。这个机制保证了前台更新路径较短，同时把布局整理成本延后到必要时再支付。

#### 2.2.6 状态访问策略与JNI快路径

系统通过状态访问策略与 JNI 快路径机制，将高频场景映射到更短、更窄、更少复制的访问路径。其实现由以下部分组成：
- 状态对象初始化阶段的访问策略固化。
- `NativeEngine` 中按类型细分的专用 JNI 接口。
- RowData 与 BinaryRowData 的快速编码和零拷贝读取。
- MapState typed inner map 与 ListState typed element 路径。

该机制的设计目标是让热点场景尽量避免通用 serializer 路径、避免多次 JNI 往返、避免重复对象分配，从而形成整个后端的快速访问层。

访问策略在各状态对象初始化阶段一次性确定。例如：
- `ForL0ValueState` 使用 `KeyNsStrategy` 区分 `LONG_VOID`、`INT_VOID`、`ROWDATA_LONG_VOID`、`ROWDATA_INT_VOID`、`ROWDATA_FIXED_VOID`、`LONG_TW` 和 `GENERIC`。
- `ForL0ListState` 使用 `ListStrategy` 区分 `LONG_ELEM_LONG_VOID`、`LONG_VOID`、`LONG_TW` 和 `GENERIC`。
- `ForL0MapState` 使用 `MapStrategy` 区分 `LONG_LONG_VOID`、`LONG_BYTES_VOID`、`BYTES_LONG_VOID`、`LONG_MAP_VOID`、`LONG_MAP_TW` 和 `GENERIC`。

这些策略一旦确定，后续每次 `get`、`put`、`add`、`remove` 都只走对应分支，不再重复做整套类型判定。这是 Java 壳层保持轻量并具备热路径优化价值的关键原因。

#### 2.2.7 Typed Value Container

状态值在 native 层通过 typed value container 直接承载，并嵌入在 `SwissTable` 的 slot value 中。不同状态类型对应的 value 结构如下：
- ValueState：primitive 值直接内联，bytes/string 使用 `std::string` 保存。
- ListState：使用 `ElementList` 保存列表值，本质为 `std::vector<std::string>`。
- MapState：使用 typed `InnerMap` 保存内层映射，例如 `InnerMapLongLong`、`InnerMapLongString`、`InnerMapStringLong` 与 generic `InnerMap`。
- ReducingState / AggregatingState：使用对应的 value 或 accumulator 类型直接作为 slot value。

这种组织方式使得状态数据与索引保持紧密邻接，减少二次寻址和额外指针跳转。

其中，复杂值容器的设计直接影响状态操作粒度：ListState 并不是把 Java `List<V>` 常驻在堆上，而是在 native 层保存元素序列；MapState 也不是整张 map 以单个 opaque bytes blob 存取，而是优先用 typed inner map 执行单 entry 级别的 `get/put/remove/contains`。这使得常见的点查和点更新可以绕开“整值读出、Java 反序列化、整值写回”的高成本路径。

#### 2.2.8 ForL0State

ForL0State 由五类状态实现组成，分别对应 Flink 的 ValueState、ListState、MapState、ReducingState 与 AggregatingState。每类状态对象都具备三项共同能力：
- 识别当前 key、namespace、value 的最佳访问路径。
- 将状态访问请求映射为单个或少量 JNI 调用。
- 在需要用户函数参与的场景下，于 Java 侧完成逻辑计算后写回 native 状态。

状态对象是 ForL0 对外暴露状态语义的直接载体，也是 Java 层热路径优化的主要落点。

#### 2.2.9 快照与恢复

ForL0 的快照与恢复模块由 `ForL0SnapshotStrategy`、`ForL0SnapshotResources`、`ForL0KeyValueStateIterator` 与 native checkpoint 读写器组成。该模块完成以下职责：
- 在同步阶段通知 native 状态表进入 Copy-On-Write 模式。
- 在异步阶段按 key-group 导出全部 KV state 数据。
- 复用 Flink 对 priority queue state 的标准写出路径。
- 支持普通 checkpoint 恢复和 canonical savepoint 恢复。
- 在恢复过程中按状态类型与命名空间类型重建 typed `StateTable<K,V>`。

#### 2.2.10 类型分析与布局描述

系统通过类型分析与布局描述机制完成 serializer 分析、类型描述生成与缓冲区复用。其核心职责包括：
- 根据 `TypeSerializer` 识别 key、namespace、value 的原生类型 ID。
- 生成与 C++ `TypeLayout` 对齐的 descriptor 字节流，在状态注册时发送到 native 层。
- 对 RowData serializer 进行结构分析，决定使用 primitive、FixedRow 还是 generic 路径。
- 在状态对象内部复用 `DataOutputSerializer` 和 `DataInputDeserializer`，降低序列化对象分配开销。

native 侧 `type_layout.h` 对这些类型描述做了解析和落地，当前支持的核心 `TypeId` 包括 `INT32`、`INT64`、`FLOAT32`、`FLOAT64`、`BOOL`、`STRING`、`BYTES`、`LIST`、`MAP`、`FIXED_ROW`、`VOID_NS` 和 `TIME_WINDOW`。其中：
- `FIXED_ROW` 用于多字段且全部可按固定 8 字节槽位编码的 RowData key，底层以 `FixedRow` 结构内联存储。
- `TIME_WINDOW` 用于窗口 namespace，底层以 `{start, end}` 的二元组表示。
- `BYTES` 与 `STRING` 在 native 层都落到 `std::string`，但 checkpoint 写出格式不同，前者遵循 Flink bytes serializer 语义，后者遵循字符串 serializer 语义。

这套类型系统使 Java 侧的 serializer 世界与 native 侧的模板实例化世界建立了明确映射，是 typed `StateTable` 能够成立的前提。

### 2.3 原生状态组织实现

ForL0 的原生状态组织以 `StateEngine` 为中心。系统通过 state handle 将 Java 状态描述符与具体 typed 状态表关联起来，并通过 key-group、namespace 和 `SwissTable` 共同完成状态映射。

#### 2.3.1 结构组成

如图 5所示，原生状态组织由四层结构组成：
- `StateEngine`：任务级状态引擎，维护所有已注册状态表和状态句柄。
- `StateTable<K,V>`：单个状态描述符的分区状态表。
- Namespace 容器：在非 VoidNamespace 模式下，将一个 key-group 下的 namespace 映射到各自的 `SwissTable<K,V>`。
- `SwissTable<K,V>`：具体保存键和值的底层哈希表。

图 5 原生状态组织结构

在值组织上，slot value 直接持有 primitive、`std::string`、`ElementList`、`InnerMap` 或 accumulator，因此结构上不再需要额外的单独值地址层。

#### 2.3.2 访问优化路径实现

访问优化路径主要由以下机制组成：
- 访问策略预计算：在状态对象初始化时确定固定访问路径。
- 类型专用 JNI：为高频组合提供单独 native 入口，减少分支和装箱。
- RowData 快路径：支持 primitive 解包、FixedRow 编码和零拷贝读取。
- 安全组合读取：通过 safe get 类 JNI 在一次调用中完成存在性判断与值读取。

表 1 访问优化路径组成

字段 说明
策略枚举 固化 key/namespace/value 访问路径
专用 JNI 为高频类型组合提供短路径访问
RowData 访问器 支持单字段和多字段固定长度编码
零拷贝读取 减少 bytes 中转和对象复制

#### 2.3.3 SwissTable 设计

`SwissTable<K,V>` 的内存和访问组织如下：

表 2 SwissTable ctrl区语义

字段 说明 字节数(Byte)
ctrl EMPTY / DELETED / H2 / SENTINEL 1
cloned ctrl group 越界镜像区 若干

表 3 SwissTable Slot数据结构

字段 说明
key 键对象或其紧凑表示
value 值对象、字符串、列表、内层 map 或 accumulator

图 7 SwissTable布局

`SwissTable` 的查找与更新流程依赖以下设计：
- 先根据 H1 计算探测起点。
- 使用 group 匹配检查 ctrl 区中等于 H2 的候选槽位。
- 对候选槽位执行键相等比较。
- 若未命中，则继续按照 probing 序列前进。
- 插入时寻找 empty 或 deleted 槽位；扩容时执行 rehash，将旧条目重新散列到新表。

#### 2.3.4 值容器组织

slot value 中承载的实际状态值结构按状态类型定义如下：

图 8 值容器组织结构

表 4 键值数据组织

字段 说明
primitive value 直接以内联值形式保存
string/bytes 使用 `std::string` 保存
list 使用 `ElementList` 保存元素序列
map 使用 typed `InnerMap` 保存内部键值

表 5 复杂值恢复方式

字段 说明
generic bytes 通过 Flink serializer 反序列化
BinaryRowData 通过指针包装或字节重建
accumulator 通过 serializer 或 RowData accessor 重建

#### 2.3.5 主要操作流程

**插入（Put）**

图 9 状态写入流程

1. Java 状态对象根据 key、namespace 和 value 类型选择预计算访问策略。
2. 若对应状态尚未注册，则在 `ForL0KeyedStateBackend` 中完成 native 状态表注册。
3. Java 层将键和值转换为目标 JNI 接口需要的类型表示，例如 primitive、FixedRow、bytes 或 typed map entry。
4. native 层定位目标 `StateTable`、key-group 与 namespace。
5. 在对应 `SwissTable` 中执行 `insert_or_assign` 或容器内更新。
6. 若表达到负载阈值，则执行 grow / rehash，重新组织 slot 布局。

**查询（Get）**

图 10 状态查询流程

1. Java 状态对象根据策略选择最短访问路径。
2. 对 primitive 场景使用 safe get，在一次 JNI 调用中完成查找和取值。
3. 对 RowData / BinaryRowData 场景优先使用零拷贝读取。
4. 对 generic 场景使用 `byte[]` 返回，再通过 Flink serializer 反序列化。
5. 未命中时返回默认值、空集合或 null，保持 Flink 状态语义一致。

**变换（Transform）**

1. ReducingState 通过读取旧值、执行 `ReduceFunction.reduce` 并写回结果完成变换。
2. AggregatingState 通过读取或创建 accumulator、执行 `AggregateFunction.add` / `merge` 并写回 accumulator 完成变换。
3. ListState 的 `addAll` 与 MapState 的 `putAll` 通过批量更新当前 value 完成变换。
4. namespace merge 通过读取源 namespace 值、清理源状态并写回目标 namespace 完成变换。

其中两类“变换型状态”采用“native 存储，Java 执行用户函数”的模式：
- `ForL0ReducingState.add(value)` 对 primitive 快路径优先使用 get-and-put JNI，先把新值暂存到 native，再基于返回的旧值在 Java 侧执行 `reduce`，若旧值存在则再回写归约结果。
- `ForL0AggregatingState.add(value)` 先构造新的 accumulator，尝试与旧 accumulator 合并，然后把结果重新写回 native。对于 RowData accumulator，则优先走零拷贝读指针路径，避免先把 accumulator 转成 `byte[]` 再反序列化。

因此，ForL0 并未把用户自定义 `ReduceFunction` / `AggregateFunction` 下沉到 native，而是将存储和函数执行分层处理，既保证 Flink 语义一致，又让状态本身仍停留在 native 数据面。

**删除（Remove）**

1. Java 层根据状态类型调用 `clear` 或 `remove(userKey)`。
2. native 层在目标 `SwissTable` 中执行 `erase`。
3. 删除的槽位以 tombstone 形式保留，供后续 rehash 回收。
4. 若 namespace 对应的 `SwissTable` 为空，则移除该 namespace 表。

**全局扩容（Resize）**

1. 当 `SwissTable` 的增长预算耗尽时触发扩容。
2. 分配新的 ctrl 区和 slot 区，容量扩大为新的 2 的幂。
3. 重新遍历旧表中的有效条目，根据新的 H1/H2 重新插入。
4. 清理 tombstone，恢复表的紧凑布局。
5. Java 层无需感知扩容过程，整个过程由 native 表结构内部完成。

### 2.4 ForL0State实现

ForL0State 层提供五类状态实现，对外保持与 Flink State API 一致的语义，并在内部针对不同类型组织高效访问路径。

#### 2.4.1 ForL0ValueState

`ForL0ValueState` 负责单值状态的读取、写回和清理。其设计重点如下：
- 支持 long key、int key、TimeWindow namespace、RowData key、FixedRow key 与 generic key 的多路径访问。
- 对 primitive 值提供 safe get 和专用 put。
- 对 `BinaryRowData` 和部分 RowData value 提供零拷贝读取。
- 对 generic value 使用 serializer 转换为 bytes 并存入 native 状态表。

其主要操作与底层映射如下：
- `value()`：调用对应 JNI get 路径返回当前值或默认值。
- `update(value)`：根据类型调用对应 JNI put；当 value 为空时执行 clear。
- `clear()`：调用对应类型的 native clear 接口。

`ForL0ValueState` 优先采用以下几类短路径：
- long key + long value: 单次 JNI 中完成存在性判断和数值返回。
- long key + bytes/string value: 直接取回 native `std::string` 对应字节序列。
- RowData value: 若 native 能返回底层地址和长度，则直接包装成 `BinaryRowData` 或借助 `MemorySegment` 构建只读视图。
- RowData key: 若 key 只有单字段 primitive，则直接解包为 long/int；若是多字段固定布局，则编码为 `FixedRow`；否则回退到 generic bytes key。

因此，`ForL0ValueState` 承担了最丰富的类型分发逻辑。

#### 2.4.2 ForL0ListState

`ForL0ListState` 负责列表状态的读取、追加、整体替换与命名空间合并。其主要设计如下：
- long key + long element 使用专用 long[] 路径。
- long key + generic element 使用序列化元素路径。
- TimeWindow namespace 采用专用窗口 JNI 路径。
- generic key 使用序列化 key 路径。

主要操作如下：
- `get()`：从 native 读取整个列表。
- `add(value)`：追加单个元素。
- `update(values)`：整体替换列表。
- `addAll(values)`：批量追加多个元素。
- `mergeNamespaces(target, sources)`：按 Flink 语义合并多个源 namespace 的列表到目标 namespace。

ListState 的 value 容器在 native 层表现为元素序列而不是 Java 集合对象。对于 `LONG_ELEM_LONG_VOID` 路径，JNI 直接返回 `long[]`，避免逐元素反序列化；对于 generic element 路径，Java 侧将每个元素编码成 bytes，再由 native 顺序拼接存储。`mergeNamespaces` 不依赖专门的 native merge 原语，而是遵循 Flink 语义，在 Java 层依次读取源 namespace、清空源状态、收集元素后写回目标 namespace。

#### 2.4.3 ForL0MapState

`ForL0MapState` 负责外层 keyed state 下的内层映射维护。系统为其设计了 typed inner map 支持，以减少整张 map 的重复反序列化。其主要策略包括：
- `LONG_LONG_VOID`：outer key 为 long，inner key/value 为 long。
- `LONG_BYTES_VOID`：outer key 为 long，inner key 为 long，inner value 为 bytes/string。
- `BYTES_LONG_VOID`：outer key 为 long，inner key 为 bytes/string，inner value 为 long。
- `LONG_MAP_VOID`：outer key 为 long，inner map 为 generic bytes-bytes。
- `LONG_MAP_TW`：TimeWindow namespace 下的 long key generic inner map。
- `GENERIC`：所有通用回退路径。

主要操作包括：
- `get(userKey)`：读取单个内层条目。
- `put(userKey, userValue)`：写入单个内层条目。
- `putAll(map)`：批量写入内层条目。
- `remove(userKey)`：删除单个内层条目。
- `contains(userKey)`、`entries()`、`keys()`、`values()`：直接访问 native 内层 map 视图。

这部分设计的关键点在于更新粒度。对于 typed inner map 路径，常见操作直接命中 native 内层 map 的单个 entry，而不是把整张 map 序列化成一个 blob 后整值覆盖。只有在 generic 回退路径下，才通过 bytes 形式读写条目或整体 map。这使 MapState 在高频点查和点更新场景下相比传统“整 map 反序列化”路径更有优势。

#### 2.4.4 ForL0ReducingState

`ForL0ReducingState` 负责增量归约状态。其设计模式为“native 存储 + Java reducer 计算”：
- 旧值存放在 native 状态中。
- `add(value)` 时先读取旧值，再在 Java 侧执行 `ReduceFunction.reduce`，最后写回结果。
- 对 long key / long value 等高频场景提供更短的 get-and-put 路径。
- `mergeNamespaces` 按 reducer 语义合并多个 namespace 的中间值。

在该设计中，ReducingState 尽量把“是否已有旧值”的判断和一次临时写入合并到 JNI 调用中，从而减少 Java 与 native 的往返次数。对于非 primitive 值，如果 native 返回的是旧 bytes，Java 先反序列化旧值，再调用 reducer，最后将新结果重新序列化并写回。也就是说，真正的归约逻辑始终由用户提供的 Java 函数决定，native 负责存储和旧值读取辅助。

#### 2.4.5 ForL0AggregatingState

`ForL0AggregatingState` 负责基于 accumulator 的聚合状态。其设计模式为“native 存储 accumulator + Java aggregate function 执行”：
- accumulator 保存在 native 状态中。
- `get()` 时读取 accumulator 并调用 `AggregateFunction.getResult` 生成输出值。
- `add(value)` 时执行 `createAccumulator`、`add` 和必要的 `merge`，再将新 accumulator 写回 native 状态。
- 对 RowData accumulator 支持零拷贝读取与重建。
- `mergeNamespaces` 按聚合函数语义合并源 namespace 的 accumulator。

与 ReducingState 相比，AggregatingState 多了一层 accumulator 生命周期管理。聚合阶段在 Java 侧创建新的 accumulator，然后与 native 中的旧 accumulator 合并；对于 RowData accumulator，若 native 返回底层地址和长度，则优先直接包装为 `BinaryRowData` 参与聚合，只有在无法走零拷贝路径时才回退到标准反序列化。这样可以降低复杂 accumulator 在热点路径上的对象构造成本。

## 3. 容错机制设计

ForL0 StateBackend 的容错机制遵循 Flink Checkpoint / Savepoint 的一致性语义，并将 native 状态表导出与恢复能力纳入统一流程。系统的 checkpoint 数据来源于 `StateTable<K,V>` 的一致性视图，priority queue 状态仍沿用 Flink 原生写出机制，最终共同组成完整的 Keyed State 快照结果。

### 3.1 快照与恢复流程

#### 3.1.1 快照发起

在 Flink 发起 snapshot 后，系统通过 `ForL0SnapshotStrategy` 进入同步准备阶段。主要流程如下：

1. 调用 `NativeEngine.prepareSnapshot(engineHandle)`。
2. `StateEngine` 将所有 `StateTable` 切换到 Copy-On-Write 模式。
3. 每个 key-group 开始记录快照期间的覆盖、新增和删除轨迹。
4. Java 层构造 `ForL0SnapshotResources`，收集 KV state 的 meta info、priority queue snapshot 与 key-value 迭代资源。

通过这一阶段，系统能够在前台继续处理状态更新的同时，为异步写出提供一致性视图。

这里的一致性不是通过暂停前台访问获得，而是通过 `StateTable` 内部的 snapshot 标记、按 key-group 的 COW 数据结构和快照迭代接口共同实现。前台线程在快照期间的覆盖、新增、删除被记录到额外的快照辅助结构中，而异步写出线程通过 `for_each_snapshot_in_key_group` 一类接口读取稳定视图。这一设计用于避免 checkpoint 期间的全局停顿。

具体而言，COW 的工作过程分为四个阶段：

1. 快照准备阶段

系统在每个 key-group 上打开快照活动标记，并清空该 key-group 对应的覆盖记录、删除记录以及 namespace 级跟踪表。此时主状态表本身不被冻结，也不复制。

2. 前台写入阶段

当快照窗口内发生 `put`、`insert_or_assign`、原地修改、`remove` 或 namespace merge 时，系统先检查该 key 是否已经被跟踪：
- 若某个 key 在快照开始时已经存在，且这是该 key 在本轮快照中的第一次写入，则先保存旧值，再执行更新。
- 若某个 key 在快照开始后才插入，则在覆盖记录中登记为“新增后产生”，而不是保存旧值。
- 若某个 key 在快照窗口内被删除，则把快照开始时刻的旧值转入删除记录，以便异步遍历时仍能输出它。

3. 异步遍历阶段

快照线程遍历 key-group 时，不直接把当前表原样写出，而是执行一层 COW 视图合成：
- 当前表中仍然存在且未被覆盖记录命中的条目，直接输出当前值。
- 当前表中命中覆盖记录且记录中携带旧值的条目，输出旧值而不是当前值。
- 当前表中命中“快照后新增”标记的条目，直接跳过。
- 删除记录中的条目额外补写一次，因为它们在快照时刻存在，但在当前表中已经被删掉。

对于一般 namespace 状态，上述过程按 namespace 分别进行：先遍历当前 namespace 表，再叠加该 namespace 的覆盖记录和删除记录。

4. 快照释放阶段

当异步写出完成后，系统关闭快照活动标记，并清空该轮快照积累的全部 COW 辅助结构。主状态表在整个过程中始终保持可写，只是快照期间多维护了一层按 key-group、按 namespace 组织的增量历史信息。

#### 3.1.2 快照采集

快照采集阶段按 key-group 写出数据。其流程如下：

1. 写出 `KeyedBackendSerializationProxy`，记录 key serializer snapshot、state meta info snapshot 和压缩信息。
2. 为每个 key-group 记录输出偏移位置。
3. 写出 priority queue state block。
4. 调用 `NativeEngine.writeKeyGroupData(engineHandle, keyGroupId)` 导出该 key-group 下所有 KV state 条目。
5. native 导出的 KV 数据按 stateId、entryCount 和具体条目组织，末尾使用 key-group 结束标记。
6. 异步写出完成后调用 `NativeEngine.releaseSnapshot(engineHandle)`，释放 Copy-On-Write 快照上下文。

checkpoint 文件在每个 key-group 上采用固定块数布局：先写该 key-group 的所有 priority queue block，再写一个 KV state block。即使某个 key-group 下没有 KV 条目，也写出一个空的压缩块，这样恢复端就可以依赖“每个 key-group 等于 `PQ 状态数 + 1` 个压缩块”的固定结构顺序读取，而不需要额外猜测边界。

native KV block 的内部格式由 `checkpoint_writer.h` 定义，核心组织为：对每个已注册 KV state 顺序写出 `stateId(short)`、`entryCount(int)`，然后依次写出 namespace、key、value。VoidNamespace 会写一个单字节标记；TimeWindow namespace 写出 `{start, end}`；ListState、MapState、FixedRow、InnerMap 等复杂值则按照与 Flink serializer 兼容的二进制格式输出。这保证了 restore 端和 `ForL0KeyValueStateIterator` 都能基于统一格式读取数据。

### 3.2 Savepoint与状态恢复

ForL0 支持普通 checkpoint 恢复与 canonical savepoint 恢复。

恢复流程如下：

1. `ForL0KeyedStateBackendBuilder` 读取状态句柄并恢复 `KeyedBackendSerializationProxy`。
2. 完成 key serializer 兼容性校验和 state meta info 重建。
3. 恢复 priority queue state。
4. 对每个 key-group 的 KV block，按 state handle 类型分发到对应 typed `StateTable<K,V>`。
5. 对 canonical savepoint，使用 `ForL0KeyValueStateIterator` 导出的规范格式进行写出，并在恢复时将规范格式重新翻译回 native 状态表条目。
6. 完成恢复后，backend 重新持有所有 native state handle，继续提供状态访问服务。

状态恢复过程区分两类输入格式：
- 普通 checkpoint: 使用 ForL0 的 key-group block 格式。构建阶段先按 meta info 注册缺失的 KV state 和 PQ state，然后逐个 key-group 读取 PQ block，最后把剩余 KV block 整块交给 `NativeEngine.readKeyGroupData(...)` 写入 native 状态。
- canonical savepoint: 使用 Flink `FullSnapshotAsyncWriter` 的规范布局。构建阶段先恢复 serializer 与 meta info，然后按 key-group 顺序解析压缩块中的 state entries、识别 key-group 前缀和 metadata follows 标记，再把规范格式条目转换为 ForL0 native 表中的键值写入。

此外，构建阶段对 key serializer 只进行一次兼容性校验，并把校验结果复用于后续所有 state handle。这样可以保证多 handle 恢复时的兼容性判断一致，也避免重复执行相同校验。

### 3.3 资源回收与异常处理

系统在快照与恢复路径中通过显式生命周期控制保证 native 资源不会泄漏：
- snapshot 正常完成后，必须释放 Copy-On-Write 快照状态。
- snapshot 失败时，Java 层在异常路径中也会尝试调用 `releaseSnapshot`。
- backend 构建或 restore 失败时，builder 会销毁已创建的 native engine。
- backend dispose 时调用 `NativeEngine.destroyEngine`，释放全部 native 状态表与内部容器。
- checkpoint 输出流和 priority queue snapshot 资源由 Flink 的 `CloseableRegistry` 与标准输出流生命周期统一管理。

## 4. 插件式集成设计

ForL0 StateBackend 采用插件式集成方式接入 Flink。系统通过 `StateBackendFactory` 完成后端工厂注册，通过 JNI 动态加载本地状态引擎，通过标准 Flink backend 构建流程完成运行时接入。

### 4.1 加载机制

ForL0 的加载机制分为后端工厂加载和 native 共享库加载两部分。

1. Flink 通过 `ForL0StateBackendFactory` 创建 `ForL0StateBackend` 实例。
2. 工厂标识为 `forl0`，配置文件中使用 `state.backend: forl0` 即可启用该后端。
3. `ForL0KeyedStateBackendBuilder` 在构建过程中调用 `NativeEngine.ensureLoaded()` 装载 native 共享库。
4. `NativeEngine` 优先通过 `System.loadLibrary("forl0_engine")` 从系统库路径加载。
5. 若系统库路径不可用，则从 JAR 打包资源中的 `resources/native/` 提取 `.so` 或 `.dylib` 到临时目录后调用 `System.load`。

图 11 Flink运行时StateBackend动态加载流程

### 4.2 打包与部署

ForL0 的打包与部署流程如下：

1. Java 部分通过 Maven 构建，生成包含 backend 实现和 JNI 声明的 JAR。
2. native 部分通过 `src/main/native/CMakeLists.txt` 构建共享库 `forl0_engine`。
3. CMake 在构建完成后将共享库复制到 `src/main/resources/native/`，以便随 JAR 一起打包。
4. 部署时将该 JAR 放入 Flink `lib` 目录，或作为作业依赖引入。
5. 配置 `state.backend: forl0` 和 `state.backend.forl0.async-snapshots` 等参数后即可启用。
6. 根据目标平台选择对应架构的 native 库，确保与实际运行机器的架构和系统一致。

## 5. 总结

ForL0 StateBackend 通过 Java 状态后端壳层、JNI 桥接与 C++ 原生状态引擎的协同，实现了一套面向 Flink Keyed State 的高性能状态后端。系统以 `StateEngine -> StateTable -> KeyGroup -> Namespace -> SwissTable` 为核心组织方式，将状态读写、命名空间管理、快照导出、恢复重建和热路径优化统一纳入同一套架构中。

在状态设计上，ValueState、ListState、MapState、ReducingState 和 AggregatingState 均围绕 typed native state table 展开，支持 long key、TimeWindow、RowData、typed inner map、零拷贝读取等多种优化路径；在容错机制上，系统通过 native Copy-On-Write 与 key-group 级别快照导出建立与 Flink checkpoint/savepoint 语义兼容的状态快照体系；在集成方式上，系统通过 Flink backend factory 与 native 共享库装载机制实现无侵入接入。

整体而言，ForL0 StateBackend 的设计重点在于：以统一的 native 数据面承载全部 Keyed State，以稳定的 Java 控制面保持 Flink API 兼容，以一组围绕类型特化、布局优化和复制最小化的热路径机制支撑高效状态访问，并在此基础上实现可持续扩展的状态后端架构。