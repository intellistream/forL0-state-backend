# ForL0 State Backend

ForL0 State Backend是一个为Apache Flink设计的高性能状态后端实现，旨在通过缓存友好的数据结构和分层索引架构来优化状态访问性能。

## 项目概述

ForL0 State Backend采用双层索引结构设计：
- **L0 Table**：热点缓存层，为高频访问的键提供极低延迟的访问
- **Main Table**：主索引表，存储全量状态数据，支持高负载和局部扩展

该项目的长期目标是充分利用鲲鹏CPU的L0 Cache特性，但当前实现专注于软件层面的优化，为未来的硬件集成奠定基础。

## 核心特性

### 🚀 性能优化
- **缓存友好设计**：64字节对齐的桶结构，匹配CPU缓存行
- **多级索引**：L0缓存 + 主表的分层架构减少访问延迟
- **局部扩展**：主表支持局部树状扩展，避免全局重哈希
- **多种替换策略**：支持LRU、LFU、FIFO、Random等缓存替换算法

### 🔧 架构特点
- **键值分离**：索引指向堆外的Entry数据块
- **扩展桶池**：统一管理扩展桶，最多支持255个扩展桶/主桶
- **内存管理**：基于Flink MemoryManager的统一内存管理
- **状态快照**：完整支持Flink的检查点机制

### 📊 监控统计
- **多维度指标**：L0缓存命中率、主表负载因子、扩展桶使用率
- **性能统计**：访问计数、驱逐率、内存使用量等
- **运行时监控**：提供详细的运行时统计信息

## 快速开始

### 环境要求

- Java 8+
- Apache Flink 1.13+
- Maven 3.6+

### 编译构建

```bash
# 克隆项目
git clone https://github.com/intellistream/forL0-state-backend.git
cd forL0-state-backend

# 编译项目
mvn clean compile

# 运行测试
mvn test

# 打包
mvn package
```

### 集成使用

#### 1. 添加依赖

将生成的JAR包添加到Flink的lib目录：

```bash
cp target/flink-statebackend-forL0-1.0-SNAPSHOT.jar $FLINK_HOME/lib/
```

#### 2. 配置StateBackend

**编程方式配置：**

```java
import org.apache.flink.runtime.state.heap.ForL0StateBackend;

// 创建ForL0StateBackend实例
ForL0StateBackend stateBackend = new ForL0StateBackend();

// 设置到执行环境
env.setStateBackend(stateBackend);
```

**配置文件方式：**

在`flink-conf.yaml`中添加：

```yaml
state.backend: org.apache.flink.runtime.state.heap.ForL0StateBackendFactory
```

#### 3. 使用状态API

ForL0 State Backend完全兼容Flink的状态API：

```java
// ValueState示例
ValueStateDescriptor<String> descriptor = 
    new ValueStateDescriptor<>("myState", String.class);
ValueState<String> valueState = getRuntimeContext().getState(descriptor);

// MapState示例
MapStateDescriptor<String, Integer> mapDescriptor = 
    new MapStateDescriptor<>("myMapState", String.class, Integer.class);
MapState<String, Integer> mapState = getRuntimeContext().getMapState(mapDescriptor);
```

## 架构设计

### 数据结构布局

#### L0 Table结构
```
桶大小: 64字节 (缓存行对齐)
槽位数: 4个槽位/桶
槽位结构:
├── Tag (2B): 键的哈希摘要
├── Valid (1B): 有效位标记
├── Extension (5B): 扩展字段 (LRU/LFU数据)
└── Pointer (8B): Entry指针
```

#### Main Table结构
```
桶大小: 64字节
槽位数: 6个槽位/桶 + 4个扩展指针
槽位结构:
├── Tag (2B): 键的哈希摘要
└── Pointer (8B): Entry指针
扩展指针: 4个扩展桶指针 (1B each)
```

### 核心组件

- **ForL0StateTable**: 状态表顶层抽象
- **L0Table**: L0缓存表实现
- **MainTable**: 主索引表实现
- **ExtensionBucketPool**: 扩展桶池管理
- **EntryArena**: 键值对物理存储
- **MemoryManagerAllocator**: 内存分配器

## 性能特性

### 查找操作流程
1. 优先查询L0 Table缓存
2. L0未命中时查询Main Table
3. 主桶未命中时查询扩展桶
4. 成功查找时更新L0缓存

### 替换策略
- **LRU (Least Recently Used)**: 适合时间局部性强的工作负载
- **LFU (Least Frequently Used)**: 适合频率局部性强的工作负载
- **FIFO (First In First Out)**: 适合流式处理工作负载
- **RANDOM**: 适合随机访问模式的工作负载

## 监控与统计

### L0缓存统计
```java
L0TableStats stats = l0Table.getStats();
double hitRate = stats.getHitRate();
double evictionRate = stats.getEvictionRate();
long accessCount = stats.getAccessCount();
```

### 主表统计
```java
MainTableStats stats = mainTable.getStats();
double loadFactor = stats.getLoadFactor();
int extensionBucketsUsed = stats.getExtensionBucketsUsed();
```

## 配置选项

当前实现主要通过构造函数参数进行配置：

```java
// L0Table配置
L0Table l0Table = new L0Table(
    allocator, 
    bucketCountPow2,        // 桶数量(2的幂)
    ReplacementPolicy.LRU   // 替换策略
);

// MainTable配置
MainTable mainTable = new MainTable(
    allocator,
    bucketCountPow2,        // 桶数量(2的幂)
    0.75                   // 负载因子阈值
);
```

## 测试

### 运行单元测试

```bash
# 运行所有测试
mvn test

# 运行特定测试类
mvn test -Dtest=L0TableTest

# 运行特定测试方法
mvn test -Dtest=MainTableTest#testInsertAndGet
```

### 性能测试

项目包含多个性能测试用例，用于验证不同场景下的性能表现：

- **内存分配器测试**: `MemoryManagerAllocatorTest`
- **L0缓存测试**: `L0TableTest`
- **主表测试**: `MainTableTest`
- **扩展桶池测试**: `ExtensionBucketPoolTest`

## 当前差距

### 已实现功能
✅ 双表架构 (L0Table + MainTable)  
✅ 多种替换策略 (LRU/LFU/FIFO/Random)  
✅ 扩展桶池管理  
✅ 完整的统计监控  
✅ Flink StateBackend集成  

### 待实现功能
❌ L0 Cache硬件特性集成  
❌ JNI层面的L0 Cache映射  
❌ 配置文件系统  
❌ 动态配置调整  


## 文档

- 📖 [详细设计说明书](ForL0-State-Backend设计说明书.md)

## 联系方式

- 邮箱: yangjinyun@hust.edu.cn

---

**注意**: 当前实现专注于软件层面的性能优化，L0 Cache硬件特性的集成将在未来版本中实现。
