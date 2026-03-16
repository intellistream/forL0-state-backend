# L0 Allocator 加速方案设计

## 1. 背景与动机

### 1.1 性能瓶颈

VTune Profile 显示 SwissTable 的 **ctrl 字节加载** 是状态读写主要瓶颈：

- `Group(ctrl_ + seq.pos())` 的内存加载：**~34% Memory Bound, ~10.6% LLC Miss**
- 原因：Hash 探测导致 ctrl 数组访问位置随机，CPU prefetcher 对此无效
- 当工作集超过 CPU Cache 容量时，每次 ctrl group 加载都可能产生 LLC miss (~100 cycles)

### 1.2 L0 Memory 特性

鲲鹏 CPU 的 L0 Memory 将 L3 Cache 的一部分映射为用户可直接寻址的内存区域：

- **访问延迟**: ~4-10 cycles（相比 LLC miss 的 ~100 cycles）
- **容量**: 通过 `max_numa_capacity` 参数配置，典型值 ~20MB/NUMA node
- **用户态 API**: `libl0mempool.so` 提供 `cache_tuner` + `l0_mem_alloc/free` 接口
- **NUMA 感知**: 原生支持指定 NUMA node 分配

### 1.3 核心洞察

正确的 L0 利用方式不是做热点 record 缓存（命中率不可控），而是 **将 SwissTable 的数据结构直接分配在 L0 内存中**，从源头消除 ctrl 加载的 LLC miss。

## 2. 可行性分析

### 2.1 已有的 Allocator 抽象

C++ engine 已预留 `Allocator` 接口（`engine/allocator.h`），**所有 SwissTable 分配路径均经过此接口**：

```
SwissTable(initial_capacity, Allocator* alloc)
  → allocate_and_init(cap)
    → alloc_->allocate(total, alignment)       // 分配 [ctrl | slots]
  → rehash(new_capacity)
    → allocate_and_init(new_cap)               // 新块
    → alloc_->deallocate(old_alloc_ptr, ...)   // 释放旧块
```

且 `StateEngine` → `StateTable` → `SwissTable` 的 `Allocator*` 逐层传递已实现：

```cpp
// state_engine.h
StateEngine(start, num, total, Allocator* alloc = &DefaultAllocator::instance())
  → register_state() → StateTable(start, num, void_ns, alloc_)
    → SwissTable(16, alloc_)
```

**结论：只需实现一个 `L0Allocator : Allocator`，无需修改 SwissTable 或 StateTable 内部逻辑。**

### 2.2 内存布局兼容性

SwissTable 的内存布局是单块连续分配：

```
[ctrl bytes (cap + kGroupWidth)] [padding to align] [slots (cap × sizeof(pair<K,V>))]
 ← alloc_ptr_ 指向这里                                                            →
```

`L0Allocator::allocate()` 只需返回一块满足 size + alignment 要求的 L0 内存指针即可，对布局完全透明。

### 2.3 容量分析

`kGroupWidth = 16`（NEON，鲲鹏平台），`ctrl_bytes(cap) = cap + 16`。

对于 `SwissTable<int64_t, std::string>`（典型的 `Long key + String value` 场景）：

| capacity (slots) | ctrl 区域 | slots 区域（~48B/slot） | 总分配 | 能放 20MB L0？ |
|---|---|---|---|---|
| 16 | 32 B | 768 B | ~800 B | ✅ |
| 64 | 80 B | 3 KB | ~3.1 KB | ✅ |
| 256 | 272 B | 12 KB | ~12.3 KB | ✅ |
| 1024 | 1040 B | 48 KB | ~49 KB | ✅ |
| 4096 | 4112 B | 192 KB | ~196 KB | ⚠️ 少量 |
| 16384 | 16400 B | 768 KB | ~784 KB | ❌ 过大 |

典型部署（128 KeyGroups × 3 states = 384 个 SwissTable），大多数表在 16-256 slots 范围：

- 384 × 3.1 KB (avg) = **~1.2 MB** → 轻松放入 20MB L0
- 384 × 12.3 KB (上界) = **~4.7 MB** → 仍可放入
- 即使随数据增长到 1024 slots: 384 × 49 KB = **~18.8 MB** → 接近上限

### 2.4 为什么不做 Ctrl-Only L0（ctrl 与 slots 分离分配）

对于超出 L0 容量的大表，曾考虑只将 ctrl 分配到 L0、slots 留在 Heap。但这存在问题：

1. **abseil 布局天然保证 ctrl 和 slots 在同一分配块内**。当 ctrl group match 成功后，紧接着访问对应 slot 做 key 比较。虽然 ctrl[i] 和 slots[i] 不在同一 cache line（相隔 `slot_offset` 字节），但它们在同一虚拟地址区域内，TLB 共享更好，全连续时 prefetcher 也有机会沿地址方向投机。
2. **分离分配增加 TLB 压力**：两块独立 VA → 多占 TLB entry。
3. **代码复杂度高**：SwissTable 内部需从"单次 allocate"改为"双次 allocate"，rehash、COW、析构逻辑全部适配。
4. **性价比不足**：ctrl 分离到 L0 只消除 ctrl 的 miss，slot 的 miss 依然存在，而大表本身因工作集大、slot 随机访问占比更高。

**结论：不做 ctrl/slots 分离。整表放入 L0 或整表留在 Heap，通过 L0Allocator 的 fallback 自动决策。**

### 2.5 收益模型验证：为什么小表放 L0 有效？

**潜在质疑**：小表数据量小，在普通 L3 Cache 中被驱逐的概率也低；真正 cache miss 严重的大表反而放不进 L0——那 L0 到底有没有用？

**关键分析**：L0 的核心价值不是"加速单个表"，而是**消除 key group 切换导致的 cache 抖动**。

Flink 流处理中，每条输入 record 的 key 决定了它被路由到哪个 key group。在典型部署下：

- 128 key groups × 3 states = **384 个 SwissTable**
- 每个表虽小（~3-12KB），但 384 个表的**总工作集 = 1.2-4.7MB**
- 加上 JVM heap、GC 活动、Flink 框架代码等其他 cache 竞争者，**总 cache 压力远超单表大小**
- 每条 record 随机命中不同 key group → 频繁在 384 个表间切换
- 普通 L3 Cache 中，切换到一个"冷" key group 的 SwissTable 时，其 ctrl+slots 可能已被其他表或 GC 扫描驱逐 → LLC miss

**L0 的作用**：L0 是 L3 Cache 的**专用隔离分区**，放在 L0 中的数据**不会被其他缓存活动驱逐**。即使系统中有大量 cache 竞争，L0 中的 384 个 SwissTable 始终保持在 cache 中，key group 切换时零 miss。

| 场景 | 普通 L3 | L0 |
|---|---|---|
| 384 个小表，顺序访问同一表 | ✅ 表内数据热，L3 命中 | ✅ 同样命中 |
| 384 个小表，随机切换 key group | ❌ 冷表被驱逐，LLC miss | ✅ L0 隔离，永不驱逐 |
| GC 扫描后立即访问状态 | ❌ GC 污染 L3，状态被驱逐 | ✅ L0 不受 GC 影响 |
| 多线程共享 LLC | ❌ 其他线程竞争 cache | ✅ L0 分区独立 |

**结论：L0 的收益来自于 cache 隔离，而非单纯的"小表加速"。在多表随机访问 + cache 竞争的流处理场景下，收益显著。**

### 2.6 替代方案对比

| 方案 | 描述 | 优点 | 缺点 | 结论 |
|---|---|---|---|---|
| **A. L0 Allocator（本方案）** | 整表分配到 L0 内存 | 零侵入（利用已有 Allocator 抽象）；透明 fallback | 大表放不下 | ✅ 最优 |
| B. 热点 Record 缓存 | L0 存储最近/最频繁访问的 key-value 对 | 理论上覆盖大表场景 | 命中率不可控；需维护缓存一致性；lookup 路径增加一层间接 | ❌ 复杂度高，收益不确定 |
| C. Ctrl-Only L0 | 仅将 ctrl 数组分配在 L0 | 大表也能受益 | 分离 ctrl/slots 增加 TLB 压力；破坏连续布局；改动 SwissTable 内部 | ❌ 性价比不足 |
| D. L0 作为 Write Buffer | 写入先到 L0，后台 flush 到 Heap | 减少写路径延迟 | SwissTable 是 in-place 更新（insert_or_assign），无法拆分读写路径 | ❌ 不适用 |

### 2.7 L0 内存对齐

SwissTable 要求 64 字节对齐（cache line aligned）：

```cpp
size_t alignment = slot_align<slot_type>();
if (alignment < 64) alignment = 64;
```

`libl0mempool.so` 的 Fix Allocator 缓存级别为 32B/64B/128B/..., 且内部基于 page-aligned 内存块分配。对于 >= 64B 的分配，对齐通常满足。但为安全起见，`L0Allocator` 实现中需要显式处理对齐（过量分配 + 手动对齐或验证返回地址）。

## 3. 详细设计

### 3.1 架构总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        StateEngine (C++)                                │
│                                                                          │
│   ┌────────────────────────────────────────────────────────────────┐    │
│   │                 L0Allocator : Allocator                         │    │
│   │                                                                  │    │
│   │   allocate(size, alignment):                                     │    │
│   │     if size ≤ threshold && l0_remaining ≥ size                   │    │
│   │       → l0_mem_alloc(tuner, size)      [L0 Memory]               │    │
│   │     else                                                         │    │
│   │       → posix_memalign(size, align)    [Heap]                    │    │
│   │                                                                  │    │
│   │   deallocate(ptr, size):                                         │    │
│   │     if ptr in l0_regions                                         │    │
│   │       → l0_mem_free(tuner, ptr)                                  │    │
│   │     else                                                         │    │
│   │       → free(ptr)                                                │    │
│   └────────────────────────────────────────────────────────────────┘    │
│          │                                │                              │
│          ▼                                ▼                              │
│   ┌──────────────────┐           ┌──────────────────┐                   │
│   │ SwissTable (小表) │           │ SwissTable (大表) │                   │
│   │ ctrl+slots in L0  │           │ ctrl+slots in Heap│                   │
│   │ ~10 cycles/lookup │           │ ~100 cycles/lookup│                   │
│   └──────────────────┘           └──────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 L0Allocator 实现

新增文件 `engine/l0_allocator.h`：

```cpp
#include "allocator.h"
#include <unordered_map>
#include <dlfcn.h>

namespace forl0 {

class L0Allocator final : public Allocator {
public:
    // l0_capacity: L0 可用总量 (bytes)
    // max_per_alloc: 单次分配可使用 L0 的上限 (超过此值强制 Heap)
    // numa_node_id: 期望的 NUMA 节点
    L0Allocator(size_t l0_capacity, size_t max_per_alloc, int numa_node_id);
    ~L0Allocator();

    void* allocate(size_t size, size_t alignment) override;
    void deallocate(void* ptr, size_t size) override;

    // 诊断
    size_t l0_allocated() const { return l0_allocated_; }
    size_t l0_capacity() const { return l0_capacity_; }
    size_t l0_alloc_count() const { return l0_regions_.size(); }
    size_t heap_alloc_count() const { return heap_count_; }

private:
    bool init_l0_library();  // dlopen libl0mempool.so + cache_tuner_init

    // L0 pool
    void* tuner_;  // cache_tuner*
    size_t l0_capacity_;
    size_t l0_allocated_;
    size_t max_per_alloc_;
    int numa_node_id_;
    std::unordered_map<void*, size_t> l0_regions_;  // L0 分配记录

    // 统计
    size_t heap_count_;

    // dlopen 函数指针
    void* lib_handle_;
    // ... (cache_tuner_init, l0_mem_alloc, l0_mem_free 等)
};

} // namespace forl0
```

#### allocate 逻辑

```cpp
void* L0Allocator::allocate(size_t size, size_t alignment) {
    // 策略判断：大小在阈值内 + L0 有剩余空间
    if (tuner_ && size <= max_per_alloc_ && l0_allocated_ + size <= l0_capacity_) {
        // L0 分配（可能需要 over-allocate 来满足 alignment）
        size_t padded = size + alignment;
        void* raw = l0_mem_alloc_fn_(tuner_, padded);
        if (raw) {
            // 手动对齐
            uintptr_t addr = reinterpret_cast<uintptr_t>(raw);
            uintptr_t aligned = (addr + alignment - 1) & ~(alignment - 1);
            // 存储 raw 指针在 aligned 之前的 8 字节
            if (aligned == addr) {
                // 天然对齐，直接使用
            } else {
                // 在 aligned - sizeof(void*) 处存储 raw 指针
                *reinterpret_cast<void**>(aligned - sizeof(void*)) = raw;
            }
            void* result = reinterpret_cast<void*>(aligned);
            l0_regions_[result] = {raw, padded};
            l0_allocated_ += padded;
            return result;
        }
        // L0 分配失败，fallback
    }
    // Heap fallback
    heap_count_++;
    return DefaultAllocator::instance().allocate(size, alignment);
}
```

#### deallocate 逻辑

```cpp
void L0Allocator::deallocate(void* ptr, size_t size) {
    auto it = l0_regions_.find(ptr);
    if (it != l0_regions_.end()) {
        l0_mem_free_fn_(tuner_, it->second.raw_ptr);
        l0_allocated_ -= it->second.padded_size;
        l0_regions_.erase(it);
    } else {
        DefaultAllocator::instance().deallocate(ptr, size);
    }
}
```

### 3.3 分配策略：Small-Table-First

核心参数：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `l0_capacity` | `20 * 1024 * 1024` (20MB) | L0 总预算 |
| `max_per_alloc` | `64 * 1024` (64KB) | 单次分配上限，超过此值的大表强制 Heap |

策略逻辑在 `L0Allocator::allocate()` 中自包含：

1. `size > max_per_alloc`？→ Heap（大表不放 L0）
2. `l0_allocated_ + size > l0_capacity_`？→ Heap（L0 已满）
3. 否则 → L0

这样实现 **小表优先**：初始 64 slots 的小表（~3KB）在 L0 有空间时全部放入 L0。随着数据增长 rehash 到大容量后，新分配超过阈值时自动退到 Heap，同时释放旧的 L0 空间给其他表使用。

### 3.4 Grow/Rehash 处理

SwissTable `rehash()` 已有的逻辑：

```cpp
void rehash(size_t new_capacity) {
    // 1. 保存旧指针
    void* old_alloc = alloc_ptr_;
    size_t old_capacity = capacity_;

    // 2. 分配新块 (经过 L0Allocator → 自动决定 L0 或 Heap)
    allocate_and_init(new_capacity);

    // 3. 迁移 entries
    for (old entries) { insert into new; }

    // 4. 释放旧块 (经过 L0Allocator → 自动归还 L0 或 Heap)
    alloc_->deallocate(old_alloc, alloc_size<slot_type>(old_capacity));
}
```

**无需修改 SwissTable 代码**。L0→Heap 迁移和 L0 空间回收由 `L0Allocator` 透明完成。

### 3.5 Java 层集成

#### NativeEngine JNI 扩展

```java
// 扩展 createEngine 签名
public static native long createEngine(
    int startKeyGroup, int numKeyGroups, int totalKeyGroups,
    boolean l0Enabled, long l0CapacityBytes, long l0MaxPerAllocBytes);
```

#### ForL0Options 配置

已有的配置项可直接使用：

- `state.backend.forl0.l0-cache.enabled` (boolean, default=false) → 已存在
- `state.backend.forl0.l0-cache.size` (long, default=256MB) → 已存在

新增：
- `state.backend.forl0.l0-cache.max-per-alloc` (long, default=64KB)：单表 L0 分配上限

#### ForL0StateBackend → createEngine 传参

```java
long engineHandle = NativeEngine.createEngine(
    startKeyGroup, numKeyGroups, totalKeyGroups,
    config.get(ForL0Options.L0_CACHE_ENABLED),
    config.get(ForL0Options.L0_CACHE_SIZE),
    config.get(ForL0Options.L0_MAX_PER_ALLOC));
```

#### JNI 层 createEngine 适配

```cpp
JNIEXPORT jlong JNICALL
Java_..._createEngine(JNIEnv* env, jclass,
    jint startKG, jint numKG, jint totalKG,
    jboolean l0Enabled, jlong l0Capacity, jlong l0MaxPerAlloc) {
    Allocator* alloc;
    if (l0Enabled) {
        alloc = new L0Allocator(l0Capacity, l0MaxPerAlloc, /*numa_node=*/-1);
        // numa_node=-1 表示自动检测当前 CPU 的 NUMA 节点
    } else {
        alloc = &DefaultAllocator::instance();
    }
    auto* engine = new StateEngine(startKG, numKG, totalKG, alloc);
    return to_handle(engine);
}
```

### 3.6 模式检测与 Fallback

```
L0Allocator 构造:
  ├─ dlopen("libl0mempool.so")
  │   ├─ 失败 → tuner_ = nullptr (所有 allocate 走 Heap fallback)
  │   │          LOG_WARN("L0 library not found, falling back to heap")
  │   └─ 成功 → 解析函数指针
  │              ├─ cache_tuner_init(&tuner_, l0_capacity)
  │              │   ├─ 失败 → tuner_ = nullptr (同上)
  │              │   └─ 成功 → L0 模式就绪
  │              └─ 如果 /dev/hisi_l0 不存在 → tuner_init 会失败
  └─ tuner_ == nullptr → 纯 Heap 模式 (无 L0 硬件的开发环境自动如此)
```

**行为等价性保证：** L0 不可用时（开发环境无 `/dev/hisi_l0`），`L0Allocator` 的所有 allocate/deallocate 调用等价于 `DefaultAllocator`，状态后端功能和正确性不受影响。

## 4. 性能预期

### 4.1 延迟分析

SwissTable lookup 的关键延迟组成（87.5% 负载因子下平均探测 ~1.2 groups）：

| 操作 | Heap (LLC miss) | L0 | 加速比 |
|---|---|---|---|
| 加载 ctrl group (16B) | ~100 cycles | ~10 cycles | 10x |
| 加载 slot (key 比较) | ~100 cycles | ~10 cycles | 10x |
| 比较 + 返回 | ~5 cycles | ~5 cycles | 1x |
| **端到端 (1.2 groups, 1 hit)** | **~245 cycles** | **~25 cycles** | **~10x** |

注：以上为 worst case（ctrl 和 slot 均 LLC miss）。实际场景中 Heap 版本因 CPU cache 部分命中，加速比预期为 **3-5x**。

### 4.2 吞吐量影响

鲲鹏 920 @ 2.6GHz，单核：
- Heap (LLC miss heavy): ~10M lookups/sec
- L0 (小表全驻留): ~30-50M lookups/sec

## 5. 实施计划

### Phase 1: L0Allocator 基础实现

| 步骤 | 改动文件 | 说明 |
|---|---|---|
| 1.1 | `engine/l0_allocator.h` (新增) | 实现 L0Allocator，含 dlopen + fallback |
| 1.2 | `jni/forl0_jni.cpp` | createEngine 接收 L0 参数，创建 L0Allocator |
| 1.3 | `NativeEngine.java` | 扩展 createEngine 签名 |
| 1.4 | `ForL0Options.java` | 新增 max-per-alloc 配置项 |
| 1.5 | `ForL0KeyedStateBackendBuilder.java` | 读取 L0 配置并传参 |
| 1.6 | `engine/state_engine.h` | destroyEngine 时清理 L0Allocator |

### Phase 2: Benchmark 验证

| 步骤 | 说明 |
|---|---|
| 2.1 | JMH 微基准：对比 L0 on/off 的 ops/sec |
| 2.2 | VTune：确认 ctrl 加载的 LLC miss 比例下降 |
| 2.3 | Nexmark 端到端：确认 query 延迟/吞吐量改善 |

## 6. 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| L0 容量有限 (~20MB/NUMA node) | 大表无法放入 L0 | Small-Table-First + max_per_alloc 阈值自动 fallback |
| libl0mempool.so 不可用 | L0 功能退化 | L0Allocator 透明退化为 DefaultAllocator |
| l0_mem_alloc 不保证对齐 | SwissTable 要求 64B 对齐 | L0Allocator 内 over-allocate + 手动对齐 |
| grow/rehash 时 L0 空间不足 | 表需迁移到 Heap | rehash 调用 allocate() → 自动 fallback，零特殊处理 |
| 多 operator subtask 竞争 L0 | 总用量超出物理 L0 容量 | 每个 StateEngine 独立 L0Allocator，总 capacity 由部署配置控制 |
| Linux 开发环境无 L0 硬件 | 开发测试受阻 | dlopen 失败（无 /dev/hisi_l0）→ 自动 Heap 模式，行为等价 |
| COW Snapshot 安全性 | L0 内存的 COW 语义是否正确 | COW 在 StateTable 层实现（逻辑拷贝到 cow_entries map），与底层 allocator 无关 |
