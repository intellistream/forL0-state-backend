# L0 Hot-Key Cache 实施方案

> 状态: Proposal · Owner: ForL0 Team · 取代 [`L0_Allocator_Acceleration_Design.md`](L0_Allocator_Acceleration_Design.md)

## 0. TL;DR

- 把 L0（鲲鹏 L3 Cache 用户态分区，**典型 ~20 MB / NUMA node**）从 SwissTable 的"更快 malloc 后端"改造为 StateTable 之上的**显式 hot-key 读写穿透 cache**。
- 配置面只剩两个旋钮：`l0-cache.enabled`、`l0-cache.size`。`L0Allocator` 路径连同 `max-per-alloc` 配置一并删除。
- 让"L0 容量 → 热集合驻留数 → 命中率 → 吞吐"形成单调正相关。WordCount(skew=1.1) 在 1–20 MB L0 上预期吞吐单调上升 2.5×，命中率 71 % → 97 %。
- L0 不可用时透明降级为关闭状态，功能等价。

---

## 1. 背景与动机

### 1.1 现状的问题

[`L0Allocator`](../src/main/native/engine/l0_allocator.h) 把整张 SwissTable 的 ctrl+slots 块分配到 L0：

```
allocate(size, align):
    if size <= max_per_alloc && l0_remaining >= size  → l0_mem_alloc
    else                                              → posix_memalign
```

在 WordCount(`numKeys=1M, parallelism=8`) 下：

- 每 KG 的表 ≈ 32 KB（≤ `max_per_alloc=64 KB`，能放入 L0）
- 单 subtask 128 KG × 32 KB ≈ **4 MB**，已经吃掉典型 L0 预算的 1/5
- 把 `l0-cache.size` 从 4 MB 加到 16 MB——L0 里装的还是这 4 MB，多出的容量完全闲置
- 把 `max_per_alloc` 调大让大表入 L0——单张大表吃掉所有预算，几张表后所有新分配都掉回 heap
- **吞吐不随 L0 容量单调上升**，呈早期饱和的阶梯曲线

根因：allocator 路径把"L0 是什么内容"和"workload 的访问局部性"解耦了。

### 1.2 L0 硬件硬约束

来自 [`reference/l0_docs/l0_mem_pool.md`](../reference/l0_docs/l0_mem_pool.md) 与 [`reference/l0_docs/l0_lib_api(2025).md`](../reference/l0_docs/l0_lib_api(2025).md)：

| 约束 | 数值 |
|---|---|
| `max_numa_capacity`（内核模块默认）| **20 MB / NUMA node** |
| 鲲鹏 920 双路 NUMA node 数 | 8 |
| 整机 L0 上限 | ~160 MB（理论），现网实测 < 100 MB |
| 单进程 `cache_tuner_init` 上限 | 受 NUMA 节点 + 同 NUMA 其它进程占用 |
| 访问延迟 | 4–10 cycle |

**结论**：在 ~20 MB 量级，"装整表"的策略天然没有出路（一张表吃掉一大半预算）；只有"装 hot key-value"才能让 L0 容量与吞吐成正比。

### 1.3 设计目标

1. **单调性**：在 L0 预算覆盖热工作集之前，吞吐随 `l0-cache.size` 单调上升
2. **配置极简**：用户只需 `enabled` + `size`，其余内部决策
3. **零侵入**：不改 SwissTable / StateTable 的存储路径，cache 是上层 fast-path filter
4. **零回归**：cache miss 路径相比无 cache 仅多 ~10 cycle，最坏情况 < 5 % 退化
5. **正确性**：写穿透保证 SwissTable 始终是 source of truth，checkpoint/COW 不感知 cache
6. **硬件门禁**：检测不到 L0 硬件时，无论 `enabled=true/false` 一律强制关闭 cache，**不做 heap fallback**——理由见 §3.2

---

## 2. 可行性论证

### 2.1 单 op 周期分解（鲲鹏 920，DRAM-bound 工况）

当前 `valueGetLongLongSafe` 路径（[`ForL0ValueState.valueForLongKey`](../src/main/java/org/apache/flink/state/forl0/ForL0ValueState.java)）：

| 阶段 | cycles |
|---|---:|
| JNI 跨界 | ~15 |
| `SwissTable::find` ctrl group 加载（LLC miss）| ~80 |
| key 比较的 slot 加载（LLC miss）| ~80 |
| 返回 | ~5 |
| **小计** | **~180** |

加 cache 命中后：

| 阶段 | cycles |
|---|---:|
| JNI 跨界 | ~15 |
| set 定位 + NEON 8-way tag 比较（L0）| ~10 |
| key 比较 + value 读取（同 set，L0）| ~5 |
| **小计** | **~30** |

**单 op 峰值加速 6×，端到端含 Java 端 boxing 开销稳健估计 3–5×**。

### 2.2 命中率（Zipf α=1.1, K=1M, entry=24 B）

```
H(N) = Σ_{i=1..N} i^{-α} / Σ_{i=1..K} i^{-α}
```

| L0 size | entry 数 N | 命中率 H | 单 op 加速估计 |
|---:|---:|---:|---:|
| 1 MB | 44 K | 71 % | ~2.4× |
| 4 MB | 175 K | 86 % | ~3.3× |
| 8 MB | 350 K | 92 % | ~3.8× |
| 16 MB | 700 K | 96 % | ~4.3× |
| 20 MB | 870 K | 97 % | ~4.4× |

**1–20 MB 区间单调上升**，正好覆盖 L0 实际可用范围。20 MB 后即使有更多 L0，命中率提升也极有限——这就是"应当饱和"的物理上限。

### 2.3 miss 路径的代价

cache miss 时多一次 set 定位 + 一次 NEON tag 比较 ≈ 10 cycle，相对原 180 cycle 路径多 5 %。命中率 50 % 即可平账，命中率 90 % 时净收益约 3×。**最坏退化可接受**。

### 2.4 正确性

- **单线程**：Flink keyed state 访问本身单线程，cache 内部无并发
- **写穿透 (write-through)**：所有写先到 SwissTable 再写 cache，SwissTable 始终权威
- **rehash 安全**：cache 只存 (key, value) 拷贝，不持有 SwissTable 内部指针
- **erase 同步失效**：删除路径同步调用 `cache.invalidate(key)`
- **checkpoint/COW 解耦**：snapshot 只读 SwissTable + COW shadow map（[`StateTable::COWState`](../src/main/native/engine/state_engine.h)），cache 不参与
- **KG 迁移**：rescale 时整段 KG 释放，对应 cache set 还回全局池

---

## 3. 总体架构

Cache 是 `StateTable` 之上的统一 fast-path filter，**所有进入 native 的 keyed state 访问都先过它**——具体包括 value / list / map / reducing / aggregating 五类 state 的 get/put/remove，以及它们针对 `<long,long>`、`<int,long>`、`<long[],long>`、`<long, long+TimeWindow>` 等的多种 JNI 特化。

```
Java 端任意 keyed state 操作 (value()/update()/get()/put()/clear()/...)
         │
         ▼
JNI 层 (valueGet*/valuePut*/valueRemove*/listGet*/...)
         │
         ▼
  ┌────────────────────────────┐  hit (4–10 cycle)   ┌──────────────┐
  │  HotKeyCache (in L0)       │ ───────────────────▶│  return val  │
  │  set-associative, 8-way    │                      └──────────────┘
  │  NEON tag-compare          │  miss
  └────────────────────────────┘ ──────┐
         ▲ backfill (read)             ▼
         │ write-through (write)
         │                      ┌─────────────────┐
         └──────────────────────│ SwissTable(DRAM)│
                                └─────────────────┘
```

每个 `StateTable<K,V>` 持有一个 `HotKeyCache*`，背后是全局共享的 `HotCacheManager`（持有整块 L0）。读路径：cache hit 直接返回；miss 时走 SwissTable 并把命中结果回填到 cache。写路径：写穿透——先写 SwissTable，再更新 cache。删除路径：先 erase SwissTable，再 invalidate cache。

### 3.1 配置面（最终）

```properties
state.backend.forl0.l0-cache.enabled = false   # 默认关
state.backend.forl0.l0-cache.size    = 8mb     # 受 max_numa_capacity 约束
```

**移除**：`L0_CACHE_MAX_PER_ALLOC`、未来不引入 `mode` / `ratio` 等切分参数。

### 3.2 硬件门禁（Effective-Enabled 决策）

用户配置 `enabled` 只是**意愿**，真正生效与否由启动期硬件探测决定：

```
effective_enabled = config.enabled
               AND dlopen("libl0mempool.so")          succeeds
               AND access("/dev/hisi_l0", F_OK)      succeeds
               AND cache_tuner_init(&t, size)         succeeds
               AND l0_mem_alloc(t, ≥ 1 set)           succeeds (探测性分配并立即归还)
```

任何一步失败 → `effective_enabled = false`，**整个 `HotCacheManager` 不创建**，所有 `StateTable` 的 `cache_` 字段为 `nullptr`。JNI 热路径用 `if (st->cache_)` 单分支跳过，对无 L0 环境零开销。

**严禁的反模式**：把 cache 内存挪到 heap 上"模拟"运行。理由：
- heap-backed cache 没有 4–10 cycle 物理优势，反而引入 set 定位 + tag 比较的额外 ~10 cycle，**变成净亏损**
- 用户在无 L0 的机器上看到的"开启 L0"性能数据是误导，掩盖真实硬件依赖
- 测试场景需要的是"行为等价"（结果正确），而非"性能等价"——直接关闭 cache 已满足

启动期一次性日志策略：

| 探测结果 | 用户配置 | 行为 | 日志级别 |
|---|---|---|---|
| L0 可用 | `enabled=true` | cache 开启 | INFO「L0 cache enabled, capacity=Xmb」|
| L0 可用 | `enabled=false` | cache 关闭 | INFO「L0 cache disabled by config」|
| L0 不可用 | `enabled=true` | **强制关闭** | **WARN「L0 hardware not available (reason: ...), cache forcibly disabled」**|
| L0 不可用 | `enabled=false` | 关闭 | DEBUG（无需告警）|

第 3 行的 WARN 是关键——用户主动开启但没生效，必须显式告知，否则会误以为"开了但没效果"。

### 3.3 内部默认（不暴露）

| 参数 | 值 | 说明 |
|---|---|---|
| `ways` | 8 | NEON `vceq_u8` 一指令并行 |
| `set_size` | 192 B | 1 set = 3 cache line（tag/keys/vals 各占 1 line）|
| `entry_eff_bytes` | 24 B | 摊到每条记录：1 tag + 8 key + 8 val + ~7 填充 |
| `rebalance_interval_ops` | 2²⁰ | 触发自适应再分配 |

---

## 4. 物理布局（针对 `<long, long>` 主热路径）

```c
// 192 B / 64-byte aligned，1 set 占 3 cache line
struct alignas(64) HotSet {
    // line 0: 元数据 + tag 数组
    uint8_t  tags[8];     // H2 = hash & 0x7F；0x80 = EMPTY
    uint8_t  rr;          // round-robin eviction pointer (3 bit)
    uint8_t  _pad[55];
    // line 1: keys
    int64_t  keys[8];
    // line 2: values
    int64_t  vals[8];
};
```

**hot path 触及 L0 字节数 = 64 (line 0 全读) + 8 (line 1 命中 slot) + 8 (line 2 命中 slot) = 80 B**，全部在 L0 → 总延迟 ≤ 10 cycle。

### 4.1 Lookup（NEON）

```c
static inline bool hotcache_get_ll(HotCache* c, int64_t key, int64_t* out) {
    uint64_t h    = mix64((uint64_t)key);
    uint8_t  h2   = h & 0x7F;
    HotSet*  s    = &c->sets[(h >> 7) & c->set_mask];

    uint8x8_t v   = vld1_u8(s->tags);
    uint8x8_t cmp = vceq_u8(v, vdup_n_u8(h2));
    uint64_t  m   = vget_lane_u64(vreinterpret_u64_u8(cmp), 0);
    while (m) {
        int i = __builtin_ctzll(m) >> 3;
        if (s->keys[i] == key) { *out = s->vals[i]; return true; }
        m &= m - 1;
    }
    return false;
}
```

H1/H2 与 SwissTable 复用同一 hash，省一次 mix。

### 4.2 Insert / Update（写穿透）

```c
static inline void hotcache_put_ll(HotCache* c, int64_t key, int64_t val) {
    uint64_t h  = mix64((uint64_t)key);
    uint8_t  h2 = h & 0x7F;
    HotSet*  s  = &c->sets[(h >> 7) & c->set_mask];

    // 已在 set → 原地更新
    uint8x8_t v   = vld1_u8(s->tags);
    uint8x8_t cmp = vceq_u8(v, vdup_n_u8(h2));
    uint64_t  m   = vget_lane_u64(vreinterpret_u64_u8(cmp), 0);
    while (m) {
        int i = __builtin_ctzll(m) >> 3;
        if (s->keys[i] == key) { s->vals[i] = val; return; }
        m &= m - 1;
    }
    // 找空位
    int slot = -1;
    for (int j = 0; j < 8; ++j) if (s->tags[j] == 0x80) { slot = j; break; }
    // RR 淘汰
    if (slot < 0) { slot = s->rr; s->rr = (s->rr + 1) & 7; }
    s->tags[slot] = h2;
    s->keys[slot] = key;
    s->vals[slot] = val;
}
```

### 4.3 Invalidate（erase 路径）

```c
static inline void hotcache_invalidate_ll(HotCache* c, int64_t key) {
    uint64_t h  = mix64((uint64_t)key);
    uint8_t  h2 = h & 0x7F;
    HotSet*  s  = &c->sets[(h >> 7) & c->set_mask];
    uint8x8_t v   = vld1_u8(s->tags);
    uint8x8_t cmp = vceq_u8(v, vdup_n_u8(h2));
    uint64_t  m   = vget_lane_u64(vreinterpret_u64_u8(cmp), 0);
    while (m) {
        int i = __builtin_ctzll(m) >> 3;
        if (s->keys[i] == key) { s->tags[i] = 0x80; return; }
        m &= m - 1;
    }
}
```

### 4.4 淘汰策略：per-set round-robin（不做 LRU）

理由：Zipf(α=1.1) 下热 key 访问频率比冷 key 高 1–2 个数量级，热 key 自然能 "活" 在 set 里；LRU 需要维护链表，反而引入额外写放大（L0 内的写虽快但仍是写）。

---

## 5. 多类型 value 处理

| value 类别 | cache 布局 | 适用场景 |
|---|---|---|
| `long / int / double / 原语` | 8 B 内联（如 §4）| WordCount、计数器 |
| `RowData` 单字段定长 | 等价于原语 | SQL 简单聚合 |
| `RowData` 固定长度 row（≤ 32 B）| `vals[8][32]`（set 增至 5 line/320 B）| Nexmark 部分 query |
| 变长 bytes / string | **不缓存值，只缓存 `(key → SwissTable slot ptr + len)`**（每条 16 B：tag+key+ptr）| 窗口聚合、状态较大场景 |
| key 类型不可哈希为 long | **不入 cache**，走原 SwissTable | 复杂复合 key |

**Phase A 必须实现"原语"和"变长 ptr 模式"两类**——后者把 entry 从 24 B 压到 16 B，对窗口聚合等大 value 场景是关键。

实现：`HotCache` 是模板/variant，`StateTable<K,V>` 在构造时根据 `<K,V>` 类型选择具体实例化或不创建 cache。

---

## 6. 全局预算管理

### 6.1 单一 L0 池

`HotCacheManager` 启动时调用 **一次** `l0_mem_alloc(tuner, total_size)` 拿到整块 L0，避免内部碎片。切成 `S = total / 192 B` 个 set，进入全局 free-set 池。

```cpp
class HotCacheManager {
public:
    HotCacheManager(size_t l0_capacity);   // dlopen + cache_tuner_init + l0_mem_alloc
    ~HotCacheManager();                    // l0_mem_free + cache_tuner_destroy

    // 给 (state_id, kg) 分配 num_sets 个 set；返回 set 索引基址
    HotCache* acquire(uint32_t state_id, int kg, uint32_t num_sets);
    void release(HotCache* cache);

    // Phase C: 自适应再分配
    void rebalance_if_needed();

    bool is_active() const { return tuner_ != nullptr; }
    size_t capacity_bytes() const;    // 实际拿到的 L0 字节
    size_t used_bytes() const;
    size_t total_sets() const;
    size_t free_sets() const;

private:
    void* tuner_;
    void* l0_base_;
    size_t l0_capacity_;
    std::vector<HotSet> all_sets_;        // 物理 set 数组
    std::vector<uint32_t> free_set_ids_;  // 空闲 set 池
    // 每个 acquire 出去的 cache 持有 set_ids 列表
};
```

### 6.2 每 (StateTable, KG) 持有 set 列表

```cpp
struct HotCache {
    uint32_t set_mask;          // num_sets - 1 (power of 2)
    HotSet*  sets;              // 物理上指向 manager 的连续区域子集
    // 统计
    std::atomic<uint64_t> hits, misses;
};
```

set 数固定为 2 的幂，便于 `(h >> 7) & mask`。

### 6.3 自适应再分配（Phase C）

每 N=2²⁰ 次 op 触发：

1. 每个 `HotCache` 收集 `(hits, misses, sets)`
2. 计算"边际收益" = `miss_rate × access_rate / sets`
3. 从最低的对象抽 1–2 个 set 给最高的，迁移即"清空目标 set 的 tag"（best-effort，无需原子）

预算大时可能不需要 rebalance；预算紧（1–4 MB）时显著影响。

### 6.4 多 slot/进程协调

`size × same-NUMA TM slots ≤ max_numa_capacity`。超出时后启的 slot `cache_tuner_init` 失败，进入降级（cache 关闭）。运维需要在文档中按公式配置：

```
recommended_size = (max_numa_capacity_MB - reserve) / slots_per_numa
```

---

## 7. JNI 集成点

| 文件 | 改动 |
|---|---|
| `src/main/native/engine/hot_cache.h`（新）| `HotCache` 模板 + `HotCacheManager` |
| `src/main/native/engine/state_engine.h` | `StateTable<K,V>` 持有 `HotCache*`，构造时按类型决定是否创建 |
| `src/main/native/engine/l0_allocator.h` | **删除**。dlopen + `cache_tuner_init` 逻辑迁到 `hot_cache.cpp` |
| `src/main/native/jni/forl0_jni.cpp` | 5 个热 JNI 注入 cache：`valueGet/Put LongLong`、`IntLong`、`FixedRowLong`、`LongLongWithTW`、`valueRemove*` |
| `ForL0Options.java` | 删除 `L0_CACHE_MAX_PER_ALLOC` |
| `NativeEngine.java::createEngine` | 签名简化为 `(startKG, numKG, totalKG, l0Enabled, l0SizeBytes)` |
| `ForL0KeyedStateBackendBuilder.java` | 简化配置传递 |

热 JNI 模板（伪码）：

```cpp
JNIEXPORT jboolean JNICALL
Java_..._valueGetLongLongSafe(JNIEnv* env, jclass, jlong h, jlong key, jint kg, jlongArray out) {
    auto* st = (StateTable<int64_t, int64_t>*)h;
    int64_t v;
    if (st->cache_ && hotcache_get_ll(st->cache_, key, &v)) {
        env->SetLongArrayRegion(out, 0, 1, &v);
        return JNI_TRUE;
    }
    auto* p = st->find(kg, key);
    if (!p) return JNI_FALSE;
    if (st->cache_) hotcache_put_ll(st->cache_, key, *p);
    env->SetLongArrayRegion(out, 0, 1, p);
    return JNI_TRUE;
}
```

---

## 8. 可观测性

通过 JNI 暴露给 Flink Metrics（`org.apache.flink.metrics.MetricGroup`）：

| Metric | 类型 | 维度 | 用途 |
|---|---|---|---|
| `forl0.hotcache.lookups` | Counter | state, kg | 命中率分母 |
| `forl0.hotcache.hits` | Counter | state, kg | 命中率分子 |
| `forl0.hotcache.invalidations` | Counter | state, kg | erase 频率 |
| `forl0.hotcache.bytes_used` | Gauge | global | ≤ `l0-cache.size` |
| `forl0.hotcache.bytes_capacity` | Gauge | global | **实际拿到的 L0**（与配置可能不一致）|
| `forl0.hotcache.sets_per_state` | Gauge | state | rebalance 调试 |

**`bytes_capacity` 与 `bytes_used` 必须打到默认 dashboard**：用户配 8 MB 但内核只给 4 MB 时，必须显式可见。

---

## 9. 风险与缓解

| 风险 | 缓解 |
|---|---|
| miss 路径多 ~10 cycle 开销 | 退化 < 5 %；命中率 50 % 即平账 |
| Set 冲突踢热 key | 8-way + Wyhash 良好分布；Zipf 下 set 内冲突 < 5 % |
| 变长 value 不能完整缓存 | "key→slot ptr"模式仍能省 60 % miss |
| L0 库不可用（dev 机/无 `/dev/hisi_l0`）| §3.2 硬件门禁强制关闭，不做 heap fallback；状态后端继续工作 |
| 用户配 `enabled=true` 但硬件缺失 | 一次性 WARN「forcibly disabled」，避免静默误导 |
| TaskManager 多 slot 抢 L0 | 后启 slot 的 `cache_tuner_init` 失败 → 该 slot 走门禁关闭路径，其它 slot 不受影响 |
| `l0_mem_alloc` 实际拿到 < 配置 size | 接受实际值，按实际值切 set，metric 暴露 |
| KG rescale 迁移 | KG 释放时把 set 还给 manager；新 KG 申请时从池里要 |
| 复杂 key（GENERIC strategy）| 不创建 cache，直接走 SwissTable |

---

## 10. 实施计划

> 性能验证（吞吐曲线、profile）不在实施计划内，由后续独立的 benchmark 任务跟进。本节关注代码与单测。

### Phase A — `<long,long>` 主热路径 + 变长 ptr 缓存

代码：
- [ ] `hot_cache.h` / `hot_cache.cpp`：`HotCacheManager` + `HotCache<long,long>`
- [ ] `hot_cache.h`：`HotCache<long, slot_ptr>` 用于变长 value
- [ ] `state_engine.h`：`StateTable` 接 cache 字段 + 类型选择（`<K,V>` 不在白名单时 `cache_=nullptr`）
- [ ] `forl0_jni.cpp`：`valueGetLongLongSafe` / `valuePutLongLong` / `valueRemove` 接入 cache
- [ ] `valueGetLongStringPtr` / `valueGetGeneric` 接入 ptr-mode cache
- [ ] `ForL0Options.java`：删除 `L0_CACHE_MAX_PER_ALLOC`
- [ ] `NativeEngine.createEngine` 签名简化
- [ ] §3.2 硬件门禁实现：dlopen / `/dev/hisi_l0` 探测 / `cache_tuner_init` / 探测性 `l0_mem_alloc` 全过才启用

单测（C++ GoogleTest，`src/main/native/test/hot_cache_test.cc`）：
- [ ] **HotSet 布局**：`sizeof(HotSet)==192`、`alignof==64`、tag/keys/vals 偏移正确
- [ ] **lookup 命中**：put 后 get 返回相同 value
- [ ] **lookup miss**：未 put 的 key 返回 false
- [ ] **update 原地**：同 key 两次 put，get 返回最新值；set 内只占一槽
- [ ] **invalidate**：put → invalidate → get miss
- [ ] **RR 淘汰**：单 set 灌入 9 个不同 key（全落同一 set，构造 H1 冲突）后第 1 个被淘汰，第 2-9 个仍可命中
- [ ] **EMPTY tag 与合法 H2 区分**：H2=0 的 key 不能误命中 EMPTY 槽
- [ ] **大量随机 key**：put 100k key 后随机 lookup，命中率符合 set 容量上界（理论 hits ≈ min(N, sets×8)）
- [ ] **NEON / scalar 等价**：编译开关切到 scalar 路径，输出与 NEON 一致

单测（C++ GoogleTest，`src/main/native/test/hot_cache_manager_test.cc`）：
- [ ] **dlopen 失败模拟**：通过 LD_PRELOAD stub 让 dlopen 返回 NULL → manager 不创建，`is_active()==false`
- [ ] **`cache_tuner_init` 失败模拟** → 同上
- [ ] **acquire/release set**：申请 N set 后 `free_sets()` 减 N；release 后归还
- [ ] **set 数非 2 的幂**：申请 100 set，实际向下取整到 64
- [ ] **多 cache 并存**：两个 cache 各自 mask 独立，set 不重叠

单测（Java JUnit5，`src/test/java/.../HotCacheIntegrationTest.java`）：
- [ ] **`enabled=false`**：`createEngine` 不创建 manager；`ValueState` get/put/remove 行为与现状一致
- [ ] **`enabled=true` 且 L0 不可用**：JNI 返回 `effective_enabled=false`，Java 侧能从日志或 metric 看到 WARN
- [ ] **`enabled=true` 且 L0 可用**（鲲鹏环境跳过/启用）：插入后 lookup 命中，`hits` counter 增长
- [ ] **写穿透正确性**：`update` 后通过 SwissTable 直查（绕过 cache 的调试 JNI）值一致
- [ ] **erase 正确性**：`clear()` 后 `value()==null`
- [ ] **checkpoint 一致**：snapshot 前后 cache 状态变化不影响 restore 出来的值
- [ ] **KG rescale**：scale-out / scale-in 后状态与 cache off 时一致

### Phase B — 扩展类型覆盖

代码：
- [ ] `<int, long>` / `<int, double>` 特化
- [ ] `<long[], long>`（FixedLengthRow key）特化：set index 用 `mix(fold(fields))`
- [ ] `<long, long with TimeWindow ns>`：set index 用 `mix(key) ^ mix(ns_start)`
- [ ] `<long, fixed-row-bytes ≤ 32 B>`：set 扩展到 5 cache line

单测：
- [ ] 每个新增特化复用 Phase A 的 8 类基础测试矩阵（lookup/update/invalidate/RR/EMPTY/random/NEON/scalar）
- [ ] **TimeWindow set index 分布**：不同 (key, ns) 组合应均匀分布到 set，构造冲突测试
- [ ] **FixedRow key 折叠正确性**：不同 row 不能折叠到同一 long
- [ ] **fixed-row value 字节级一致**：put → get 字节相同，含 padding

### Phase C — 自适应预算 + Metrics

代码：
- [ ] `HotCacheManager::rebalance_if_needed()`
- [ ] Flink Metrics 注册（§8）
- [ ] 删除 [`l0_allocator.h`](../src/main/native/engine/l0_allocator.h) 及 JNI 中的 `L0Allocator` 创建路径
- [ ] 文档：在 `README.md` 添加多 slot 配置公式

单测：
- [ ] **rebalance 收益方向**：构造高 miss-rate cache A + 低 miss-rate cache B，触发 rebalance 后 A 的 set 数应增、B 应减
- [ ] **rebalance 不丢数据**：迁移即清空目标 set tag，下一轮 lookup 必定 miss 但 SwissTable 仍能取到正确值
- [ ] **rebalance 阈值边界**：访问数 < `rebalance_interval_ops` 时不触发
- [ ] **Metric 计数**：`lookups = hits + misses`；`bytes_capacity` 反映 manager 实际拿到的 L0 字节
- [ ] **L0Allocator 路径删除回归**：现有所有 ITCase 仍通过，未泄漏对 `L0Allocator` 的引用

---

## 11. 验证标准 (Definition of Done)

> 性能验证（吞吐曲线、profile）由后续独立 benchmark 任务跟进，**不在本实施计划的 DoD 范围内**。本节只列代码 / 单测维度的验收标准。

### 11.1 单元测试覆盖

- 所有 §10 列出的 C++ GoogleTest 与 Java JUnit5 用例**全部通过**
- 行覆盖率 `hot_cache.{h,cpp}` ≥ 90 %，分支覆盖率 ≥ 80 %
- AddressSanitizer / UndefinedBehaviorSanitizer 跑单测无报错
- Native 单测在 macOS（无 L0）与 Linux 鲲鹏（有 L0）双环境均能跑

### 11.2 正确性回归

- 现有所有单测 + ITCase 通过
- Checkpoint/restore 端到端测试通过（cache 内容不参与 snapshot 但状态结果一致）
- KG rescale 测试通过

### 11.3 降级与硬件门禁

由 §10 Phase A 的 Java 集成测试和 C++ manager 测试覆盖：

- 无 `/dev/hisi_l0` 的开发机 + `enabled=false`：DEBUG 日志，正常运行
- 无 `/dev/hisi_l0` 的开发机 + `enabled=true`：**WARN 日志「forcibly disabled」**，行为与 `enabled=false` 一致
- 配 `size=64mb` 但实际只批 16 MB：`bytes_capacity` metric = 16 MB，无 crash
- `cache_` 字段为 `nullptr` 时所有 JNI 单分支跳过（由代码 review 确认 + ITCase 行为等价证明）

---

## 12. 替代方案对比与抉择

| 方案 | 吞吐 vs L0 size | L0 ~20 MB 下表现 | 工程复杂度 | 决策 |
|---|---|---|---|---|
| A. L0Allocator（现状）| 阶梯/早饱和 | 几张表占满即止 | 已实现 | **删除** |
| B. **L0 Hot-Key Cache（本方案）** | 单调 → 饱和 | 覆盖 ~97 % 热集合 | 中 | **采纳** |
| C. Ctrl-Only L0 | 阶梯 | 仅省 ctrl miss | 高（改 SwissTable）| 拒绝 |
| D. L0 Write Buffer | 不适用（in-place 更新）| — | 高 | 拒绝 |
| A+B 共存 | 资源分薄 | 两边都不够 | 高 | 拒绝（L0 太小）|

**最终结论：以 hot-key cache 完全替代 allocator 路径。** 在 L0 ~20 MB 这个硬约束下，allocator 路线没有出路；hot-key cache 是唯一能让 `l0-cache.size` 与吞吐成正比的方案。
