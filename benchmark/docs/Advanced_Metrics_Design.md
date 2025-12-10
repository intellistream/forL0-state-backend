# 高级性能指标采集方案

## 实现状态

| 功能 | 状态 | 平台支持 | 说明 |
|------|------|----------|------|
| L0Table 指标采集 | ✅ 已完成 | macOS ✅ / Linux ✅ | `L0TableMetricsCollector.java` |
| 指标集成到 Backend | ✅ 已完成 | macOS ✅ / Linux ✅ | `ForL0KeyedStateBackend.java` |
| WordCount 日志解析 | ✅ 已完成 | macOS ✅ / Linux ✅ | `run_wordcount.py` |
| NexMark 日志解析 | ✅ 已完成 | macOS ✅ / Linux ✅ | `run_nexmark.py` |
| 时序图表生成 | ✅ 已完成 | macOS ✅ / Linux ✅ | `generate_report.py` (支持多测试合并) |
| 火焰图 (CPU/Alloc) | ✅ 已完成 | macOS ✅ / Linux ✅ | Async Profiler `-e itimer/cpu/alloc` |
| CPU Cache 命中率 | ✅ 已完成 | macOS ❌ / Linux ✅ | Async Profiler `-e cache-misses` (需要 perf_events) |

**启用方式**：
- **L0Table 指标**：当使用 L0Allocator 时自动启用
- **火焰图/Cache 统计**：使用 `--profile` 参数启用

## 平台限制说明

### macOS 限制

macOS **不支持** `perf_events` 硬件性能计数器，因此以下功能 **仅在 Linux** 上可用：

| 事件 | 说明 | macOS | Linux |
|------|------|-------|-------|
| `-e cache-misses` | LLC (Last Level Cache) 未命中 | ❌ | ✅ |
| `-e L1-dcache-load-misses` | L1 数据缓存未命中 | ❌ | ✅ |
| `-e LLC-load-misses` | 最后一级缓存加载未命中 | ❌ | ✅ |
| `-e dTLB-load-misses` | 数据 TLB 未命中 | ❌ | ✅ |

macOS 上 Async Profiler **支持**的模式：
- `-e itimer` - 基于 setitimer 的 CPU 采样（精度较低）
- `-e wall` - Wall-clock 采样
- `-e alloc` - 内存分配采样

### CPU 采样引擎对比

| 引擎 | Linux | macOS | 精度 | 说明 |
|------|-------|-------|------|------|
| `-e cpu` (perf_events) | ✅ | ❌ | 高 | 支持内核栈 |
| `-e itimer` | ✅ | ✅ | 低 | 通用，macOS 默认 |
| `-e ctimer` | ✅ | ❌ | 中 | Linux 替代方案 |

---

## Async Profiler 安装

### 下载安装

```bash
# macOS
cd benchmark/tools
curl -LO https://github.com/async-profiler/async-profiler/releases/download/v4.2.1/async-profiler-4.2.1-macos.zip
unzip async-profiler-4.2.1-macos.zip

# Linux x64
curl -LO https://github.com/async-profiler/async-profiler/releases/download/v4.2.1/async-profiler-4.2.1-linux-x64.tar.gz
tar xzf async-profiler-4.2.1-linux-x64.tar.gz

# Linux arm64 (鲲鹏)
curl -LO https://github.com/async-profiler/async-profiler/releases/download/v4.2.1/async-profiler-4.2.1-linux-arm64.tar.gz
tar xzf async-profiler-4.2.1-linux-arm64.tar.gz
```

### 配置环境变量

```bash
# 添加到 ~/.zshrc 或 ~/.bashrc
export ASYNC_PROFILER_HOME=/path/to/async-profiler-4.2.1-<platform>
```

### 验证安装

```bash
python benchmark/scripts/utils/profiler.py
```

### 使用方式

```bash
# 运行 WordCount 并采集火焰图
python run_wordcount.py --backend all --profile

# 运行 NexMark 并采集火焰图
python run_nexmark.py --mode cluster --backend all --profile
```

---

## 概述

本文档描述如何采集以下高级性能指标：
1. L0Table 内部指标（命中率、填充率、分布）
2. CPU Cache 命中率
3. 火焰图（CPU/内存热点分析）

---

## 1. L0Table 内部指标

### 1.1 采集目标

生成**时序折线图**，展示以下指标随时间的变化：

| 指标 | 说明 | 数据类型 |
|------|------|----------|
| **命中率 (hitRate)** | `hitCount / accessCount` | 0.0 ~ 1.0 |
| **填充率 (loadFactor)** | `validSlots / totalSlots` | 0.0 ~ 1.0 |
| **淘汰率 (evictionRate)** | 每秒淘汰次数 | count/s |
| **访问率 (accessRate)** | 每秒访问次数 | count/s |

### 1.2 采集方案：周期性采样（推荐）

**核心思路**：使用独立采样线程，定期读取 L0Table 统计信息，避免影响热路径。

```
┌─────────────────────────────────────────────────────────┐
│                    Flink TaskManager                     │
│  ┌─────────────┐     ┌─────────────────────────────┐    │
│  │ L0Table     │     │ MetricsCollector (Thread)   │    │
│  │             │────▶│ - 每 1s 采样一次            │    │
│  │ getStats()  │     │ - 写入内存 buffer           │    │
│  │ (原子读取)  │     │ - 任务结束时输出 CSV        │    │
│  └─────────────┘     └─────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

#### 性能影响分析

| 操作 | 开销 | 频率 | 总影响 |
|------|------|------|--------|
| `getStats()` 读取 | ~100ns | 1次/秒 | **可忽略** |
| 写入内存 buffer | ~50ns | 1次/秒 | **可忽略** |
| 输出 CSV | ~10ms | 任务结束1次 | **无影响** |

**关键设计**：
1. **采样线程独立**：不在热路径（get/put）中采集
2. **原子读取**：`getStats()` 只读取 volatile 计数器，无锁
3. **低频采样**：1 秒 1 次，足够绘制折线图
4. **延迟写入**：累积到内存，任务结束时一次性输出

### 1.3 现有基础

`L0Table.getStats()` 已提供：

```java
public static class L0TableStats {
    public final int validSlots;      // 有效 slot 数
    public final int totalSlots;      // 总 slot 数 (固定 16)
    public final long accessCount;    // 总访问次数
    public final long hitCount;       // 命中次数
    public final long missCount;      // 未命中次数
    public final long evictionCount;  // 淘汰次数
    public final double loadFactor;   // 填充率
    public final double hitRate;      // 命中率
}
```

### 1.4 需要修改的内容

#### 1.4.1 L0Table.java - 确保计数器是 volatile

```java
// 确保线程安全读取（已有或需添加）
private volatile long accessCount = 0;
private volatile long hitCount = 0;
private volatile long missCount = 0;
private volatile long evictionCount = 0;
```

#### 1.4.2 新增 L0TableMetricsCollector.java

```java
public class L0TableMetricsCollector implements Runnable, AutoCloseable {
    
    private final L0Table l0Table;
    private final List<MetricsSample> samples = new ArrayList<>();
    private final long intervalMs;
    private volatile boolean running = true;
    
    public static class MetricsSample {
        long timestamp;
        double hitRate;
        double loadFactor;
        long accessCount;
        long evictionCount;
    }
    
    @Override
    public void run() {
        long lastAccessCount = 0;
        long lastEvictionCount = 0;
        
        while (running) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                break;
            }
            
            L0TableStats stats = l0Table.getStats();
            MetricsSample sample = new MetricsSample();
            sample.timestamp = System.currentTimeMillis();
            sample.hitRate = stats.hitRate;
            sample.loadFactor = stats.loadFactor;
            sample.accessCount = stats.accessCount - lastAccessCount;  // delta
            sample.evictionCount = stats.evictionCount - lastEvictionCount;
            
            samples.add(sample);
            lastAccessCount = stats.accessCount;
            lastEvictionCount = stats.evictionCount;
        }
    }
    
    public void outputToCSV(String filepath) {
        // 输出格式：timestamp,hitRate,loadFactor,accessRate,evictionRate
    }
}
```

#### 1.4.3 集成到 ForL0KeyedStateBackend

```java
public class ForL0KeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
    
    private L0TableMetricsCollector metricsCollector;
    private Thread collectorThread;
    
    // 在初始化时启动
    private void startMetricsCollection() {
        if (metricsEnabled) {
            metricsCollector = new L0TableMetricsCollector(l0Table, 1000); // 1s interval
            collectorThread = new Thread(metricsCollector, "L0Table-Metrics");
            collectorThread.setDaemon(true);
            collectorThread.start();
        }
    }
    
    // 在 dispose() 中输出
    @Override
    public void dispose() {
        if (metricsCollector != null) {
            metricsCollector.close();
            metricsCollector.outputToCSV(metricsOutputPath);
        }
        super.dispose();
    }
}
```

### 1.5 输出格式

#### CSV 文件（用于绘图）

```csv
timestamp,hit_rate,load_factor,access_rate,eviction_rate
1702300000000,0.85,0.75,125000,500
1702300001000,0.87,0.78,130000,450
1702300002000,0.89,0.80,128000,400
...
```

#### 汇总统计（JSON，用于报告）

```json
{
  "l0table_stats": {
    "avg_hit_rate": 0.87,
    "min_hit_rate": 0.75,
    "max_hit_rate": 0.95,
    "avg_load_factor": 0.78,
    "total_accesses": 15000000,
    "total_evictions": 50000,
    "samples_count": 60
  }
}
```

### 1.6 折线图设计

```
┌────────────────────────────────────────────────────────────┐
│  L0Table Metrics Over Time                                  │
│                                                             │
│  1.0 ─┬─────────────────────────────────────────────────    │
│       │    ╭──────────────────────────────╮                 │
│  0.8 ─┼───╱                                ╲────────────    │
│       │  ╱  Hit Rate                        ╲               │
│  0.6 ─┼─╱────────────────────────────────────╲──────────    │
│       │╱         ╭───────────────────╮        ╲             │
│  0.4 ─┼─────────╱  Load Factor        ╲────────╲────────    │
│       │        ╱                       ╲        ╲           │
│  0.2 ─┼───────╱─────────────────────────╲────────╲──────    │
│       │                                                     │
│  0.0 ─┴─────────────────────────────────────────────────    │
│       0s    10s    20s    30s    40s    50s    60s          │
└────────────────────────────────────────────────────────────┘
```

---

## 2. CPU Cache 命中率

### 2.1 推荐方案：Async Profiler

统一使用 Async Profiler 采集 CPU Cache 和火焰图。

```bash
# 安装
wget https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-x64.tar.gz
tar xzf async-profiler-3.0-linux-x64.tar.gz

# 采集 cache-misses
./asprof -d 60 -e cache-misses -f cache_misses.html <pid>

# 采集 L1-dcache-load-misses（更精确）
./asprof -d 60 -e L1-dcache-load-misses -f l1_cache.html <pid>
```

**支持的事件**：
- `cache-misses` - LLC (Last Level Cache) 未命中
- `L1-dcache-load-misses` - L1 数据缓存未命中
- `L1-icache-load-misses` - L1 指令缓存未命中
- `dTLB-load-misses` - 数据 TLB 未命中

### 2.2 使用 perf（Linux 命令行）

```bash
# 启动 Flink TaskManager 并获取 PID
TM_PID=$(jps | grep TaskManagerRunner | awk '{print $1}')

# 采集 60 秒
perf stat -e cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses \
    -p $TM_PID sleep 60

# 输出示例：
#  1,234,567,890  cache-references
#     12,345,678  cache-misses  # 1.0% of all cache refs
```

### 2.3 集成到 Benchmark 脚本

```python
def collect_cache_metrics(tm_pid, duration=60):
    """使用 async-profiler 收集 cache 指标"""
    cmd = [
        f"{ASYNC_PROFILER_HOME}/asprof",
        "-d", str(duration),
        "-e", "cache-misses",
        "-f", "cache_misses.html",
        str(tm_pid)
    ]
    subprocess.run(cmd, check=True)
```

---

## 3. 火焰图

### 3.1 推荐方案：Async Profiler

```bash
# CPU 火焰图
./asprof -d 60 -e cpu -f cpu_flamegraph.html <pid>

# 内存分配火焰图
./asprof -d 60 -e alloc -f alloc_flamegraph.html <pid>

# Wall-clock 火焰图（包含阻塞时间）
./asprof -d 60 -e wall -f wall_flamegraph.html <pid>
```

### 3.2 自动化脚本集成

```python
def generate_flamegraph(tm_pid, output_dir, backend, duration=60):
    """生成火焰图"""
    profiler_path = os.environ.get('ASYNC_PROFILER_HOME', '/opt/async-profiler')
    
    events = ['cpu', 'alloc']
    if platform.system() == 'Linux':
        events.append('cache-misses')
    
    flamegraphs = {}
    for event in events:
        output_file = f"{output_dir}/flamegraph_{event}_{backend}.html"
        cmd = [
            f"{profiler_path}/asprof",
            "-d", str(duration),
            "-e", event,
            "-f", output_file,
            str(tm_pid)
        ]
        subprocess.run(cmd, check=True)
        flamegraphs[event] = output_file
    
    return flamegraphs
```

---

## 4. 实现计划

### Phase 1：L0Table 指标采集（1 天）

**修改文件**：
1. `L0Table.java` - 确保计数器线程安全
2. 新增 `L0TableMetricsCollector.java` - 周期性采样
3. `ForL0KeyedStateBackend.java` - 集成采集器
4. `run_wordcount.py` - 解析输出
5. `generate_report.py` - 生成折线图

### Phase 2：火焰图支持（0.5 天）

**修改文件**：
1. `run_benchmark.py` - 添加 `--profile` 选项
2. 新增 `scripts/profile.py` - 封装 async-profiler 调用
3. 检测 TaskManager PID，启动 profiling

### Phase 3：报告整合（0.5 天）

**修改文件**：
1. `generate_report.py` - 添加 L0Table 折线图
2. HTML 模板 - 嵌入火焰图链接

---

## 5. 目录结构

```
benchmark/results/
├── raw/                          # 原始 JSON 数据
├── figures/                      # 图表（PDF/PNG）
├── reports/                      # HTML 报告
├── latency/                      # 延迟样本 CSV
├── profiles/                     # 火焰图（新增）
│   ├── flamegraph_cpu_hashmap.html
│   ├── flamegraph_cpu_forl0.html
│   ├── flamegraph_alloc_forl0.html
│   └── flamegraph_cache-misses_forl0.html  # Linux only
└── l0table/                      # L0Table 指标（新增）
    ├── l0table_metrics_forl0.csv       # 时序数据
    └── l0table_summary_forl0.json      # 汇总统计
```

---

## 6. 注意事项

### 6.1 平台兼容性

| 功能 | macOS | Linux |
|------|-------|-------|
| L0Table 指标 | ✅ | ✅ |
| Async Profiler CPU/alloc | ✅ | ✅ |
| Async Profiler cache-misses | ❌ | ✅ |

### 6.2 性能影响

| 功能 | 开销 | 说明 |
|------|------|------|
| L0Table 指标 | < 0.1% | 独立线程，1秒采样1次 |
| Async Profiler | 1-5% | 仅在显式启用时运行 |

### 6.3 启用方式

```bash
# 基础运行（无额外指标）
python run_benchmark.py --test wordcount --backend all

# 启用 L0Table 指标
python run_benchmark.py --test wordcount --backend forl0 --l0-metrics

# 启用火焰图
python run_benchmark.py --test wordcount --backend all --profile

# 全部启用
python run_benchmark.py --test wordcount --backend all --l0-metrics --profile
```
