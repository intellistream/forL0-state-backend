# Flame Graph 生成与本次变更说明

本文档记录 2026-06-11 在当前鲲鹏服务器上，如何为 WordCount benchmark 成功生成 `hashmap` 和 `forl0` 两种后端的 CPU flame graph，以及本次实际做过的环境和产物变更。

## 一、目标

在这台机器上成功生成以下产物：

- `benchmark/results/profiles/hashmap_cpu.html`
- `benchmark/results/profiles/hashmap_cpu.collapsed`
- `benchmark/results/profiles/forl0_cpu.html`
- `benchmark/results/profiles/forl0_cpu.collapsed`

并基于 flame graph 分析为什么当前 ForL0 相比 HashMap 只提升约 18%。

## 二、本次实际变更

本次操作中，工作区内涉及到的主要变更如下：

### 1. Native 库重新编译

由于当前机器是 ARM64 / aarch64，而仓库中原先的 native 产物不能直接用于当前环境，因此重新编译并覆盖了以下文件：

- `src/main/native/libforl0_engine.so`
- `src/main/resources/native/libforl0_engine.so`
- `src/main/native/engine/hot_cache.o`
- `src/main/native/jni/forl0_jni.o`
- `src/main/native/jni/jni_checkpoint.o`
- `src/main/native/jni/jni_list_state.o`
- `src/main/native/jni/jni_map_state.o`
- `src/main/native/jni/jni_tw_state.o`
- `src/main/native/jni/jni_value_state.o`

### 2. 生成了 benchmark profiling 结果

本次成功生成的 flame graph 和 collapsed stacks 位于：

- `benchmark/results/profiles/hashmap_cpu.html`
- `benchmark/results/profiles/hashmap_cpu.collapsed`
- `benchmark/results/profiles/forl0_cpu.html`
- `benchmark/results/profiles/forl0_cpu.collapsed`

注意：`benchmark/results/` 在仓库中默认被 `.gitignore` 忽略，如需提交这些产物，必须使用 `git add -f`。

### 3. 新增本文档

新增文档：

- `docs/FlameGraph_Runbook_20260611.md`

## 三、为什么不能直接用 benchmark 脚本自动出图

虽然脚本支持：

```bash
python3 run_benchmark.py --test wordcount --backend all --profile cpu
```

但在这台机器上直接这样跑，没有稳定产出 flame graph，原因有两个：

1. 最初没有安装 `async-profiler`
2. 即使 host 上安装了 `async-profiler`，TaskManager 运行在 Docker 容器里，容器内部看不到 host 上的 `/home/shuhao/async-profiler-4.4-linux-arm64`，导致 attach 失败

因此最终采用的可靠方案是：

1. 在 host 上安装 `async-profiler`
2. 将 profiler 复制进 TaskManager 容器
3. 用 detached 模式提交 benchmark
4. 在容器内对 TaskManager JVM 直接采样
5. 用 `jfrconv` 将 JFR 转成 HTML flame graph 和 collapsed stacks

## 四、环境前提

需要满足以下条件：

1. Flink 已部署在 `/home/shuhao/flink-1.20.3`
2. Docker 中已启动 1 个 JobManager 和 2 个 TaskManager
3. WordCount JAR 已存在
4. ForL0 backend JAR 已放到 Flink `lib/`
5. native 库已为当前 ARM64 架构重新编译

检查命令如下：

```bash
ls -lh /home/shuhao/flink-1.20.3/bin/flink
ls -lh /home/shuhao/forL0-state-backend/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar
ls -lh /home/shuhao/flink-1.20.3/lib/flink-statebackend-forl0-1.0-SNAPSHOT.jar
ls -lh /home/shuhao/forL0-state-backend/src/main/resources/native/libforl0_engine.so
curl http://localhost:8081/overview
sudo docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
```

## 五、安装 async-profiler 到 host

本次使用版本：`async-profiler-4.4-linux-arm64`

```bash
cd /home/shuhao
curl -L -o /tmp/async-profiler-4.4-linux-arm64.tar.gz \
  https://github.com/async-profiler/async-profiler/releases/download/v4.4/async-profiler-4.4-linux-arm64.tar.gz
tar xzf /tmp/async-profiler-4.4-linux-arm64.tar.gz
/home/shuhao/async-profiler-4.4-linux-arm64/bin/asprof --version
```

## 六、将 profiler 复制进 TaskManager 容器

```bash
sudo docker cp /home/shuhao/async-profiler-4.4-linux-arm64 flink-taskmanager-1:/opt/async-profiler-4.4-linux-arm64
sudo docker cp /home/shuhao/async-profiler-4.4-linux-arm64 flink-taskmanager-2:/opt/async-profiler-4.4-linux-arm64
```

验证命令：

```bash
sudo docker exec flink-taskmanager-1 /opt/async-profiler-4.4-linux-arm64/bin/asprof --version
sudo docker exec flink-taskmanager-2 /opt/async-profiler-4.4-linux-arm64/bin/asprof --version
```

## 七、确认 TaskManager JVM 进程

本次容器环境中，TaskManager Java 进程即 PID `1`。

确认命令：

```bash
sudo docker exec flink-taskmanager-1 bash -lc \
  'ps -eo pid,cmd | grep -E "TaskManagerRunner|java" | grep -v grep'
```

## 八、采集 HashMap backend flame graph

### 1. 提交 detached benchmark

```bash
cd /home/shuhao/forL0-state-backend/benchmark/scripts

FLINK_HOME=/home/shuhao/flink-1.20.3 \
/home/shuhao/flink-1.20.3/bin/flink run -d \
  -Dstate.backend.type=org.apache.flink.runtime.state.hashmap.HashMapStateBackendFactory \
  /home/shuhao/forL0-state-backend/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar \
  --numKeys 2000000 \
  --numRecords 2000000000 \
  --arrivalRate 0 \
  --skewFactor 0 \
  --parallelism 8 \
  --checkpointInterval 0 \
  --backend hashmap
```

### 2. 在容器内录制 CPU JFR

```bash
sudo docker exec flink-taskmanager-1 bash -lc '
  mkdir -p /tmp/forl0-profiles &&
  /opt/async-profiler-4.4-linux-arm64/bin/asprof \
    -d 460 \
    -e cpu \
    -o jfr \
    -f /tmp/forl0-profiles/hashmap_cpu.jfr \
    1
'
```

### 3. 转换为 flame graph 与 collapsed stacks

```bash
sudo docker exec flink-taskmanager-1 bash -lc '
  mkdir -p /tmp/forl0-profiles/export &&
  /opt/async-profiler-4.4-linux-arm64/bin/jfrconv \
    --cpu \
    -o html \
    /tmp/forl0-profiles/hashmap_cpu.jfr \
    /tmp/forl0-profiles/export/hashmap_cpu.html &&
  /opt/async-profiler-4.4-linux-arm64/bin/jfrconv \
    --cpu \
    -o collapsed \
    /tmp/forl0-profiles/hashmap_cpu.jfr \
    /tmp/forl0-profiles/export/hashmap_cpu.collapsed
'
```

### 4. 将产物拷回工作区

```bash
mkdir -p /home/shuhao/forL0-state-backend/benchmark/results/profiles

sudo docker cp \
  flink-taskmanager-1:/tmp/forl0-profiles/export/hashmap_cpu.html \
  /home/shuhao/forL0-state-backend/benchmark/results/profiles/hashmap_cpu.html

sudo docker cp \
  flink-taskmanager-1:/tmp/forl0-profiles/export/hashmap_cpu.collapsed \
  /home/shuhao/forL0-state-backend/benchmark/results/profiles/hashmap_cpu.collapsed
```

## 九、采集 ForL0 backend flame graph

### 1. 提交 detached benchmark

注意，这里手工提交时使用的是完整工厂类名，而不是简写 `forl0`：

```bash
cd /home/shuhao/forL0-state-backend/benchmark/scripts

FLINK_HOME=/home/shuhao/flink-1.20.3 \
/home/shuhao/flink-1.20.3/bin/flink run -d \
  -Dstate.backend.type=org.apache.flink.state.forl0.ForL0StateBackendFactory \
  /home/shuhao/forL0-state-backend/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar \
  --numKeys 2000000 \
  --numRecords 2000000000 \
  --arrivalRate 0 \
  --skewFactor 0 \
  --parallelism 8 \
  --checkpointInterval 0 \
  --backend forl0
```

### 2. 在容器内录制 CPU JFR

```bash
sudo docker exec flink-taskmanager-1 bash -lc '
  mkdir -p /tmp/forl0-profiles &&
  /opt/async-profiler-4.4-linux-arm64/bin/asprof \
    -d 420 \
    -e cpu \
    -o jfr \
    -f /tmp/forl0-profiles/forl0_cpu.jfr \
    1
'
```

### 3. 转换为 flame graph 与 collapsed stacks

```bash
sudo docker exec flink-taskmanager-1 bash -lc '
  mkdir -p /tmp/forl0-profiles/export &&
  /opt/async-profiler-4.4-linux-arm64/bin/jfrconv \
    --cpu \
    -o html \
    /tmp/forl0-profiles/forl0_cpu.jfr \
    /tmp/forl0-profiles/export/forl0_cpu.html &&
  /opt/async-profiler-4.4-linux-arm64/bin/jfrconv \
    --cpu \
    -o collapsed \
    /tmp/forl0-profiles/forl0_cpu.jfr \
    /tmp/forl0-profiles/export/forl0_cpu.collapsed
'
```

### 4. 将产物拷回工作区

```bash
sudo docker cp \
  flink-taskmanager-1:/tmp/forl0-profiles/export/forl0_cpu.html \
  /home/shuhao/forL0-state-backend/benchmark/results/profiles/forl0_cpu.html

sudo docker cp \
  flink-taskmanager-1:/tmp/forl0-profiles/export/forl0_cpu.collapsed \
  /home/shuhao/forL0-state-backend/benchmark/results/profiles/forl0_cpu.collapsed
```

## 十、结果位置

最终结果位于：

- `docs/FlameGraph_Runbook_20260611.md`
- `benchmark/results/profiles/hashmap_cpu.html`
- `benchmark/results/profiles/hashmap_cpu.collapsed`
- `benchmark/results/profiles/forl0_cpu.html`
- `benchmark/results/profiles/forl0_cpu.collapsed`

## 十一、补充说明

### 1. 本机没有真实 L0

本次排查中，按 `/dev/l0` 是否存在作为判断标准，当前机器没有真实 L0 设备，因此本次 profile 和 benchmark 结果不包含真实 L0 硬件带来的收益。

### 2. 为什么只有约 18% 提升

从生成的 flame graph 和 collapsed stacks 看，ForL0 确实减少了 heap state table 的开销，但端到端 CPU 仍然主要消耗在 Flink 网络路径、source 和序列化上，因此 backend 优化只能改善总成本中的一部分，最终整体提升约 18%。

### 3. benchmark/results 默认被忽略

如果希望将 flame graph HTML 和 collapsed stacks 一并提交，需要显式执行：

```bash
git add -f benchmark/results/profiles/hashmap_cpu.html \
           benchmark/results/profiles/hashmap_cpu.collapsed \
           benchmark/results/profiles/forl0_cpu.html \
           benchmark/results/profiles/forl0_cpu.collapsed
```