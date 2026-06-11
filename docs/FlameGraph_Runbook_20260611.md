# Flame Graph Runbook 2026-06-11

本文档完全基于当前仓库现状重写，目标是让你在两类机器上都能直接照着操作：

1. 当前这台本地鲲鹏机器：无真实 L0，但已经成功生成 flame graph
2. 目标 L0 服务器：完全不联网，但你会把整个当前仓库原封不动克隆到它的 home 目录

本文档包含四部分：

1. 当前 GitHub 上已经有什么
2. 如何把文件同步到不联网的 L0 服务器
3. 什么时候需要 `async-profiler`
4. 本机上已验证通过的 flame graph 采集步骤

建议先统一设置以下路径变量，后面的命令都基于这些变量展开：

```bash
export REPO_ROOT=/path/to/forL0-state-backend
export FLINK_HOME=/path/to/your/flink
export ASYNC_PROFILER_HOME=/path/to/async-profiler-4.4-linux-arm64
```

如果你当前就在仓库根目录，也可以执行：

```bash
export REPO_ROOT="$(pwd)"
```

## 一、GitHub 上当前已经有的内容

截至当前状态，`main` 分支上已经有以下几类关键文件。

### 1. 核心三件套

这三件是最小可运行集合：

- `target/flink-statebackend-forL0-1.0-SNAPSHOT.jar`
- `src/main/resources/native/libforl0_engine.so`
- `benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar`

用途：

1. 在目标机上部署 ForL0 backend
2. 在目标机上运行 WordCount benchmark

### 2. 离线包

这是一份给不联网 L0 服务器准备的完整同步包：

- `docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz`
- `docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz.sha256`
- `docker/deploy/forl0-l0-offline-bundle-20260611.README.md`

它已经包含：

1. 核心三件套
2. `benchmark/config/`
3. `benchmark/scripts/`
4. `docker/conf/`
5. `docker/install_offline_bundle.sh`
6. `docker/docker_run.sh`
7. `docker/start.sh`
8. `docker/stop.sh`
9. `docker/restart.sh`
10. `tools/async-profiler-4.4-linux-arm64/`
11. 本 runbook 文档

### 3. 本机生成的 flame graph 结果

当前仓库中还包含已经产出的 flame graph 结果：

- `benchmark/results/profiles/hashmap_cpu.html`
- `benchmark/results/profiles/hashmap_cpu.collapsed`
- `benchmark/results/profiles/forl0_cpu.html`
- `benchmark/results/profiles/forl0_cpu.collapsed`

## 二、推荐方式

在你当前的交付方式下，最推荐的入口不是离线包，而是：

1. 把整个当前仓库原封不动放到目标 L0 服务器的 home 目录
2. 在目标机直接执行 `docker/server_setup.sh`
3. 部署完成后直接执行 `docker/run_all_apps.sh`

这样最省心，因为：

1. 不需要额外解压 tar 包
2. 不需要手工拷贝 JAR 和 native 库
3. 不需要再记一套离线包目录结构
4. 脚本会自动探测 `FLINK_HOME`，找不到时才要求你手工指定

你在目标 L0 服务器上的推荐执行方式是：

```bash
cd ~/forL0-state-backend/docker
./server_setup.sh
```

部署完成后，如果你希望一口气把当前仓库里能跑的 apps 全部跑完，直接执行：

```bash
cd ~/forL0-state-backend/docker
./run_all_apps.sh
```

如果你只想跑某一类应用，可以执行：

```bash
cd ~/forL0-state-backend/docker
./run_all_apps.sh --test wordcount
./run_all_apps.sh --test nexmark
./run_all_apps.sh --test benchset
./run_all_apps.sh --test client_usecase
./run_all_apps.sh --test unittest
```

如果你想只跑 ForL0 backend：

```bash
cd ~/forL0-state-backend/docker
./run_all_apps.sh --backend forl0
```

如果你想在全量运行时顺带抓 profiler：

```bash
cd ~/forL0-state-backend/docker
./run_all_apps.sh --profile cpu
```

如果自动找不到 Flink，再执行：

```bash
cd ~/forL0-state-backend/docker
./server_setup.sh --flink-home /path/to/your/flink
```

如果你只想先安装、不立刻拉起 Docker 集群：

```bash
cd ~/forL0-state-backend/docker
./server_setup.sh --no-start
```

## 三、两种交付方式

### 方式 A：只同步核心三件套

适用场景：

1. 目标机只需要运行 benchmark
2. 目标机不需要抓 flame graph
3. 你想手工控制 Flink 安装路径和部署位置

需要同步的文件：

```text
target/flink-statebackend-forL0-1.0-SNAPSHOT.jar
src/main/resources/native/libforl0_engine.so
benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar
```

### 方式 B：直接同步离线包

适用场景：

1. 目标 L0 服务器不联网
2. 希望一次性把运行脚本、配置、核心产物和 profiler 一起带过去
3. 希望减少手工漏文件的风险

需要同步的文件：

```text
docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz
docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz.sha256
docker/deploy/forl0-l0-offline-bundle-20260611.README.md
```

## 四、什么时候需要 `async-profiler`

### 场景 1：只跑 benchmark，不抓 flame graph

不需要额外关心 `async-profiler`。

你可以：

1. 只同步核心三件套
2. 或者直接同步离线包

### 场景 2：目标机要抓 flame graph

必须保证目标机可用 `async-profiler-4.4-linux-arm64`。

这时分两种情况：

1. 如果你只同步核心三件套：
   还需要另外准备 `async-profiler-4.4-linux-arm64`
2. 如果你直接同步离线包：
   不需要再单独准备，因为离线包里已经有 `tools/async-profiler-4.4-linux-arm64/`

一句话总结：

1. `async-profiler` 不在核心三件套里
2. `async-profiler` 已经在离线包里

## 五、目标 L0 服务器的环境前提

目标 L0 服务器即使不联网，也需要满足以下前提：

1. 目标机器是兼容的 Linux ARM64 / 鲲鹏环境
2. 已安装 Java
3. 已安装 Flink
4. 如果使用 Docker 方案，目标机已有 Docker 运行环境
5. 目标机有真实 L0 运行环境，例如设备节点和相关系统库

注意：

1. 离线包不包含系统级 L0 驱动
2. 离线包不包含 Docker 镜像本身
3. 离线包也不替代目标机的 Flink 安装

## 六、备选方案：直接同步离线包

如果你不是整仓库拷过去，而是只想带一个 tar 包，那就使用这个备选方案。

### 第 1 步：在本地确认离线包存在

在当前仓库根目录执行：

```bash
cd "$REPO_ROOT"

ls -lh docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz
ls -lh docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz.sha256
ls -lh docker/deploy/forl0-l0-offline-bundle-20260611.README.md
```

### 第 2 步：校验离线包摘要

在本地执行：

```bash
cd "$REPO_ROOT"

sha256sum docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz
cat docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz.sha256
```

如果你想直接自动校验：

```bash
cd "$REPO_ROOT/docker/deploy"
sha256sum -c forl0-l0-offline-bundle-20260611.tar.gz.sha256
```

### 第 3 步：把离线包传到目标 L0 服务器

目标 L0 服务器完全不联网，因此这里默认只能使用你们现有的离线拷贝方式，把这三个文件带过去：

```text
forl0-l0-offline-bundle-20260611.tar.gz
forl0-l0-offline-bundle-20260611.tar.gz.sha256
forl0-l0-offline-bundle-20260611.README.md
```

### 第 4 步：在目标机创建工作目录

以下命令在目标 L0 服务器执行：

```bash
mkdir -p ~/forl0-offline
cd ~/forl0-offline
```

### 第 5 步：将离线包放入工作目录

假设你已经把文件拷到了 `~/forl0-offline/`：

```bash
cd ~/forl0-offline
ls -lh
```

应该至少能看到：

```text
forl0-l0-offline-bundle-20260611.tar.gz
forl0-l0-offline-bundle-20260611.tar.gz.sha256
forl0-l0-offline-bundle-20260611.README.md
```

### 第 6 步：在目标机校验离线包

在目标机执行：

```bash
cd ~/forl0-offline
sha256sum forl0-l0-offline-bundle-20260611.tar.gz
cat forl0-l0-offline-bundle-20260611.tar.gz.sha256
```

如果两边摘要一致，就说明离线拷贝没有损坏。

### 第 7 步：解压离线包

在目标机执行：

```bash
cd ~/forl0-offline
tar xzf forl0-l0-offline-bundle-20260611.tar.gz
```

### 第 8 步：检查解压后的目录结构

在目标机执行：

```bash
cd ~/forl0-offline
find forl0-l0-offline-bundle-20260611 -maxdepth 2 -type d | sort
```

你应该能看到这些关键目录：

```text
forl0-l0-offline-bundle-20260611/artifacts
forl0-l0-offline-bundle-20260611/benchmark
forl0-l0-offline-bundle-20260611/docker
forl0-l0-offline-bundle-20260611/tools
forl0-l0-offline-bundle-20260611/docs
```

### 第 9 步：执行一键安装脚本

以下命令在目标 L0 服务器执行：

```bash
cd ~/forl0-offline/forl0-l0-offline-bundle-20260611

chmod +x docker/install_offline_bundle.sh

./docker/install_offline_bundle.sh \
  --flink-home /path/to/your/flink \
  --install-dir ~/forl0-runtime \
  --copy-profiler
```

如果你希望安装完后立刻拉起 Docker 集群，可以执行：

```bash
cd ~/forl0-offline/forl0-l0-offline-bundle-20260611

./docker/install_offline_bundle.sh \
  --flink-home /path/to/your/flink \
  --install-dir ~/forl0-runtime \
  --copy-profiler \
  --start-docker
```

这个脚本会完成以下动作：

1. 将 backend JAR 复制到 `$FLINK_HOME/lib/`
2. 将 `libforl0_engine.so` 复制到 `$FLINK_HOME/native/`
3. 将 benchmark 脚本、docker 脚本、配置文件复制到 `~/forl0-runtime/`
4. 生成 `~/forl0-runtime/forl0-offline.env`
5. 可选复制 profiler
6. 可选直接启动 Docker 集群

### 第 10 步：检查核心产物是否齐全

在目标机执行：

```bash
cd ~/forl0-runtime

source ~/forl0-runtime/forl0-offline.env

ls -lh artifacts/flink-statebackend-forL0-1.0-SNAPSHOT.jar
ls -lh artifacts/libforl0_engine.so
ls -lh artifacts/wordcount-benchmark-1.0-SNAPSHOT.jar
ls -lh docker/docker_run.sh
ls -lh docs/FlameGraph_Runbook_20260611.md
ls -lh "$FLINK_HOME/lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
ls -lh "$FLINK_HOME/native/libforl0_engine.so"
```

### 第 11 步：如果需要 flame graph，检查 profiler 是否齐全

在目标机执行：

```bash
cd ~/forl0-runtime

ls -lh tools/async-profiler-4.4-linux-arm64/bin/asprof
ls -lh tools/async-profiler-4.4-linux-arm64/bin/jfrconv
ls -lh tools/async-profiler-4.4-linux-arm64/lib/libasyncProfiler.so
```

### 第 12 步：加载运行环境并启动 Docker 集群

在目标机执行：

```bash
source ~/forl0-runtime/forl0-offline.env
cd ~/forl0-runtime/docker

./docker_run.sh start
```


如果只想先检查状态：

```bash
source ~/forl0-runtime/forl0-offline.env
cd ~/forl0-runtime/docker

./docker_run.sh status
```

## 七、备选方案：只同步核心三件套

如果你不想同步整个离线包，也可以只同步三件套。

### 第 1 步：在本地确认三件套都存在

```bash
cd "$REPO_ROOT"

ls -lh target/flink-statebackend-forL0-1.0-SNAPSHOT.jar
ls -lh src/main/resources/native/libforl0_engine.so
ls -lh benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar
```

### 第 2 步：将三件套拷到目标机

目标机如果不联网，就按你们现有的离线方式拷这三个文件：

```text
flink-statebackend-forL0-1.0-SNAPSHOT.jar
libforl0_engine.so
wordcount-benchmark-1.0-SNAPSHOT.jar
```

### 第 3 步：在目标机创建工作目录

```bash
mkdir -p ~/forl0-manual/artifacts
cd ~/forl0-manual/artifacts
```

### 第 4 步：把三件套放进该目录

```bash
ls -lh
```

你应该看到：

```text
flink-statebackend-forL0-1.0-SNAPSHOT.jar
libforl0_engine.so
wordcount-benchmark-1.0-SNAPSHOT.jar
```

### 第 5 步：手工部署到 Flink

```bash
export FLINK_HOME=/path/to/your/flink

mkdir -p "$FLINK_HOME/native"

cp ~/forl0-manual/artifacts/flink-statebackend-forL0-1.0-SNAPSHOT.jar "$FLINK_HOME/lib/"
cp ~/forl0-manual/artifacts/libforl0_engine.so "$FLINK_HOME/native/"
```

### 第 6 步：放置 benchmark JAR

```bash
mkdir -p ~/forl0-benchmark-artifacts
cp ~/forl0-manual/artifacts/wordcount-benchmark-1.0-SNAPSHOT.jar ~/forl0-benchmark-artifacts/
```

### 第 7 步：如果还要抓 flame graph，则额外同步 profiler

只有在“只同步三件套但目标机还要抓 flame graph”时，才需要再额外带上：

```text
async-profiler-4.4-linux-arm64/
```

## 八、本机上已验证通过的 flame graph 方案

下面这部分是已经在当前无真实 L0 的鲲鹏机器上实际跑通过的方法。

### 第 1 步：为什么不能直接依赖脚本的 `--profile cpu`

虽然脚本支持：

```bash
python3 run_benchmark.py --test wordcount --backend all --profile cpu
```

但在当前环境里，直接这样跑不能稳定产出 flame graph，原因是：

1. 最初没有安装 `async-profiler`
2. 即使 host 上安装了 profiler，TaskManager 运行在 Docker 容器里，容器内部看不到 host 上的 profiler 目录

所以最终采用的是“容器内 attach”方案。

### 第 2 步：在 host 安装 profiler

```bash
mkdir -p "$(dirname \"$ASYNC_PROFILER_HOME\")"
cd "$(dirname \"$ASYNC_PROFILER_HOME\")"

curl -L -o /tmp/async-profiler-4.4-linux-arm64.tar.gz \
  https://github.com/async-profiler/async-profiler/releases/download/v4.4/async-profiler-4.4-linux-arm64.tar.gz

tar xzf /tmp/async-profiler-4.4-linux-arm64.tar.gz

"$ASYNC_PROFILER_HOME/bin/asprof" --version
```

### 第 3 步：将 profiler 复制进两个 TaskManager 容器

```bash
sudo docker cp "$ASYNC_PROFILER_HOME" flink-taskmanager-1:/opt/async-profiler-4.4-linux-arm64
sudo docker cp "$ASYNC_PROFILER_HOME" flink-taskmanager-2:/opt/async-profiler-4.4-linux-arm64
```

### 第 4 步：验证容器里 profiler 可用

```bash
sudo docker exec flink-taskmanager-1 /opt/async-profiler-4.4-linux-arm64/bin/asprof --version
sudo docker exec flink-taskmanager-2 /opt/async-profiler-4.4-linux-arm64/bin/asprof --version
```

### 第 5 步：确认容器内 TaskManager 进程 PID

```bash
sudo docker exec flink-taskmanager-1 bash -lc \
  'ps -eo pid,cmd | grep -E "TaskManagerRunner|java" | grep -v grep'
```

当前环境下 PID 是 `1`。

### 第 6 步：提交 HashMap benchmark

```bash
cd "$REPO_ROOT/benchmark/scripts"

FLINK_HOME="$FLINK_HOME" \
"$FLINK_HOME/bin/flink" run -d \
  -Dstate.backend.type=org.apache.flink.runtime.state.hashmap.HashMapStateBackendFactory \
  "$REPO_ROOT/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar" \
  --numKeys 2000000 \
  --numRecords 2000000000 \
  --arrivalRate 0 \
  --skewFactor 0 \
  --parallelism 8 \
  --checkpointInterval 0 \
  --backend hashmap
```

### 第 7 步：在容器内录制 HashMap JFR

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

### 第 8 步：将 HashMap JFR 转为 flame graph 和 collapsed

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

### 第 9 步：将 HashMap 结果拷回工作区

```bash
mkdir -p "$REPO_ROOT/benchmark/results/profiles"

sudo docker cp \
  flink-taskmanager-1:/tmp/forl0-profiles/export/hashmap_cpu.html \
  "$REPO_ROOT/benchmark/results/profiles/hashmap_cpu.html"

sudo docker cp \
  flink-taskmanager-1:/tmp/forl0-profiles/export/hashmap_cpu.collapsed \
  "$REPO_ROOT/benchmark/results/profiles/hashmap_cpu.collapsed"
```

### 第 10 步：提交 ForL0 benchmark

```bash
cd "$REPO_ROOT/benchmark/scripts"

FLINK_HOME="$FLINK_HOME" \
"$FLINK_HOME/bin/flink" run -d \
  -Dstate.backend.type=org.apache.flink.state.forl0.ForL0StateBackendFactory \
  "$REPO_ROOT/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar" \
  --numKeys 2000000 \
  --numRecords 2000000000 \
  --arrivalRate 0 \
  --skewFactor 0 \
  --parallelism 8 \
  --checkpointInterval 0 \
  --backend forl0
```

### 第 11 步：在容器内录制 ForL0 JFR

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

### 第 12 步：将 ForL0 JFR 转为 flame graph 和 collapsed

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

### 第 13 步：将 ForL0 结果拷回工作区

```bash
sudo docker cp \
  flink-taskmanager-1:/tmp/forl0-profiles/export/forl0_cpu.html \
  "$REPO_ROOT/benchmark/results/profiles/forl0_cpu.html"

sudo docker cp \
  flink-taskmanager-1:/tmp/forl0-profiles/export/forl0_cpu.collapsed \
  "$REPO_ROOT/benchmark/results/profiles/forl0_cpu.collapsed"
```

## 九、当前主要结果文件

当前与本 runbook 直接相关的主要文件如下：

- `docs/FlameGraph_Runbook_20260611.md`
- `target/flink-statebackend-forL0-1.0-SNAPSHOT.jar`
- `src/main/resources/native/libforl0_engine.so`
- `benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar`
- `docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz`
- `docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz.sha256`
- `docker/deploy/forl0-l0-offline-bundle-20260611.README.md`
- `docker/server_setup.sh`
- `docker/run_all_apps.sh`
- `docker/install_offline_bundle.sh`
- `benchmark/results/profiles/hashmap_cpu.html`
- `benchmark/results/profiles/hashmap_cpu.collapsed`
- `benchmark/results/profiles/forl0_cpu.html`
- `benchmark/results/profiles/forl0_cpu.collapsed`

## 十、补充说明

### 1. 当前本机没有真实 L0

当前这台本地鲲鹏机器没有真实 L0 设备，因此本机 benchmark 和 flame graph 反映的是“无真实 L0 硬件加速”的情况。

### 2. 目标机不联网不是阻塞点

目标 L0 服务器不联网不是问题，当前推荐方案就是直接同步离线包。

### 3. 为什么当前只有约 18% 提升

从已生成的 flame graph 和 collapsed stacks 看，ForL0 减少了 heap state table 的开销，但端到端 CPU 仍主要消耗在 Flink 网络路径、source 和序列化上，因此 backend 优化只能改善总成本中的一部分，最终提升约 18%。

### 4. `benchmark/results/` 默认被忽略

如果需要将 flame graph HTML 和 collapsed stacks 一并提交，需要显式执行：

```bash
git add -f benchmark/results/profiles/hashmap_cpu.html \
           benchmark/results/profiles/hashmap_cpu.collapsed \
           benchmark/results/profiles/forl0_cpu.html \
           benchmark/results/profiles/forl0_cpu.collapsed
```