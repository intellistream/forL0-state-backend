# 鲲鹏服务器部署指南

本文档介绍如何在无法上网的鲲鹏 (openEuler) 服务器上部署 ForL0 StateBackend 并运行 Benchmark 测试。

## 环境要求

### 服务器端 (openEuler/鲲鹏)
- **OS**: openEuler (ARM64/aarch64)
- **Java**: JDK 8 或 JDK 11
- **Flink**: 1.20.x (已部署)
- **GCC**: 用于编译 native 库
- **Python**: 3.6+ (通常系统自带)

---

## 第一步：在 macOS 开发机编译

```bash
cd /Users/jinyunyang/IdeaProjects/forL0-state-backend

# 1. 编译 ForL0 StateBackend
mvn clean package -DskipTests

# 2. 编译 WordCount
cd benchmark/wordcount
mvn clean package -DskipTests

# 3. 编译 NexMark
cd ../nexmark-src
mvn clean package -DskipTests
```

---

## 第二步：打包部署文件

```bash
cd /Users/jinyunyang/IdeaProjects/forL0-state-backend

mkdir -p ~/forl0-deploy

# 核心文件
cp target/flink-statebackend-forL0-1.0-SNAPSHOT.jar ~/forl0-deploy/
cp -r src/main/native ~/forl0-deploy/native-src
cp benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar ~/forl0-deploy/
cp -r benchmark/nexmark-src/nexmark-flink/target/nexmark-flink-bin/nexmark-flink ~/forl0-deploy/

# 脚本和配置
cp -r benchmark/scripts ~/forl0-deploy/
cp -r benchmark/config ~/forl0-deploy/

# 打包
cd ~
tar czvf forl0-deploy.tar.gz forl0-deploy/
```

---

## 第三步：传输到服务器

通过 Windows 中转：

```powershell
# 从 macOS 下载到 Windows
scp user@mac-ip:~/forl0-deploy.tar.gz .

# 从 Windows 上传到服务器
scp forl0-deploy.tar.gz user@server-ip:~/
```

---

## 第四步：服务器部署

SSH 登录服务器：

### 4.1 解压

```bash
cd ~
tar xzvf forl0-deploy.tar.gz
cd forl0-deploy
```

### 4.2 编译 Native 库

```bash
cd native-src

# 检查 L0 库是否存在
ls -la /usr/lib64/libl0mempool.so 2>/dev/null && echo "✓ L0 mode" || echo "× Simulation mode"

# 编译
make clean && make

# 验证
ls -la libforl0_native.so
```

### 4.3 安装到 Flink

```bash
export FLINK_HOME=/path/to/flink  # 修改为实际路径

# 安装 JAR 和 native 库
cp ~/forl0-deploy/flink-statebackend-forL0-1.0-SNAPSHOT.jar $FLINK_HOME/lib/
cp ~/forl0-deploy/native-src/libforl0_native.so $FLINK_HOME/lib/
```

### 4.4 配置 Flink

编辑 `$FLINK_HOME/conf/flink-conf.yaml`：

```yaml
# 添加 native 库路径
env.java.opts.taskmanager: "-Djava.library.path=/path/to/flink/lib"
```

### 4.5 重启 Flink

```bash
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

---

## 第五步：配置 Benchmark

编辑 `~/forl0-deploy/config/benchmark.yaml`：

```yaml
mode: cluster

flink_home: /path/to/flink  # 修改为实际路径

cluster:
  parallelism: 8  # 根据 CPU 核数调整
  
  wordcount:
    num_keys: 10000000
    num_records: 100000000
    arrival_rate: 1000000
    skew_factor: 1.1
    window_size: 5000
    slide_size: 200
  
  nexmark:
    events_num: 100000000
    tps_limit: 10000000
    queries: "q4,q5,q7,q8"
```

---

## 第六步：安装 Python 依赖

```bash
# 检查 Python
python3 --version

# 安装依赖 (最小依赖)
pip3 install --user pyyaml

# 可选：安装图表依赖 (生成报告需要)
pip3 install --user matplotlib numpy
```

如果没有 pip，可以离线安装（见附录）。

---

## 第七步：安装 Async Profiler（--profile 必需）

使用 `--profile` 选项需要安装 Async Profiler：

```bash
cd ~/forl0-deploy

# 下载 Async Profiler (Linux ARM64 版本)
# 需要提前在有网的机器下载，然后传输到服务器
# 下载地址: https://github.com/async-profiler/async-profiler/releases
# 选择: async-profiler-3.0-linux-arm64.tar.gz

# 解压
tar xzvf async-profiler-3.0-linux-arm64.tar.gz
mv async-profiler-3.0-linux-arm64 tools/async-profiler

# 设置环境变量
export ASYNC_PROFILER_HOME=~/forl0-deploy/tools/async-profiler

# 验证
ls $ASYNC_PROFILER_HOME/bin/asprof
```

### 配置 perf_events（采集 CPU cache 统计需要）

```bash
# 检查 perf 是否可用
perf stat -e cache-misses ls 2>&1 | head -5

# 如果报权限错误，需要调整内核参数（需要 root）
sudo sysctl kernel.perf_event_paranoid=1

# 或永久设置
echo "kernel.perf_event_paranoid = 1" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

---

## 第八步：运行 Benchmark

```bash
cd ~/forl0-deploy/scripts

# 设置环境
export FLINK_HOME=/path/to/flink
export ASYNC_PROFILER_HOME=~/forl0-deploy/tools/async-profiler  # --profile 需要

# ========== 基础运行（无性能分析）==========
python3 run_benchmark.py --test all --backend all

# ========== 使用 --profile（推荐）==========
# 启用火焰图 + 硬件统计采集
python3 run_wordcount.py --backend all --profile
python3 run_nexmark.py --backend all --profile

# 或一起运行
python3 run_benchmark.py --test all --backend all --profile

# 生成报告（包含火焰图和硬件统计图表）
python3 generate_report.py
```

### --profile 采集的指标

| 指标类型 | 说明 | 输出文件 |
|---------|------|----------|
| CPU 火焰图 | 热点函数分析 | `results/profiles/*_cpu.html` |
| Alloc 火焰图 | 内存分配分析 | `results/profiles/*_alloc.html` |
| 内存使用时间线 | RSS 内存变化 | `results/hardware/*_memory_*.json` |
| CPU Cache 统计 | cache-misses 等 | `results/hardware/*_cache_*.json` |

### 火焰图查看

```bash
# 列出生成的火焰图
ls ~/forl0-deploy/results/profiles/*.html

# 打包下载到本地用浏览器查看
tar czvf profiles.tar.gz results/profiles/
```

---

## 第九步：查看结果

```bash
# 查看摘要
cat ~/forl0-deploy/results/reports/benchmark_report.html

# 打包结果下载到本地查看
cd ~/forl0-deploy
tar czvf results.tar.gz results/
```

下载到 Windows 后用浏览器打开 HTML 报告。

### 报告内容（使用 --profile 时）

1. **性能对比表格** - 吞吐量、延迟
2. **L0 命中率图表** - ForL0 热点缓存效果
3. **火焰图** - CPU 和内存分配热点
4. **内存使用时间线** - 各 Query 内存变化对比（hashmap 虚线，forl0 实线）
5. **CPU Cache 统计** - cache-misses 对比（仅 Linux）

---

## 验证 L0 模式

```bash
# 查看 TaskManager 日志
grep -i "ForL0\|L0 mode" $FLINK_HOME/log/flink-*-taskexecutor-*.log
```

预期输出：
```
[ForL0] Native library loaded successfully
[ForL0] Running in L0 mode (Kunpeng L0 Cache enabled)
```

如果显示 `simulation mode`，说明 L0 库未安装，但 ForL0 仍可正常工作。

---

## 常见问题

### Native 库加载失败

```bash
# 检查库文件
ls -la $FLINK_HOME/lib/libforl0_native.so

# 检查依赖
ldd $FLINK_HOME/lib/libforl0_native.so
```

### 没有 pip

离线安装 PyYAML：
```bash
# 在有网的机器下载
pip download pyyaml -d ./packages

# 传输到服务器后安装
pip3 install --user --no-index --find-links=./packages pyyaml
```

---

## 快速命令

```bash
# 环境设置
export FLINK_HOME=/path/to/flink
export ASYNC_PROFILER_HOME=~/forl0-deploy/tools/async-profiler
cd ~/forl0-deploy/scripts

# 运行测试 (带性能分析)
python3 run_benchmark.py --test all --backend all --profile

# 查看 L0 命中率
grep "hit rate" $FLINK_HOME/log/flink-*-taskexecutor-*.log

# 打包所有结果
cd ~/forl0-deploy && tar czvf results.tar.gz results/
```

---

## 离线准备 Async Profiler

在有网络的机器上下载：

```bash
# Linux ARM64 版本
wget https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-arm64.tar.gz

# 传输到服务器
scp async-profiler-3.0-linux-arm64.tar.gz user@server:~/forl0-deploy/
```
