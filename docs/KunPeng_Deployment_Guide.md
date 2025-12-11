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
cp target/flink-statebackend-forl0-1.0-SNAPSHOT.jar ~/forl0-deploy/
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
cp ~/forl0-deploy/flink-statebackend-forl0-1.0-SNAPSHOT.jar $FLINK_HOME/lib/
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

## 第七步：运行 Benchmark

```bash
cd ~/forl0-deploy/scripts

# 设置环境
export FLINK_HOME=/path/to/flink

# 运行所有测试
python3 run_benchmark.py --test all --backend all

# 或分开运行
python3 run_wordcount.py --backend all
python3 run_nexmark.py --backend all

# 生成报告
python3 generate_report.py
```

---

## 第八步：查看结果

```bash
# 查看摘要
cat ~/forl0-deploy/results/reports/benchmark_report.html

# 打包结果下载到本地查看
cd ~/forl0-deploy
tar czvf results.tar.gz results/
```

下载到 Windows 后用浏览器打开 HTML 报告。

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
cd ~/forl0-deploy/scripts

# 运行测试
python3 run_benchmark.py --test all --backend all

# 查看 L0 命中率
grep "hit rate" $FLINK_HOME/log/flink-*-taskexecutor-*.log
```
