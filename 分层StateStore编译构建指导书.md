# 分层 StateStore 编译构建指导书

| 版本 | 日期 | 作者 | 说明 |
|------|------|------|------|
| v1.0 | 2026-04-14 | ForL0 Team | 初始版本 |

---

## 1. 概述

本文档指导如何在鲲鹏（Kunpeng）ARM64 服务器上编译构建 ForL0StateBackend 的所有组件，包括 Java 主体工程、C++ 原生引擎、WordCount Benchmark 和 NexMark Benchmark。

### 1.1 构建产物

| 产物 | 说明 | 路径 |
|------|------|------|
| `flink-statebackend-forL0-1.0-SNAPSHOT.jar` | ForL0 状态后端 JAR 包 | `target/` |
| `libforl0_engine.so` | C++ 原生引擎动态库 (aarch64) | `src/main/resources/native/` |
| `wordcount-benchmark-*.jar` | WordCount 性能测试 JAR | `benchmark/wordcount/target/` |
| `nexmark-datastream-*.jar` | NexMark 性能测试 JAR | `benchmark/nexmark-src/nexmark-flink/nexmark-datastream/target/` |

### 1.2 目标平台

| 项目 | 要求 |
|------|------|
| 硬件 | 鲲鹏 920 / 鲲鹏 930 ARM64 服务器 |
| 操作系统 | openEuler 20.03+ / CentOS 7.6+ (aarch64) |
| 架构 | AArch64 (ARM64) |

---

## 2. 环境准备

### 2.1 基础工具安装

```bash
# openEuler / CentOS
sudo yum install -y gcc gcc-c++ cmake3 make

# 确认 GCC 版本 >= 7.0（C++17 支持）
gcc --version

# 如果 cmake3 命令名为 cmake3，创建软链接
sudo ln -sf /usr/bin/cmake3 /usr/bin/cmake
```

### 2.2 JDK 安装

安装 JDK 1.8+ (aarch64 版本)：

```bash
# 使用 yum 安装 OpenJDK
sudo yum install -y java-1.8.0-openjdk-devel

# 或使用华为毕昇 JDK（推荐，鲲鹏优化）
# 从华为镜像站下载 BiSheng JDK 1.8 aarch64 版本
# https://www.hikunpeng.com/developer/devkit/compiler/jdk

# 配置 JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# 验证
java -version
javac -version
```

### 2.3 Maven 安装

安装 Maven 3.6+：

```bash
# 下载 Maven
wget https://archive.apache.org/dist/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz
tar xzf apache-maven-3.8.8-bin.tar.gz -C /opt/
export MAVEN_HOME=/opt/apache-maven-3.8.8
export PATH=$MAVEN_HOME/bin:$PATH

# 验证
mvn --version
```

### 2.4 Apache Flink 安装

```bash
# 下载 Flink 1.20.3
wget https://archive.apache.org/dist/flink/flink-1.20.3/flink-1.20.3-bin-scala_2.12.tgz
tar xzf flink-1.20.3-bin-scala_2.12.tgz -C /opt/
export FLINK_HOME=/opt/flink-1.20.3

# 验证
$FLINK_HOME/bin/flink --version
```

### 2.5 CMake 版本要求

C++ 原生引擎需要 CMake 3.14+：

```bash
cmake --version
# 如果版本低于 3.14，从源码编译：
wget https://github.com/Kitware/CMake/releases/download/v3.28.0/cmake-3.28.0-linux-aarch64.tar.gz
tar xzf cmake-3.28.0-linux-aarch64.tar.gz -C /opt/
export PATH=/opt/cmake-3.28.0-linux-aarch64/bin:$PATH
```

### 2.6 Python 环境（Benchmark 脚本）

```bash
# 安装 Python 3.8+
sudo yum install -y python3 python3-pip

# 安装 Benchmark 脚本依赖
cd benchmark/
pip3 install -r requirements.txt

# 如果鲲鹏服务器无外网，使用离线包：
pip3 install --no-index --find-links=offline-packages/ -r requirements.txt
```

### 2.7 L0 Cache 环境检测

ForL0StateBackend 的 L0 加速依赖鲲鹏 CPU 的 L0 Cache 硬件和 `libl0mempool.so` 库。在编译前确认 L0 环境状态：

```bash
# 1. 检查 L0 设备节点
ls -la /dev/hisi_l0 2>/dev/null && echo "✓ L0 设备存在" || echo "× L0 设备不存在"

# 2. 检查 L0 内存池库
ls -la /usr/lib64/libl0mempool.so 2>/dev/null && echo "✓ L0 库可用" || echo "× L0 库不可用"

# 3. 检查 LD_LIBRARY_PATH 是否包含 L0 库路径
echo $LD_LIBRARY_PATH | grep -q "/usr/lib64" && echo "✓ LD_LIBRARY_PATH 正确" || echo "⚠ 建议将 /usr/lib64 加入 LD_LIBRARY_PATH"
```

**说明**：
- 如果 L0 设备和库均存在，编译后的原生引擎将在运行时自动启用 L0 模式
- 如果 L0 环境不可用，原生引擎仍可正常编译和运行，自动回退到模拟模式（使用堆内存）
- L0 相关库通过 `dlopen` 动态加载，编译时**不需要** L0 库参与链接

### 2.8 环境变量汇总

建议将以下环境变量加入 `~/.bashrc`：

```bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export MAVEN_HOME=/opt/apache-maven-3.8.8
export FLINK_HOME=/opt/flink-1.20.3
export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$FLINK_HOME/bin:$PATH
```

```bash
source ~/.bashrc
```

---

## 3. 编译构建

### 3.1 构建 C++ 原生引擎

C++ 原生引擎是 ForL0StateBackend 的核心组件，包含 Swiss Tables 哈希表、L0 Allocator 和 JNI 桥接层，必须先于 Java 工程构建。

#### 3.1.1 构建步骤

```bash
cd src/main/native

# 创建构建目录
mkdir -p build && cd build

# 配置 CMake（Release 模式，鲲鹏 NEON 自动检测）
cmake .. -DCMAKE_BUILD_TYPE=Release

# 编译
make -j$(nproc)

# 安装到资源目录
make install
```

#### 3.1.2 构建产物验证

```bash
# 确认动态库已生成
ls -la src/main/resources/native/libforl0_engine.so

# 验证架构
file src/main/resources/native/libforl0_engine.so
# 预期输出: ELF 64-bit LSB shared object, ARM aarch64, ...

# 检查符号（可选）
nm -D src/main/resources/native/libforl0_engine.so | grep Java_org_apache
```

#### 3.1.3 编译选项说明

| CMake 选项 | 默认值 | 说明 |
|------------|--------|------|
| `CMAKE_BUILD_TYPE` | Release | `Release` (优化) / `Debug` (调试) |
| `FORL0_BUILD_TESTS` | OFF | 设为 `ON` 编译 C++ 单元测试 |

鲲鹏平台（aarch64）编译时自动启用：
- `-DFORL0_NEON=1`: NEON SIMD 指令集支持（SwissTable 并行匹配）
- `-O3 -DNDEBUG -march=native`: 鲲鹏 CPU 最高级别优化
- `${CMAKE_DL_LIBS}` 链接: 提供 `dlopen`/`dlsym` 支持（运行时动态加载 `libl0mempool.so`）

**关于 L0 库链接**：原生引擎通过 `dlopen("libl0mempool.so")` 在运行时动态加载 L0 库，编译期不需要 L0 库存在。CMake 仅链接 `libdl`（`${CMAKE_DL_LIBS}`）以支持动态加载。

#### 3.1.4 编译 C++ 单元测试（可选）

```bash
cd src/main/native/build

cmake .. -DCMAKE_BUILD_TYPE=Release -DFORL0_BUILD_TESTS=ON
make -j$(nproc)

# 运行 C++ 测试
./forl0_tests
```

### 3.2 构建 Java 主体工程

#### 3.2.1 编译

```bash
# 编译 Java 代码（在项目根目录下执行）
mvn clean compile
```

#### 3.2.2 运行测试

```bash
# 运行全部测试（需要 native 库已编译安装）
mvn test
```

Maven Surefire 插件已配置 `-Djava.library.path=src/main/resources/native`，测试时会自动加载 native 库。

#### 3.2.3 打包 JAR

```bash
# 打包（跳过测试）
mvn package -DskipTests

# 产物路径
ls target/flink-statebackend-forL0-1.0-SNAPSHOT.jar
```

#### 3.2.4 一键构建（编译 + 测试 + 打包）

```bash
mvn clean package
```

### 3.3 构建 WordCount Benchmark

```bash
cd benchmark/wordcount

# 编译打包
mvn clean package -DskipTests

# 产物
ls target/wordcount-benchmark-*.jar
```

### 3.4 构建 NexMark Benchmark

```bash
cd benchmark/nexmark-src

# 编译整个 NexMark 项目
mvn clean package -DskipTests

# NexMark DataStream JAR 产物
ls nexmark-flink/nexmark-datastream/target/nexmark-datastream-*.jar
```

### 3.5 构建 JMH Benchmark（可选）

```bash
cd benchmark/flink-benchmarks

# 使用 Maven Wrapper 编译
./mvnw clean package -DskipTests

# 或使用系统 Maven
mvn clean package -DskipTests
```

---

## 4. 全量构建脚本

以下为一键完成所有组件构建的脚本：

```bash
#!/bin/bash
set -e

PROJECT_ROOT=$(pwd)

echo "========== 1. 构建 C++ 原生引擎 =========="
cd $PROJECT_ROOT/src/main/native
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
make install
echo ">>> Native 库构建完成: $(ls $PROJECT_ROOT/src/main/resources/native/libforl0_engine.so)"

echo "========== 2. 构建 ForL0StateBackend JAR =========="
cd $PROJECT_ROOT
mvn clean package -DskipTests
echo ">>> JAR 构建完成: $(ls target/flink-statebackend-forL0-*.jar)"

echo "========== 3. 构建 WordCount Benchmark =========="
cd $PROJECT_ROOT/benchmark/wordcount
mvn clean package -DskipTests
echo ">>> WordCount 构建完成: $(ls target/wordcount-benchmark-*.jar)"

echo "========== 4. 构建 NexMark Benchmark =========="
cd $PROJECT_ROOT/benchmark/nexmark-src
mvn clean package -DskipTests
echo ">>> NexMark 构建完成"

echo "========== 构建完成 =========="
echo "所有产物:"
echo "  - ForL0 JAR:    $PROJECT_ROOT/target/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
echo "  - Native 库:    $PROJECT_ROOT/src/main/resources/native/libforl0_engine.so"
echo "  - WordCount:    $PROJECT_ROOT/benchmark/wordcount/target/wordcount-benchmark-*.jar"
echo "  - NexMark:      $PROJECT_ROOT/benchmark/nexmark-src/nexmark-flink/nexmark-datastream/target/nexmark-datastream-*.jar"
```

---

## 5. 部署到 Flink 集群

### 5.1 部署 ForL0StateBackend

```bash
# 1. 复制 JAR 到 Flink lib 目录
cp target/flink-statebackend-forL0-1.0-SNAPSHOT.jar $FLINK_HOME/lib/

# 2. 复制 Native 库
mkdir -p $FLINK_HOME/native
cp src/main/resources/native/libforl0_engine.so $FLINK_HOME/native/

# 3. 配置 Flink（编辑 $FLINK_HOME/conf/config.yaml）
cat >> $FLINK_HOME/conf/config.yaml << 'EOF'

# ForL0 StateBackend 配置
state:
  backend:
    type: forl0

env:
  java:
    opts:
      all: -Djava.library.path=/opt/flink/native
EOF

# 4. 多节点集群：将 JAR 和 native 库同步到所有 TaskManager 节点
# scp $FLINK_HOME/lib/flink-statebackend-forL0-*.jar user@tm-node:$FLINK_HOME/lib/
# scp $FLINK_HOME/native/libforl0_engine.so user@tm-node:$FLINK_HOME/native/
```

### 5.2 部署 Benchmark JAR

```bash
# WordCount Benchmark
cp benchmark/wordcount/target/wordcount-benchmark-*.jar $FLINK_HOME/lib/

# NexMark Benchmark
cp benchmark/nexmark-src/nexmark-flink/nexmark-datastream/target/nexmark-datastream-*.jar $FLINK_HOME/lib/
```

### 5.3 启动集群并验证

```bash
# 启动 Flink 集群
$FLINK_HOME/bin/start-cluster.sh

# 验证 ForL0 加载
grep "\[ForL0\]" $FLINK_HOME/log/flink-*-taskmanager-*.log

# 验证 Web UI
curl -s http://localhost:8081/overview | python3 -m json.tool
```

---

## 6. L0 模式验证

ForL0StateBackend 的 L0 加速通过运行时自动检测实现。部署后需要验证 L0 模式是否正确启用。

### 6.1 L0 运行时检测机制

原生引擎在初始化时执行以下检测流程：

```
1. dlopen("libl0mempool.so")               → 加载 L0 内存池库
2. dlsym() 解析 4 个函数:
   - cache_tuner_init()
   - cache_tuner_destroy()
   - l0_mem_alloc()
   - l0_mem_free()
3. cache_tuner_init(&tuner, capacity)       → 初始化 L0 Cache Tuner
   - 内部检查 /dev/hisi_l0 设备节点
   - 成功 → L0 模式激活
   - 失败 → 回退到模拟模式（堆内存）
```

任何一步失败均自动回退到模拟模式，不影响功能正确性。

### 6.2 验证 L0 模式启用

启动 Flink 集群后，提交任意一个作业（例如 WordCount），然后检查 TaskManager 日志：

```bash
grep -i "ForL0" $FLINK_HOME/log/flink-*-taskexecutor-*.log
```

**L0 模式成功启用**（预期输出）：
```
[ForL0] Native engine loaded via java.library.path
[ForL0] Running in L0 mode (Kunpeng L0 Cache enabled)
[ForL0-L0Allocator] L0 memory initialized: capacity=256MB, max_per_alloc=64KB
```

**模拟模式**（L0 硬件/库不可用时的输出）：
```
[ForL0] Native engine loaded via java.library.path
[ForL0-L0Allocator] L0 library not available, all allocations will use heap memory
```

### 6.3 L0 配置与调优

确认 L0 模式成功启用后，可通过 Flink 配置调整 L0 内存参数：

```yaml
# $FLINK_HOME/conf/config.yaml
state:
  backend:
    type: forl0
    forl0:
      l0-cache:
        enabled: true            # 启用 L0 Cache（默认 false）
        size: 268435456          # L0 内存池大小，256MB（默认）
        max-per-alloc: 65536     # 单次最大 L0 分配，64KB（默认）
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `l0-cache.enabled` | `false` | 是否启用 L0 Cache 加速 |
| `l0-cache.size` | 256MB | L0 内存池总大小 |
| `l0-cache.max-per-alloc` | 64KB | 单次分配上限，超出则回退堆内存 |

**L0 分配策略（Small-Table-First）**：
- 当 SwissTable 大小 ≤ `max-per-alloc` 且 L0 池有余量 → 分配到 L0 Cache
- 当 SwissTable 大小 > `max-per-alloc` 或 L0 池已满 → 回退到堆内存
- 典型场景：128 KeyGroups × 3 states = 384 个 SwissTable，初始每表约 3KB，总计约 1.2MB，完全可由 L0 容纳

### 6.4 L0 运行统计

作业结束时，原生引擎会在 stderr 输出 L0 Allocator 统计信息：

```
[ForL0-L0Allocator] Shutdown: l0_active=1, l0_total_allocs=384, heap_allocs=0, l0_current=10, l0_used=4800KB/256MB
```

| 字段 | 说明 |
|------|------|
| `l0_active` | 1 = L0 模式激活，0 = 模拟模式 |
| `l0_total_allocs` | L0 累计分配次数 |
| `heap_allocs` | 回退到堆内存的分配次数 |
| `l0_current` | 当前存活的 L0 分配块数 |
| `l0_used` | 当前 L0 使用量 / 总容量 |

如果 `heap_allocs` 数量较大，可考虑增大 `l0-cache.size` 或 `l0-cache.max-per-alloc`。

### 6.5 L0 环境故障排查

| 现象 | 可能原因 | 解决方案 |
|------|----------|----------|
| 日志显示 "L0 library not available" | `libl0mempool.so` 不在系统库路径 | 确认 `/usr/lib64/libl0mempool.so` 存在，或将其路径加入 `LD_LIBRARY_PATH` |
| 日志显示 "dlopen failed" | 缺少 L0 库的依赖 | 执行 `ldd /usr/lib64/libl0mempool.so` 检查依赖是否完整 |
| 日志显示 "cache_tuner_init failed" | `/dev/hisi_l0` 设备不存在 | 确认服务器为鲲鹏 CPU 且内核驱动已加载 |
| `l0-cache.enabled: true` 但仍为模拟模式 | 硬件条件不满足 | L0 需同时满足：设备存在 + 库可加载 + 配置启用 |
| `l0_used` 接近 `l0_capacity` | L0 池容量不足 | 增大 `l0-cache.size` |

---

## 7. 运行 Benchmark 测试

### 7.1 WordCount Benchmark

```bash
cd benchmark/scripts

# 运行所有后端对比测试
python3 run_benchmark.py --test wordcount --backend all

# 仅运行 ForL0 后端
python3 run_benchmark.py --test wordcount --backend forl0

# 附带 CPU 火焰图
python3 run_benchmark.py --test wordcount --backend all --profile cpu
```

### 7.2 NexMark Benchmark

```bash
cd benchmark/scripts

# 运行所有有状态查询（q4,q5,q7,q8,q9,q11,q12,q18,q19,q20）
python3 run_benchmark.py --test nexmark --backend all

# 运行指定查询
python3 run_benchmark.py --test nexmark --backend all --query q4,q5,q8

# 运行全部测试（WordCount + NexMark）
python3 run_benchmark.py --test all --backend all
```

### 7.3 生成测试报告

```bash
cd benchmark/scripts

# 生成对比报告和图表
python3 generate_report.py
```

报告输出目录：
- 原始数据: `benchmark/results/raw/`
- 图表: `benchmark/results/figures/`
- 报告: `benchmark/results/reports/`

---

## 8. 可选工具安装

### 8.1 Async Profiler（火焰图）

鲲鹏 ARM64 平台需使用 v3.0+ 版本：

```bash
# 下载 ARM64 版本
wget https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-arm64.tar.gz
tar xzf async-profiler-3.0-linux-arm64.tar.gz -C /opt/

# 配置环境变量
export ASYNC_PROFILER_HOME=/opt/async-profiler-3.0-linux-arm64

# 配置 perf_events 权限
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
echo 0 | sudo tee /proc/sys/kernel/kptr_restrict
```

### 8.2 perf 工具（CPU Cache 统计）

```bash
# 安装 perf
sudo yum install -y perf

# 验证 perf_events 支持
perf stat -e cache-misses,L1-dcache-load-misses ls
```

---

## 9. 常见构建问题

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| `CMake Error: Could not find JNI` | JAVA_HOME 未设置 | 执行 `export JAVA_HOME=/path/to/jdk` |
| `error: unrecognized command line option '-march=native'` | GCC 版本过低 | 升级 GCC >= 7.0 |
| `mvn: command not found` | Maven 未安装 | 参见 2.3 节安装 Maven |
| `fatal error: jni.h: No such file or directory` | 缺少 JDK 开发包 | 安装 `java-1.8.0-openjdk-devel` |
| `cannot find -lstdc++fs` | GCC/libstdc++ 版本问题 | 升级 GCC 或安装 `libstdc++-devel` |
| `mvn test` 中 `UnsatisfiedLinkError` | Native 库未编译 | 先执行 4.1 节编译 native 库 |
| 测试中 `L0MemoryAllocationException` | L0 设备不可用 | 正常现象，模拟模式下部分测试跳过 |
| NexMark 编译失败缺少依赖 | Maven 仓库不可达 | 配置 Maven 镜像源或使用离线仓库 |

---

## 10. 附录

### 10.1 项目目录结构

```
forL0-state-backend/
├── pom.xml                          # Java 主工程 POM
├── src/
│   ├── main/
│   │   ├── java/                    # Java 源码
│   │   │   └── org/apache/flink/state/forl0/
│   │   ├── native/                  # C++ 原生引擎源码
│   │   │   ├── CMakeLists.txt       # CMake 构建配置
│   │   │   ├── cpp/                 # C++ 核心实现
│   │   │   ├── engine/              # 引擎头文件
│   │   │   │   ├── swiss_table.h    # Swiss Tables 哈希表
│   │   │   │   ├── allocator.h      # 内存分配器抽象接口
│   │   │   │   ├── l0_allocator.h   # L0 Cache 分配器（dlopen libl0mempool.so）
│   │   │   │   └── state_engine.h   # 状态引擎
│   │   │   ├── jni/                 # JNI 桥接代码
│   │   │   ├── checkpoint/          # Checkpoint 序列化
│   │   │   └── test/                # C++ 单元测试
│   │   └── resources/native/        # 编译后的 .so 文件
│   └── test/java/                   # Java 测试代码
├── benchmark/
│   ├── config/benchmark.yaml        # Benchmark 配置
│   ├── scripts/                     # Python 运行脚本
│   ├── wordcount/                   # WordCount Benchmark
│   ├── nexmark-src/                 # NexMark Benchmark
│   ├── flink-benchmarks/            # JMH Benchmark
│   └── results/                     # 测试结果输出
├── docker/                          # Docker 部署（开发用）
└── docs/                            # 文档
```

### 10.2 构建命令速查表

| 操作 | 命令 |
|------|------|
| 编译 Native 库 | `cd src/main/native && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$(nproc) && make install` |
| 编译 Java | `mvn clean compile` |
| 运行 Java 测试 | `mvn test` |
| 打包 JAR | `mvn package -DskipTests` |
| 编译 WordCount | `cd benchmark/wordcount && mvn package -DskipTests` |
| 编译 NexMark | `cd benchmark/nexmark-src && mvn package -DskipTests` |
| 编译 C++ 测试 | `cd src/main/native/build && cmake .. -DFORL0_BUILD_TESTS=ON && make -j$(nproc)` |
| 运行 C++ 测试 | `cd src/main/native/build && ./forl0_tests` |
