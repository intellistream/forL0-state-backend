# L0 内存分配设计说明

## 概述

L0Table 是 ForL0StateBackend 中用于热键缓存的组件，其内存分配独立于 Flink MemoryManager 管理的内存。L0 内存通过 JNI 调用 C 语言的 `malloc/free` 进行分配和释放，为将来支持 CXL 内存、PMEM 等特殊内存类型预留了扩展接口。

## 架构设计

### 内存分配器层次

```
┌─────────────────────────────────────────────────────────────────┐
│                        ForL0StateMap                            │
├────────────────────────────────┬────────────────────────────────┤
│         MainTable              │           L0Table              │
│    (MemoryManagerAllocator)    │    (NativeL0MemoryAllocator)   │
├────────────────────────────────┼────────────────────────────────┤
│    Flink MemoryManager         │        JNI Native Memory       │
│    (Off-heap managed memory)   │      (C malloc/free)           │
└────────────────────────────────┴────────────────────────────────┘
```

### 核心类

| 类名 | 职责 |
|------|------|
| `L0MemoryAllocator` | L0 内存分配器接口 |
| `NativeL0MemoryAllocator` | JNI 实现，使用 native 内存 |
| `NativeL0Memory` | JNI 桥接类，声明 native 方法 |
| `MemoryManagerAllocator` | MainTable 使用，由 Flink MemoryManager 管理 |

### 设计要点

1. **单线程设计**：Flink 状态访问是单线程的，allocator 不需要并发支持
2. **无降级选项**：L0Allocator 必须使用 native 内存，如果 native 库不可用会抛出异常
3. **自动提取加载**：native 库可以从 JAR 中自动提取到临时目录加载

## 文件结构

```
src/main/
├── java/org/apache/flink/runtime/state/heap/space/
│   ├── L0MemoryAllocator.java          # 接口定义
│   ├── NativeL0MemoryAllocator.java    # JNI 实现
│   ├── NativeL0Memory.java             # JNI 桥接类
│   └── MemoryManagerAllocator.java     # MainTable 用
├── native/
│   ├── forl0_native.c                  # C 实现
│   └── Makefile                        # 构建脚本
└── resources/native/
    ├── libforl0_native.dylib           # macOS 动态库
    └── libforl0_native.so              # Linux 动态库（需在 Linux 上编译）
```

## Native 库加载流程

```
NativeL0Memory 类加载
        │
        ▼
尝试 System.loadLibrary("forl0_native")
从 java.library.path 加载
        │
        ├─ 成功 → 使用系统路径的库
        │
        └─ 失败 ─┐
                 ▼
        尝试从 JAR 中提取
        /native/libforl0_native.{dylib|so}
                 │
                 ├─ 成功 → 提取到临时目录，System.load() 加载
                 │
                 └─ 失败 → 抛出异常，L0Table 不可用
```

---

# 跨平台开发与部署教程

## 环境要求

### Mac 开发环境
- JDK 8+
- Maven 3.6+
- Xcode Command Line Tools（包含 gcc/clang）

### Linux 服务器环境
- JDK 8+（需要 JDK，不是 JRE，因为需要 JNI 头文件）
- gcc
- make

## 部署方式

### 方式一：打包多平台库到 JAR（推荐，用户零配置）

这种方式用户只需导入 JAR 包即可直接使用，**无需任何额外配置**。

#### 一次性准备（开发者操作）

**步骤 1：在 Mac 上编译 .dylib**
```bash
cd src/main/native
make clean && make && make install
# 生成 src/main/resources/native/libforl0_native.dylib
```

**步骤 2：在 Linux 服务器上编译 .so**
```bash
# 传输源码到服务器
scp -r src/main/native user@server:/tmp/forl0_native/

# SSH 到服务器编译
ssh user@server
cd /tmp/forl0_native
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64  # 根据实际情况设置
make clean && make

# 拷贝回 Mac
exit
scp user@server:/tmp/forl0_native/libforl0_native.so src/main/resources/native/
```

**步骤 3：打包包含双平台库的 JAR**
```bash
# 确认两个库文件都存在
ls src/main/resources/native/
# 应该看到: libforl0_native.dylib  libforl0_native.so

# 打包
mvn clean package -DskipTests
```

**步骤 4：分发 JAR**
```bash
scp target/flink-statebackend-forL0-1.0-SNAPSHOT.jar user@server:/path/to/flink/lib/
```

#### 用户使用

用户只需：
1. 将 JAR 包放入 Flink 的 `lib/` 目录
2. 配置状态后端为 ForL0StateBackend
3. 直接运行作业

**无需设置 java.library.path 或其他任何配置！**

Native 库会自动从 JAR 中提取到临时目录并加载。

---

### 方式二：手动配置 native 库路径

如果无法获取预编译的 `.so` 文件，或者需要使用自定义编译的库：

#### 步骤 A：在服务器上编译 .so

```bash
# 传输源码
scp -r src/main/native user@server:/tmp/forl0_native/

# SSH 编译
ssh user@server
cd /tmp/forl0_native
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
make clean && make
```

#### 步骤 B：配置 Flink

编辑 `flink-conf.yaml`：
```yaml
env.java.opts.taskmanager: "-Djava.library.path=/tmp/forl0_native"
```

或放到系统库目录：
```bash
sudo cp /tmp/forl0_native/libforl0_native.so /usr/lib/
sudo ldconfig
```

#### 步骤 C：重启 Flink

```bash
# 重启 Flink 集群
./bin/stop-cluster.sh
./bin/start-cluster.sh

# 查看日志确认 native 库加载成功
grep "native L0 memory library" log/flink-*-taskexecutor-*.log
# 应该看到: Successfully loaded native L0 memory library from system path: forl0_native
```

## 常见问题

### Q1: 编译时报错 "jni.h: No such file or directory"

**原因**：JAVA_HOME 未设置或指向 JRE 而非 JDK

**解决**：
```bash
# 查找 JDK 安装位置
ls /usr/lib/jvm/

# 设置 JAVA_HOME（确保指向 JDK，不是 JRE）
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# 验证
ls $JAVA_HOME/include/jni.h
```

### Q2: 运行时报错 "no forl0_native in java.library.path"

**原因**：JVM 找不到 native 库

**解决**：
1. 确认 `.so` 文件存在且路径正确
2. 检查 `java.library.path` 设置是否正确
3. 使用 `ldd libforl0_native.so` 检查依赖是否完整

### Q3: IntelliJ IDEA 运行测试报 native 库找不到

**解决**：配置 JUnit 默认 VM options

1. Run → Edit Configurations...
2. Edit configuration templates... → JUnit
3. VM options 添加：`-Djava.library.path=$ProjectFileDir$/src/main/resources/native`
4. Apply → OK

### Q4: 如何同时支持 Mac 和 Linux？

将两个平台的库都放入 `src/main/resources/native/`：
- `libforl0_native.dylib`（Mac）
- `libforl0_native.so`（Linux）

`NativeL0Memory` 会根据操作系统自动选择正确的库从 JAR 中提取加载。

## 未来扩展

当前实现使用标准 C `malloc/free`，未来可替换为：

1. **CXL 内存**：使用 libcxl 分配 CXL 设备内存
2. **PMEM**：使用 libpmem 分配持久内存
3. **自定义内存池**：实现更高效的内存分配策略

只需修改 `forl0_native.c` 中的 `Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_malloc` 等函数实现即可。
