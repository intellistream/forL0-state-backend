# 冻结产物与源码

运行实验不需要构建。`../payload/BUILD.json` 记录依赖包和Job/backend/native的SHA-256；解包后的 `runtime/MANIFEST.json` 记录全部运行时文件哈希。

后端源码为本分支根目录 `src/main/`，其中 `src/main/resources/native/libforl0_engine.so` 与离线包使用的native二进制相同。编译使用C++17、Release、`-O3 -DNDEBUG -march=native`，目标为本实验同型号Kunpeng ARM64。已在构建环境执行：

```bash
JAVA_HOME=/usr/lib/jvm/java-11-openjdk cmake -S src/main/native -B build \
  -DCMAKE_BUILD_TYPE=Release -DFORL0_FORCE_MINI_GTEST=ON
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

Java后端增量类使用 `javac --release 11`，以同一Flink发行版和原R30后端为classpath，更新 `NativeEngine`、`PrimitiveLongAccess`、`PrimitiveRowCacheAccess`、`ForL0MapState`，并将新native嵌入JAR。其他类沿用冻结R30。原生测试及真实状态验证日志在 `../validation/`。

`source/job/src/main/java/` 为主实验的稳态Job源码，`source/job/provenance.json` 为原始字节码追溯记录。Job target为Java8，实际运行于同一Java11；三个BriskState容量组、关闭组及Heap共用同一个Job。`oracle/` 为独立源前缀重放及最终feature投影核验程序。

`../scripts/l0_probe.cpp` 是双进程容量读写探测器源码，构建时直接链接同一 `src/main/native/engine/hot_cache.cpp`，并使用同一头文件及ABI。两进程通过barrier同时持有申请区域，再共同释放，不用顺序申请假装验证共享容量。

运行时来自已用于主实验/裸机实验的BiSheng JDK11.0.32+9、Flink1.20.3及私有glibc/jemalloc；Python3.10及其标准库和动态依赖随包提供。许可文件在 `../licenses/`、JDK `legal/` 和运行时 `LICENSES/` 中。此处是产物追溯说明；合作方只需运行根目录入口，不需要这些编译工具。
