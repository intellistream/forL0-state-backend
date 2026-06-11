# 一键离线部署与运行

本指南适用于：将仓库原封不动复制到目标机器 `~/forL0-state-backend`，并在离线环境下完成部署、对比测试和火焰图采集。

## 1. 一键打包（在有构建环境的机器执行）

```bash
cd ~/forL0-state-backend
chmod +x docker/package_offline_bundle.sh
./docker/package_offline_bundle.sh
```

按目标架构显式打包（推荐）：

```bash
cd ~/forL0-state-backend
./docker/package_offline_bundle.sh --arch arm64
```

可选参数：

- `--with-tests`：打包时执行测试
- `--skip-docker-save`：跳过导出 Docker 镜像
- `--output-dir /path/to/dir`：额外输出目录
- `--arch arm64|x64`：指定目标离线机器架构（默认按本机自动判断）

产物位置：

- `docker/deploy/`：JAR 等部署产物
- `docker/images/`：离线 Docker 镜像
- `offline-packages/`：可选 async-profiler 离线包

## 2. 一键部署 + 运行（在离线目标机器执行）

首次运行：

```bash
cd ~/forL0-state-backend/docker
./run_all_apps.sh --flink-home ~/flink-1.20.3
```

后续重复运行：

```bash
cd ~/forL0-state-backend/docker
./run_all_apps.sh
```

不采集火焰图：

```bash
cd ~/forL0-state-backend/docker
./run_all_apps.sh --no-profile
```

## 3. 结果查看

- 性能对比：`benchmark/results/raw/`
- NexMark 汇总：`benchmark/results/nexmark_*/nexmark_results.json`
- 火焰图：`benchmark/results/**/profiles/`
