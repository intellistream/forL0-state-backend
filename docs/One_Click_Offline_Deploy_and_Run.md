# 一键离线部署与运行

本指南适用于：在联网/构建机生成离线包，将离线包拷贝到目标机器后，在无外网环境下完成安装、对比测试和报告生成。

## 1. 一键打包（在有构建环境的机器执行）

```bash
cd ~/forL0-state-backend
chmod +x docker/package_offline_bundle.sh
./docker/package_offline_bundle.sh --arch arm64 --output-dir /tmp/forl0-offline
```

按目标架构显式打包（推荐）：

```bash
cd ~/forL0-state-backend
./docker/package_offline_bundle.sh --arch arm64
```

可选参数：

- `--with-tests`：打包时执行测试
- `--skip-docker-save`：跳过导出 Docker 镜像
- `--skip-python-wheels`：跳过下载 benchmark Python 离线依赖
- `--output-dir /path/to/dir`：额外输出目录
- `--arch arm64|x64`：指定目标离线机器架构（默认按本机自动判断）

产物位置：

- `docker/deploy/`：JAR 等部署产物
- `docker/images/`：离线 Docker 镜像
- `offline-packages/`：Python wheels 与可选 async-profiler 离线包
- `offline-artifacts/artifacts/`：结构化 JAR/native 部署产物
- `offline-artifacts/benchmark/`：benchmark 配置、脚本与离线 Python wheels
- `offline-artifacts/docker/`：离线安装、部署、运行脚本与 Docker 镜像
- `offline-artifacts/offline_bundle_manifest.txt`：文件清单
- `offline-artifacts/offline_bundle_sha256.txt`：SHA256 校验清单

离线机校验示例：

```bash
cd ~/forL0-state-backend/offline-artifacts
sha256sum -c offline_bundle_sha256.txt
```

## 2. 一键部署 + 运行（在离线目标机器执行）

首次安装：

```bash
cd /path/to/forl0-offline/docker
./install_offline_bundle.sh --flink-home ~/flink-1.20.3 --install-dir ~/forl0-runtime
```

一键运行全部应用实验并生成报告：

```bash
cd ~/forl0-runtime/docker
./run_all_apps.sh --offline --test apps --backend all --no-profile
```

如需启用 CPU 火焰图，离线包中需要包含 async-profiler，然后去掉 `--no-profile` 或显式传入 `--profile cpu`。

只基于已有结果重新生成报告：

```bash
cd ~/forl0-runtime/docker
./run_all_apps.sh --offline --report-only --no-profile
```

## 3. 结果查看

- 性能对比：`benchmark/results/raw/`
- NexMark 汇总：`benchmark/results/nexmark_*/nexmark_results.json`
- 火焰图：`benchmark/results/**/profiles/`
- HTML 报告：`benchmark/results/reports/benchmark_report.html`
