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

推荐从仓库根目录运行 `./reproduce-all`。运行中的结果和完整日志分别位于
`benchmark/results/runs/<run_id>/` 与其 `.logs`；成功后发布为
`benchmark/results/latest/`。`latest/` 中只有文件、没有文件夹，文件名中的 `__`
代表原目录分隔；`UPLOAD_MANIFEST.tsv` 可用于还原来源路径。需要从网页上传时，
直接选择 `latest/` 中的全部文件。

专门标定与完整调参使用同一个一键入口：

```bash
./reproduce-all --profile
./reproduce-all --full
```

- `--profile`：仅采集硬件、NUMA、软件版本、DRAM 和分级 L0 标定，输出到
  `benchmark/results/profiles/<run_id>/`。开始前会自动停止遗留 Flink 容器，避免
  其他进程占用全局 L0 池；L0 或并行实例标定不完整时命令返回失败，不产生虚假的
  “完成”状态。L0 按每个 TaskManager 进程 64/128/192 MiB 的生产形态容量探测，
  不再使用 vendor runtime 会拒绝的 1 MiB tuner 池。
- `--full`：穷举 `benchmark/config/tuning_space.yaml` 中的 162 组配置；每组运行
  W01-W02、N01-N14、C01-C08 全部 24 个正式 workload，输出到
  `benchmark/results/tuning/<run_id>/`。开始搜索前必须先通过 smoke 正确性门禁。
- 每个 trial 保存 `parameters.json`、实际生效的 `benchmark_config.yaml`、运行日志、
  原始结果和 `trial_manifest.json`；每个 workload 也有独立 manifest，可在 trial
  中间断点续跑。只有全部 workload 成功的 trial 才进入排名，目标分数为 12 对
  ForL0/HashMap workload 比值的几何平均。
- 中断后再次执行 `./reproduce-all --full` 会继续未完成的 campaign；停止使用
  `./reproduce-all --stop`。已有 profile 只有 `status=complete` 才会复用，失败或
  旧的 partial profile 会自动重新采集。
- 临时缩短验证可设置 `FORL0_TUNING_MAX_TRIALS=N`。这只验证前 N 组，不代表完成
  全参数搜索。

没有真实 L0 的开发机可运行：

```bash
./reproduce-all --full --simulate
```

该模式仍生成完整 162×24 搜索产物，但明确标记为 `simulation/model`，只用于验证
搜索、恢复和排名逻辑，不得作为真实性能或论文 speedup 证据。

`reproduce-all` 在离线服务器上只产出 raw、NexMark JSON、失败证据和日志，不生成
figure/PDF/HTML。复制 campaign 目录到分析工作站后运行：

```bash
python benchmark/scripts/generate_campaign_analysis.py \
  --campaign benchmark/results/runs/<run_id> --output output
```

`output/` 是本地派生目录，已加入 `.gitignore`，不会推送远端。
