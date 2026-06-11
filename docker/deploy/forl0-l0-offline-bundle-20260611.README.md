# ForL0 Offline Bundle 2026-06-11

这个离线包用于将本地鲲鹏构建好的运行产物同步到不联网的 L0 服务器。

如果你会把整个仓库原封不动拷到目标机器，优先直接使用仓库内的 `docker/server_setup.sh`，那会比离线包更傻瓜一些。

## 包内内容

- `artifacts/flink-statebackend-forL0-1.0-SNAPSHOT.jar`
- `artifacts/libforl0_engine.so`
- `artifacts/wordcount-benchmark-1.0-SNAPSHOT.jar`
- `benchmark/config/`
- `benchmark/scripts/`
- `docker/conf/`
- `docker/install_offline_bundle.sh`
- `docker/docker_run.sh`
- `docker/start.sh`
- `docker/stop.sh`
- `docker/restart.sh`
- `tools/async-profiler-4.4-linux-arm64/`
- `docs/FlameGraph_Runbook_20260611.md`

## 适用场景

1. 目标机器不联网。
2. 目标机器是兼容的 Linux ARM64 / 鲲鹏环境。
3. 目标机器已有 Flink、Java、Docker 和 L0 运行时环境。

## 使用方式

1. 通过离线拷贝方式把 `forl0-l0-offline-bundle-20260611.tar.gz` 和对应 `.sha256` 文件带到目标机器。
2. 在目标机器解压。
3. 执行 `./docker/install_offline_bundle.sh --flink-home /path/to/flink` 完成安装。
4. 如需抓 flame graph，可加 `--copy-profiler`，或直接使用 `tools/async-profiler-4.4-linux-arm64/`。

## 更推荐的方式

如果目标机器上会保留完整仓库目录，直接执行：

```bash
cd ~/forL0-state-backend/docker
./server_setup.sh
```

找不到 Flink 时再手工指定：

```bash
cd ~/forL0-state-backend/docker
./server_setup.sh --flink-home /path/to/flink
```

## 说明

这个离线包不包含 Docker 镜像本身，也不包含系统级 L0 驱动或 `libl0mempool.so` 等目标机运行时依赖。