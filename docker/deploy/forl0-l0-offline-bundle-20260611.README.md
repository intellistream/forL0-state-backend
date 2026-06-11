# ForL0 Offline Bundle 2026-06-11

这个离线包用于将本地鲲鹏构建好的运行产物同步到不联网的 L0 服务器。

## 包内内容

- `artifacts/flink-statebackend-forL0-1.0-SNAPSHOT.jar`
- `artifacts/libforl0_engine.so`
- `artifacts/wordcount-benchmark-1.0-SNAPSHOT.jar`
- `benchmark/config/`
- `benchmark/scripts/`
- `docker/conf/`
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

1. 从 GitHub 同步 `docker/deploy/forl0-l0-offline-bundle-20260611.tar.gz`。
2. 在目标机器解压。
3. 将 `artifacts/` 下的 JAR 和 `libforl0_engine.so` 按实际 Flink 安装路径放置。
4. 如需抓 flame graph，使用 `tools/async-profiler-4.4-linux-arm64/`。

## 说明

这个离线包不包含 Docker 镜像本身，也不包含系统级 L0 驱动或 `libl0mempool.so` 等目标机运行时依赖。