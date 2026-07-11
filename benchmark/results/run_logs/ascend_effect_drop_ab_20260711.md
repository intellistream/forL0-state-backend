# Ascend WordCount W02 effect-drop A/B note (2026-07-11)

Evidence label: real-online

The runs below used the repository one-click Ascend reproduction path against the
local Docker/Flink cluster. The local environment reported no real L0 device or
`libl0mempool.so`, so these runs validate the ForL0 backend/JNI/native-table path
but not real L0 hardware acceleration.

## Question

The earlier WordCount W02 result showed a larger ForL0 uplift:

- HashMap p8: `519,607 records/s/core`
- ForL0 p8: `623,592 records/s/core`
- Raw files:
  - `benchmark/results/raw/wordcount_hashmap_20260711_054351.json`
  - `benchmark/results/raw/wordcount_forl0_20260711_054640.json`

After restoring the Ascend p8 configuration, later ForL0 runs appeared lower.
We checked whether this was caused by the experimental JNI/native changes.

## Commands

Current experimental native build:

```bash
make -C src/main/native clean all install
cp -f src/main/resources/native/libforl0_engine.so docker/deploy/libforl0_engine.so
./docker/server_setup.sh --flink-home /home/shuhao/flink-1.20.3 --skip-docker-load --no-start
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 --no-report --restart-cluster
```

Clean baseline native build from commit `25526a0144bb4c3242cde355ffcca38afb905d4c`:

```bash
git worktree add --detach /tmp/forl0-ab-25526 25526a0144bb4c3242cde355ffcca38afb905d4c
cd /tmp/forl0-ab-25526
make -C src/main/native clean all install
cp -f src/main/resources/native/libforl0_engine.so /home/shuhao/forL0-state-backend/docker/deploy/libforl0_engine.so
/home/shuhao/forL0-state-backend/docker/server_setup.sh \
  --flink-home /home/shuhao/flink-1.20.3 --skip-docker-load --no-start
cd /home/shuhao/forL0-state-backend
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 --no-report --restart-cluster
```

## Results

| Build | Raw file | Best throughput/core | Repeat samples | Final state |
| --- | --- | ---: | --- | --- |
| HashMap restored p8 | `benchmark/results/raw/wordcount_hashmap_20260711_143147.json` | 519,573 | 519,125 / 519,201 / 519,573 | FINISHED |
| Experimental native before SwissTable miss fix | `benchmark/results/raw/wordcount_forl0_20260711_143444.json` | 519,651 | 479,136 / 479,616 / 519,651 | FINISHED |
| Experimental native after SwissTable miss fix | `benchmark/results/raw/wordcount_forl0_20260711_144125.json` | 566,865 | 479,370 / 566,547 / 566,865 | FINISHED |
| Clean native at `25526a0` | `benchmark/results/raw/wordcount_forl0_20260711_144733.json` | 566,784 | 387,942 / 519,427 / 566,784 | FINISHED |
| Earlier high ForL0 result | `benchmark/results/raw/wordcount_forl0_20260711_054640.json` | 623,592 | 445,078 / 566,775 / 623,592 | AUTO_STOPPED |

## Interpretation

The restored p8 configuration is correct and remains the default. The later
`566k records/s/core` ForL0 result is reproduced by both the experimental native
build and the clean `25526a0` native build, so the current experimental JNI/native
changes were not the cause of the apparent drop from `623k`.

The earlier `623k` best sample differs in job termination mode: it was recorded
as `AUTO_STOPPED`, while the later A/B samples were normal `FINISHED` runs. The
stable comparable FINISHED tier is about `566k`, which is still above HashMap
`519k` by about 9.1%.

Because the experimental JNI/native helper did not show a stable additional gain
over the clean baseline on W02, it was removed from the main worktree and should
not be submitted as an optimization without further positive evidence.
