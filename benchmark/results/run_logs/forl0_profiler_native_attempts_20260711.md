# ForL0 profiler native attempts, 2026-07-11

Evidence label: real-online

The experiments below used the repository one-click Ascend reproduction path
against the local Docker/Flink cluster. This host still reports no real L0
device and no `libl0mempool.so`, so the measurements validate the ForL0
backend/JNI/native-table path but not real L0 hardware acceleration.

## Profiler setup fix

`docker/server_setup.sh` did not discover the offline async-profiler archive
bundled under `benchmark/offline-packages`. The script now also searches:

- `benchmark/offline-packages/async-profiler-4.4-linux-arm64.tar.gz`
- `benchmark/offline-packages/async-profiler-4.4-linux-x64.tar.gz`

Validation command:

```bash
./docker/server_setup.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --no-start
```

Validation result:

```text
✓ async-profiler 已就绪: /home/shuhao/forL0-state-backend/tools/async-profiler
```

## CPU profile

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 \
  --profile cpu --no-report --restart-cluster
```

Primary raw result:

- `benchmark/results/raw/wordcount_forl0_20260711_145359.json`
- best repeat: `566,587 records/s/core`, `FINISHED`
- profiler file: `benchmark/results/profiles/flamegraph_cpu_forl0_20260711_145206.html`

Profiler interpretation:

- Most samples are outside ForL0 native code: Flink source, network, task, and
  serialization work dominate the full job profile.
- The visible ForL0 hotspot is the fused primitive path:
  `ForL0ValueState.addAndGetLong` -> `NativeEngine.valueAddAndGetLongLong` ->
  `Java_org_apache_flink_state_forl0_NativeEngine_valueAddAndGetLongLong`.
- Key-context updates and SwissTable rehashing are small in this profile, so
  optimizing them is unlikely to move the end-to-end W02 number.

## Native attempts

Baseline references:

| Build | Raw file | Best throughput/core | State |
| --- | --- | ---: | --- |
| HashMap restored p8 | `benchmark/results/raw/wordcount_hashmap_20260711_143147.json` | 519,573 | FINISHED |
| Clean ForL0 at `25526a0` | `benchmark/results/raw/wordcount_forl0_20260711_144733.json` | 566,784 | FINISHED |

Attempt 1: cache typed `StateTable<K,V>*` in `StateHandle`.

| Raw file | Best throughput/core | State | Decision |
| --- | ---: | --- | --- |
| `benchmark/results/raw/wordcount_forl0_20260711_150751.json` | 566,986 | FINISHED | Not kept; +0.04% over clean baseline is noise-level |

Attempt 2: add a native `StateTable::add_and_get` primitive using SwissTable
`emplace` for one-probe add-or-insert.

| Raw file | Best throughput/core | State | Decision |
| --- | ---: | --- | --- |
| `benchmark/results/raw/wordcount_forl0_20260711_151453.json` | 519,554 | FINISHED | Reverted; clear regression to HashMap tier |

## Conclusion

The profiler-guided native micro-optimizations did not produce a stable
end-to-end improvement for W02. The table-cache variant was noise-level, and
the add-or-insert primitive regressed. Both native experiments were removed from
the worktree. The only retained change from this round is the offline
async-profiler discovery fix, which improves one-click reproducibility without
changing ForL0 runtime behavior.
