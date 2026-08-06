# NexMark heap_squeeze_gc_probe run summary - 2026-07-08

## Environment

- Repository: `/home/shuhao/forL0-state-backend`
- Flink: `/home/shuhao/flink-1.20.3`
- Scenario: `heap_squeeze_gc_probe`
- Flink config: `docker/conf-tight/config.yaml`
- Flink TM process size: `8g`
- Managed memory: `128mb`
- Parallelism: `4`
- Docker mode: `docker/docker_run.sh`

## Commands

```bash
source docker/forl0-local.env
FORL0_FLINK_CONF_DIR=/home/shuhao/forL0-state-backend/docker/conf-tight \
FORL0_TM_DOCKER_MEMORY=10g \
FORL0_JM_DOCKER_MEMORY=6g \
./docker/docker_run.sh start

python3 benchmark/scripts/run_benchmark.py \
  --test nexmark \
  --backend all \
  --scenario heap_squeeze_gc_probe
```

The combined `--backend all` run was terminated after HashMap completed, before
ForL0 started. HashMap results were preserved in the log below.

```bash
FORL0_FLINK_CONF_DIR=/home/shuhao/forL0-state-backend/docker/conf-tight \
FORL0_TM_DOCKER_MEMORY=12g \
FORL0_JM_DOCKER_MEMORY=6g \
./docker/docker_run.sh start

python3 -u benchmark/scripts/run_benchmark.py \
  --test nexmark \
  --backend forl0 \
  --scenario heap_squeeze_gc_probe
```

## Results

| Backend | TM container memory | Query | Status | Throughput | Avg CPU cores | Notes |
|---|---:|---|---|---:|---:|---|
| HashMap | 10g | q18 | OK | 312.49 K/s | 78.96 | 2 TM run |
| HashMap | 10g | q19 | OK | 651.69 K/s | 62.48 | 2 TM run |
| HashMap | 10g | q20 | OK | 22.59 K/s | 99.17 | 2 TM run |
| ForL0 | 10g | q18 | FAILED | n/a | n/a | `flink-taskmanager-2` exited 137; heartbeat timeout |
| ForL0 | 12g | q18 | OK | 669.71 K/s | 6.88 | 2 TM reported during metric collection |
| ForL0 | 12g | q19 | OK | 1.05 M/s | 9.53 | Metric sender reported 1 TM; `flink-taskmanager-1` had exited 137 |
| ForL0 | 12g | q20 | FAILED | n/a | n/a | `flink-taskmanager-2` became unreachable / exited 137 |

## Interpretation Notes

- The pressure probe did expose a clear ForL0 benefit before instability:
  - q18: `669.71 K/s / 312.49 K/s = 2.14x`
  - q19: `1.05 M/s / 651.69 K/s = 1.61x`, but this is not a clean apples-to-apples comparison because one ForL0 TaskManager had already exited.
- ForL0 under this scenario is constrained by native/off-heap/container memory headroom:
  - 10g container memory with 8g Flink process memory is too tight for q18.
  - 12g allows q18 and q19 to produce results, but TaskManagers still exit 137 before the full q18/q19/q20 suite completes.
- HashMap completed the full q18/q19/q20 suite at 10g container memory, but with much lower throughput on q18 and q19.

## Artifacts

- HashMap 10g log: `benchmark/results/run_logs/nexmark_heap_squeeze_gc_probe_20260708_214713.log`
- ForL0 10g log: `benchmark/results/run_logs/nexmark_heap_squeeze_gc_probe_forl0_20260708_215610.log`
- ForL0 10g q18 failure JSON: `benchmark/results/run_logs/nexmark_heap_squeeze_gc_probe_forl0_q18_failure_20260708_215610.json`
- ForL0 12g log: `benchmark/results/run_logs/nexmark_heap_squeeze_gc_probe_forl0_tm12g_20260708_220114.log`
- ForL0 12g q20 failure JSON: `benchmark/results/run_logs/nexmark_heap_squeeze_gc_probe_forl0_q20_failure_tm12g_20260708_220114.json`

## Next Recommended Run

Use the same 8g Flink process size but raise TM container memory to at least
`14g` or reduce `l0_memory_max_size` from `768mb` to `256mb`. The goal is to
separate "ForL0 state-access speedup" from "native/off-heap memory headroom"
so q18/q19/q20 can all complete in one ForL0 run.
