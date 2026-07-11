# NexMark q18 monitor-window probe (2026-07-11)

Goal: explain why the q18 ForL0 uplift looked smaller than earlier runs, without changing
operator parallelism, input mix, or ForL0 implementation.

Common setup:
- Workload IDs: N03/N04 via `forl0-offline-app.sh --reproduce-ascend`
- Scenario: `forl0_no_full_gc_lateq_deep`
- Query: q18
- Parallelism: 8
- Checkpointing: disabled
- Input: `person:auction:bid = 1:9:90`
- Hot ratios: `bid.auctions=24`, `bid.bidders=24`, `auction.sellers=8`
- Configured TPS: 12,000,000
- Cluster: 2 TaskManagers, 8 slots

Findings:
- The original q18 `15s/60s` monitor window includes a visible ramp-up phase. A profiled
  baseline run reported 839,760 events/s because samples climbed from about 300K/s to
  1.57M/s during the same averaged window.
- A `45s/90s` probe showed the steady state reaches about 1.7M/s, but it is not safe for
  the offline one-shot script: the job failed after TM2 was OOM-killed (`exit=137`).
- The adopted `30s/60s` window avoids the OOM risk point while reducing ramp-up bias.

Stable fair comparison with the adopted `30s/60s` q18 window:

| Backend | Result file | Throughput | CPU cores | Throughput/core |
| --- | --- | ---: | ---: | ---: |
| ForL0 | `benchmark/results/nexmark_20260711_202603/nexmark_results.json` | 1,170,000 events/s | 13.94 | 83,931/core |
| HashMap | `benchmark/results/nexmark_20260711_202827/nexmark_results.json` | 814,290 events/s | 54.94 | 14,821/core |

Delta:
- Throughput: ForL0 is +43.7% over HashMap.
- Throughput/core: ForL0 is 5.66x HashMap.

Rejected implementation attempt:
- A fixed-row2 generic-value JNI slice put path was tested and reverted. It reported
  858,700 events/s, below the fixed-row2 baseline, because q18's BinaryRowData value path
  is already mostly exact on-heap backing and the extra slice branch did not remove a copy.

Health after final N03/N04 runs:
- `flink-taskmanager-1`: running, `oom=false`
- `flink-taskmanager-2`: running, `oom=false`
- Flink overview: 2 TaskManagers, 8 total slots, 8 available slots, 0 running jobs.
