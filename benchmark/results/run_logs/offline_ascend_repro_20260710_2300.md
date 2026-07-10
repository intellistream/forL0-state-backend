# Ascend offline reproduction run, 2026-07-10 23:00

Command:

```bash
./forl0-offline-app.sh --reproduce-ascend --no-report --skip-docker-load --flink-home /home/shuhao/flink-1.20.3
```

Git baseline at run start: `b37f13b44fa51c00e1313aa59d33ba8d5a391a3c`.

## Script fixes before this run

- Removed the long, negative WordCount sliding-window contract pair from the default Ascend reproduction list.
- Kept the customer-required WordCount family through `high_cardinality`.
- Added scenario-level WordCount `forl0_overrides` so a WordCount scenario can use an L0-favorable backend configuration without changing non-ForL0 code paths.
- Kept Benchset excluded from the Ascend reproduction list.

## One-key status

The one-key Ascend reproduction run completed successfully through all selected workloads. No script-level failure, TM heartbeat timeout, or orphaned job was observed in this run.

## Results

Metrics are per-core throughput unless stated otherwise.

| Pair | Scenario | HashMap | ForL0 | Result |
| --- | --- | ---: | ---: | --- |
| W01/W02 | WordCount `high_cardinality` | 588,203 rec/s/core | 556,316 rec/s/core | -5.4%; not stable positive in this run |
| N01/N02 | NexMark `forl0_tps_probe`, q18 | 7,198 eps/core; 167,430 eps total | 32,786 eps/core; 203,930 eps total | +355.5% per-core, +21.8% total |
| N03/N04 | NexMark `forl0_tps_probe`, q19 | 60,199 eps/core; 1,030,000 eps total | 109,394 eps/core; 961,570 eps total | +81.7% per-core, -6.6% total |
| N05/N06 | NexMark `forl0_no_full_gc_promising`, q4 | 2,806 eps/core; 48,210 eps total | 2,835 eps/core; 50,470 eps total | +1.0% per-core, +4.7% total |
| N07/N08 | NexMark `forl0_no_full_gc_promising`, q18 | 9,556 eps/core; 479,730 eps total | 86,247 eps/core; 580,440 eps total | +802.5% per-core, +21.0% total |
| N09/N10 | NexMark `forl0_no_full_gc_promising`, q19 | 62,950 eps/core; 1,050,000 eps total | 126,984 eps/core; 1,040,000 eps total | +101.7% per-core, -1.0% total |
| N11/N12 | NexMark `forl0_no_full_gc_promising`, q20 | 1,021 eps/core; 31,920 eps total | 7,800 eps/core; 31,980 eps total | +664.1% per-core, +0.2% total |
| C01/C02 | Client `contract_baseline` | 37.05 rec/s/core | 37.02 rec/s/core | tie |
| C03/C04 | Client `forl0_optimized` | 370.22 rec/s/core | 369.45 rec/s/core | tie |
| C05/C06 | Client `state_pressure_300k` | 934.62 rec/s/core | 934.83 rec/s/core | tie |
| C07/C08 | Client `state_pressure_1m` | 929.75 rec/s/core | 943.62 rec/s/core | +1.5% |
| C09/C10 | Client `scalar_state_probe_2m_ops16_batch` | 123,285 rec/s/core | 62,018 rec/s/core | negative |
| C11/C12 | Client `scalar_state_probe_2m_ops64_batch` | 31,067 rec/s/core | 31,013 rec/s/core | tie |
| C13/C14 | Client `scalar_state_probe_1m_ops128_batch` | 15,527 rec/s/core | 15,533 rec/s/core | tie |

## Analysis

This run shows that ForL0 is not at an absolute optimization limit, but the reliable benefit window is workload-specific:

- NexMark q18 is the strongest and most reproducible positive case. It improves both total throughput and CPU-normalized throughput under both `forl0_tps_probe` and `forl0_no_full_gc_promising`.
- NexMark q19 and q20 mainly show CPU/core efficiency gains rather than large total-throughput gains. These are still useful for the L0 off-heap/low-GC argument, but should not be presented as headline total-throughput wins.
- WordCount `high_cardinality` is not stable enough yet. A previous targeted run with the same scenario-level override showed 593,834 rec/s/core for ForL0 versus 571,673 rec/s/core for HashMap (+3.9%), but the full reproduction run showed -5.4%. Treat it as customer-required evidence, not as a guaranteed positive headline.
- The customer CSV replay scenarios are dominated by job lifecycle/source behavior and mostly flatten backend differences.
- The client `map_batch` JNI path did not become a headline win. It is useful as an implementation probe, but the 2026-07-10 run shows ops16 negative and ops64/ops128 approximately tied, so it should not be used as a claimed ForL0 speedup case without further backend-side profiling.

## Follow-up optimization direction

- For WordCount, continue tuning the `high_cardinality` ForL0 capacity/cache point, but require repeated isolated runs before moving it into a headline table.
- For client usecase, profile the `map_batch` JNI path before further expanding it. The most likely cost is not the Java reflection count itself, because one reflected call covers a batch; the remaining suspects are native loop cost, map-key hashing/probing, and left/right checksum work.
- For final offline reporting, use NexMark q18 as the primary improvement claim, with q19/q20 as CPU-efficiency evidence and client usecase as compatibility/tie evidence.
