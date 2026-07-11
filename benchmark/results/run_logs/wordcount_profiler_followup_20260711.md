# WordCount W02 profiler follow-up, 2026-07-11

Evidence label: real-online probe on the Ascend host.

## Goal

Use the W02 profiler evidence to check whether the current ForL0 stable line
can be raised back toward the earlier `623 K records/s/core` peak without
keeping unstable or unfair changes.

## Clean baseline

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 \
  --no-report --restart-cluster
```

Result file:

```text
benchmark/results/raw/wordcount_forl0_20260711_171311.json
```

Repeat samples:

| Repeat | records/s/core |
| ---: | ---: |
| 1 | 415,047 |
| 2 | 566,591 |
| 3 | 566,731 |

Best result: `566,731 records/s/core`.

This reproduces the current stable W02 line rather than the earlier best-of-3
peak.

## CPU profile

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 \
  --profile cpu --no-report --restart-cluster
```

Result file:

```text
benchmark/results/raw/wordcount_forl0_20260711_171643.json
```

Profile files:

```text
benchmark/results/profiles/flamegraph_cpu_forl0_20260711_171357.html
benchmark/results/profiles/flamegraph_cpu_forl0_20260711_171506.html
benchmark/results/profiles/flamegraph_cpu_forl0_20260711_171554.html
```

Repeat samples:

| Repeat | records/s/core |
| ---: | ---: |
| 1 | 389,122 |
| 2 | 566,847 |
| 3 | 566,957 |

The profile run kept the same stable-line shape. The stable repeats show the
business counter path in the profile, but a large share remains in Flink source
and network input/output code. JNI/native itself is not the dominant remaining
cost.

## Rejected probe: direct ForL0 interface call

Temporary change:

- In `WordCountBenchmark.LongKeyStatefulCounter`, replace the reflective
  `MethodHandle` fast-path wrapper with a direct
  `LongValueStateAddAndGet` interface call.
- Rebuild only the WordCount benchmark class files and update the WordCount
  benchmark JAR.

Result file:

```text
benchmark/results/raw/wordcount_forl0_20260711_172207.json
```

Repeat samples:

| Repeat | records/s/core |
| ---: | ---: |
| 1 | 415,146 |
| 2 | 519,468 |
| 3 | 519,650 |

Decision: reverted. The interface-call shape regressed W02 from the stable
`~566 K records/s/core` line to the HashMap-tier `~519 K records/s/core` line.
Do not keep this change in the offline package.

## Rejected probe: repeat count 5

Temporary change:

- Set W02 `repeat_runs` from `3` to `5` to test whether additional JVM warmup
  reliably recovers the earlier `623 K records/s/core` peak.

Result file:

```text
benchmark/results/raw/wordcount_forl0_20260711_172814.json
```

Repeat samples:

| Repeat | records/s/core |
| ---: | ---: |
| 1 | 415,077 |
| 2 | 519,301 |
| 3 | 566,553 |
| 4 | 479,571 |
| 5 | 519,671 |

Decision: reverted. More repeats did not produce a stable higher ForL0 line.
The earlier `623 K records/s/core` sample should be treated as a peak outlier,
not the default reproducible value.

## Current conclusion

Keep the committed W02 configuration at `parallelism: 8`, `repeat_runs: 3`,
and the large ForL0 table/cache window. It is the strongest stable default found
so far. Further gains likely need either a deeper ForL0 primitive-counter path
or workload/source-output overhead reduction; the attempted Java call-shape and
measurement-window changes did not raise the stable line.
