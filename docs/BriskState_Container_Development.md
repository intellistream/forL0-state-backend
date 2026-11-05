> Correction (2026-11-03): cross-backend speedups from earlier fused/batch job
> paths are withdrawn. Current benchmark jobs use standard Flink API calls only.

# BriskState container development

Validated on 2026-11-02 in `main.yjy-flink-32.user.coder`, branch
`briskstate_workspace`, openEuler 24.03 ARM64 / Kunpeng-920, OpenJDK 11.0.32.

## Installed environment

- Repository: `/root/forL0-state-backend`
- Flink: `/root/opt/flink-1.20.3` (official Scala 2.12 distribution, SHA-512 verified)
- Java: `/usr/lib/jvm/java-11-openjdk`; Maven 3.6.3; CMake and GCC 12
- Runtime/config/log/PID directories: `/root/briskstate-runtime/{jm,tm1,tm2}`
- Setup/build logs: `/root/briskstate-setup`
- Shell environment: `/root/briskstate-runtime/env.sh`, loaded by `.bashrc`
- Profiler: `/root/opt/async-profiler-4.4-linux-arm64/bin/asprof`

| Process | CPU affinity | Flink process memory | Slots |
|---|---|---|---|
| JM | 0-3 | 8 GiB | - |
| TM1 | 4-7 | 32 GiB | 4 |
| TM2 | 8-11 | 32 GiB | 4 |

These are separate JVMs in the existing container, with `ActiveProcessorCount=4`.
The parent container has 32 cores of CPU quota and 192 GiB memory. Child cgroups
are read-only: Flink process memory is a JVM/native budget, not a hard OS memory
cap or exclusive CPU reservation. Per TM: 8 GiB task off-heap, 256 MiB managed,
512 MiB network memory. ForL0 tables have a 1 GiB per-StateEngine native limit.

L0 stays disabled in all generated Flink configs and per-job parameters. This
machine does not have an L0 device. ForL0 uses its actual JNI/native StateEngine.
The runtime remains Java 11; source/bytecode compatibility remains Java 8.

## Build and cluster lifecycle

```bash
cd /root/forL0-state-backend
bash docker/briskstate-dev.sh status
bash docker/briskstate-dev.sh stop
bash docker/briskstate-dev.sh build
bash docker/briskstate-dev.sh start
```

Stop before rebuilding. Build copies canonical native sources into the runtime
build directory, compiles ARM64 code, runs native tests and the Java configuration/
state-semantics tests against that library, embeds the fresh native library in the
backend JAR, builds/tests WordCount and the ICDE benchset, and deploys the backend into Flink `lib`.
Checked-in native binaries are preserved. Some generated Maven JARs/POMs are
already tracked and can appear as modified after builds. Native-library lookup
for Java tests can be overridden with `-Dforl0.native.library.path=...`.

The cluster survives SSH disconnects. After a container restart, start it again.
REST/Web UI is loopback `127.0.0.1:8081`. From your laptop:

```bash
ssh -N -L 8081:127.0.0.1:8081 main.yjy-flink-32.user.coder
```

Then open http://localhost:8081.

## Long fast-path pressure tests

The container development launcher accepts Long keys only. Its defaults are:

- 32,000,000 distinct keys; 128,000,000 records; parallelism 8
- 16 separate state increments per input record: 2,048,000,000 total increments
- Unlimited input rate; uniform keys; checkpointing disabled for performance
- Direct Long source and Long output: no String construction or String hash keys
- The first source pass covers the entire requested key space without collisions

Both backends execute the same standard ValueState read/update pair for each
increment. The job does not call backend-specific fused methods. The 16 operations are not collapsed into a
single larger delta. Flink transport serialization and Long/Tuple allocations
still exist; the state backend uses its primitive native path.

```bash
bash docker/briskstate-dev.sh wordcount --backend heap
bash docker/briskstate-dev.sh wordcount --backend forl0

# Three alternating pairs; keep every sample, report medians rather than best runs.
for round in 1 2 3; do
  for backend in heap forl0; do
    bash docker/briskstate-dev.sh wordcount --backend "$backend"
  done
done

# Profile separately from formal timing, on both TMs.
bash docker/briskstate-dev.sh wordcount --backend forl0 --profile
```

Profiling captures CPU then allocation samples with async-profiler, saving HTML
flame graphs and collapsed stacks for each TM. The launcher also saves command,
job/checkpoint/exception details, CPU-time/RSS samples and artifact SHA-256 values.
Raw output is under `benchmark/results/runs/briskstate_*`. Archived profiles and
timing summary are in `benchmark/results/runs/briskstate_long_pressure_20261102`.
For the seven-application ICDE suite, use `benchmark/scripts/run_benchset.py` and
see `benchmark/benchset/README.md`. The Long WordCount launcher remains an
independent single-application regression tool.

## Correctness checks

Use the independent per-key count oracle only for small key spaces. It maintains
its own expected counts and fails on any missing/extra increment. It is disabled
in performance runs.

```bash
for backend in heap forl0; do
  bash docker/briskstate-dev.sh wordcount --backend "$backend" \
    --keys 10000 --records 1000000 --state-ops 16 --verify-counts
 done

# Separate checkpoint creation check, long enough to complete several checkpoints.
bash docker/briskstate-dev.sh wordcount --backend forl0 \
  --keys 1000000 --records 8000000 --state-ops 16 --rate 250000 \
  --checkpoint-interval 3000
```

The first Long WordCount campaign passed 138 native tests, 23 Java configuration/
state-semantics tests and 5 benchmark tests. The later ICDE adaptive-map campaign
passed 141 native tests, 24 Java state/configuration tests and 10 benchset tests. Both backends passed the independent count oracle. Native tests
cover checkpoint round trips; the Flink run verifies checkpoint creation rather
than automatic recovery after a process failure.

The production optimization resolves the native StateTable pointer once at state
registration and reuses it in ValueState JNI operations. Engine ownership keeps
this pointer stable through registry growth, SwissTable rehash and snapshots.
Those conditions are covered by a new native regression test.

The old String-key counter regression fix remains covered by a unit test;
performance measurements here use Long keys exclusively.

## GC and hardware evidence

All future JVM starts enable rotating `gc*,safepoint` unified logs in each role
log directory. The benchset driver records per-TM GC counter snapshots before/
after every job. `collect_jvm_evidence.py` optionally samples GC/heap/CPU metrics
every two seconds. GC times are reported per TM, not summed and interpreted as
job wall time. See `docs/BriskState_Kunpeng_TopDown.md` for the installed official
DevKit and current Kubernetes PMU permission blocker.
