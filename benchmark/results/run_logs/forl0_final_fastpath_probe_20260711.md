# ForL0 ValueState final-fastpath probe - 2026-07-11

## Purpose

Test a narrow ForL0-only Java optimization suggested by the WordCount CPU
profile: mark `ForL0ValueState.addAndGetLong(long)` as `final` so the JVM has a
stronger devirtualization signal for the benchmark fast path.

## Build method

The machine does not have Maven installed, so this was a local dirty probe:

```bash
javac -source 8 -target 8 \
  -cp 'target/classes:/home/shuhao/flink-1.20.3/lib/*' \
  -d /tmp/forl0-final-classes \
  src/main/java/org/apache/flink/state/forl0/ForL0ValueState.java
```

The generated `ForL0ValueState*.class` files were inserted into the tracked
backend JARs only for the local experiment.  The source and JAR changes were
then reverted.

## Run

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 \
  --no-report --restart-cluster
```

Result:

- Raw: `benchmark/results/raw/wordcount_forl0_20260711_154325.json`
- Best ForL0 throughput/core: `519,527 records/s/core`

This is a clear regression versus the clean ForL0 reference
(`566,784 records/s/core`) and falls back to the HashMap tier
(`519,573 records/s/core`).

## Decision

Do not keep this optimization.  The local `javac` partial rebuild likely changed
more bytecode shape than the single `final` modifier, and the measured result is
negative.  The repository was restored to the clean tracked source and JAR
state.

Follow-up clean validation after restoring the tracked source/JAR still measured
around the HashMap tier:

- Raw: `benchmark/results/raw/wordcount_forl0_20260711_154751.json`
- Best ForL0 throughput/core: `519,768 records/s/core`

The later positive recovery came from the WordCount scenario-level ForL0
table/cache window, not from this Java modifier experiment.
