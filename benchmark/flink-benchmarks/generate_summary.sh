#!/bin/bash
# Generate summary from existing CSV files

if [ -z "$1" ]; then
    echo "Usage: $0 <timestamp>"
    echo "Example: $0 20251225_202606"
    exit 1
fi

TIMESTAMP=$1
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_ROOT="${FORL0_RESULTS_DIR:-${SCRIPT_DIR}/../results}"
RESULTS_DIR="${RESULTS_ROOT}/state-benchmark"
SUMMARY_FILE="$RESULTS_DIR/summary_${TIMESTAMP}.txt"
mkdir -p "$RESULTS_DIR"

# Benchmark list
BENCHMARKS=(
    "org.apache.flink.state.benchmark.ValueStateBenchmark.valueAdd"
    "org.apache.flink.state.benchmark.ValueStateBenchmark.valueGet"
    "org.apache.flink.state.benchmark.ValueStateBenchmark.valueUpdate"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listAdd"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listAddAll"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listAppend"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listGet"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listGetAndIterate"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listUpdate"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapAdd"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapContains"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapEntries"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapGet"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapIsEmpty"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapIterator"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapKeys"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapPutAll"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapRemove"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapUpdate"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapValues"
)

BACKENDS=("HEAP" "FORL0")

echo "ForL0 vs Heap State Backend Benchmark Summary" > "$SUMMARY_FILE"
echo "Generated at: $(date)" >> "$SUMMARY_FILE"
echo "===========================================" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

for BENCHMARK in "${BENCHMARKS[@]}"; do
    BENCH_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $(NF-1) "." $NF}')
    METHOD_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $NF}')
    echo "=== $METHOD_NAME ===" >> "$SUMMARY_FILE"
    
    for BACKEND in "${BACKENDS[@]}"; do
        OUTPUT_FILE="$RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}.csv"
        if [ -f "$OUTPUT_FILE" ]; then
            echo "" >> "$SUMMARY_FILE"
            echo "Backend: $BACKEND" >> "$SUMMARY_FILE"
            tail -n +2 "$OUTPUT_FILE" | awk -F',' '{print "  " $1 ": " $5 " " $7}' >> "$SUMMARY_FILE"
        fi
    done
    echo "" >> "$SUMMARY_FILE"
done

echo "Summary generated: $SUMMARY_FILE"
cat "$SUMMARY_FILE"
