#!/bin/bash
# ForL0 vs Heap State Backend Benchmark Runner
# This script runs all state benchmarks EXCEPT TTL-related tests

set -e

# Color output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Parse command line arguments
ENABLE_PROFILING=false
SKIP_BUILD=false
while getopts "ps" opt; do
  case $opt in
    p) ENABLE_PROFILING=true ;;
    s) SKIP_BUILD=true ;;
    *) echo "Usage: $0 [-p] [-s]"; echo "  -p: Enable async-profiler for flame graph generation"; echo "  -s: Skip Maven build (use existing benchmarks.jar)"; exit 1 ;;
  esac
done

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}ForL0 vs Heap State Backend Benchmark${NC}"
echo -e "${BLUE}Excluding TTL tests${NC}"
if [ "$ENABLE_PROFILING" = true ]; then
    echo -e "${YELLOW}Profiling: ENABLED${NC}"
fi
if [ "$SKIP_BUILD" = true ]; then
    echo -e "${YELLOW}Build: SKIPPED (using existing jar)${NC}"
fi
echo -e "${BLUE}======================================${NC}"
echo ""

# Navigate to flink-benchmarks directory and save absolute path
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Check async-profiler if profiling is enabled
PROFILER_ARGS=""
if [ "$ENABLE_PROFILING" = true ]; then
    # Support both ASYNC_PROFILER_LIB and ASYNC_PROFILER_HOME
    if [ -n "$ASYNC_PROFILER_HOME" ]; then
        ASYNC_PROFILER_LIB="$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so"
    fi
    
    if [ -z "$ASYNC_PROFILER_LIB" ]; then
        echo -e "${RED}Error: Neither ASYNC_PROFILER_HOME nor ASYNC_PROFILER_LIB is set${NC}"
        echo -e "${YELLOW}Please set one of:${NC}"
        echo "  export ASYNC_PROFILER_HOME=/path/to/async-profiler"
        echo "  export ASYNC_PROFILER_LIB=/path/to/libasyncProfiler.so"
        exit 1
    fi
    
    if [ ! -f "$ASYNC_PROFILER_LIB" ]; then
        echo -e "${RED}Error: libasyncProfiler.so not found: $ASYNC_PROFILER_LIB${NC}"
        exit 1
    fi
    
    PROFILER_ARGS="-prof async:libPath=$ASYNC_PROFILER_LIB;output=flamegraph;dir=../../results/profiles"
    echo -e "${GREEN}Using async-profiler: $ASYNC_PROFILER_LIB${NC}"
    mkdir -p ../../results/profiles
fi

# Clean and build
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${GREEN}Step 1: Building benchmarks...${NC}"
    mvn clean package -DskipTests -q -B
    echo -e "${GREEN}✓ Build completed${NC}"
    echo ""
else
    echo -e "${YELLOW}Skipping build, checking for existing jar...${NC}"
    if [ ! -f "target/benchmarks.jar" ]; then
        echo -e "${RED}Error: target/benchmarks.jar not found!${NC}"
        echo -e "${YELLOW}Please build first with: mvn clean package -DskipTests${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Found existing benchmarks.jar${NC}"
    echo ""
fi

# Define specific benchmark methods to run (matching the chart)
# Format: "ClassName.methodName"
BENCHMARKS=(
    "org.apache.flink.state.benchmark.ValueStateBenchmark.valueAdd"
    "org.apache.flink.state.benchmark.ValueStateBenchmark.valueGet"
    "org.apache.flink.state.benchmark.ValueStateBenchmark.valueUpdate"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listAdd"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listAppend"
    "org.apache.flink.state.benchmark.ListStateBenchmark.listGet"
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

# Define backend types
BACKENDS=("HEAP" "FORL0")

# Output directory for results
RESULTS_DIR="../../results/state-benchmark"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo -e "${YELLOW}Results will be saved to: $RESULTS_DIR${NC}"
echo ""

# Run benchmarks
TOTAL_TESTS=${#BENCHMARKS[@]}
CURRENT_TEST=0

for BENCHMARK in "${BENCHMARKS[@]}"; do
    CURRENT_TEST=$((CURRENT_TEST + 1))
    BENCH_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $(NF-1) "." $NF}')
    
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}[$CURRENT_TEST/$TOTAL_TESTS] Running: $BENCH_NAME${NC}"
    echo -e "${BLUE}========================================${NC}"
    
    for BACKEND in "${BACKENDS[@]}"; do
        echo -e "${GREEN}Backend: $BACKEND${NC}"
        OUTPUT_FILE="$RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}.csv"
        
        # Run benchmark (use absolute path to jar)
        cd "$SCRIPT_DIR"
        java -jar target/benchmarks.jar "$BENCHMARK" \
            -p "backendType=$BACKEND" \
            -rf csv -rff "$OUTPUT_FILE" \
            -wi 3 -i 5 -f 1 -t 1 \
            $PROFILER_ARGS \
            2>&1 | tee "$RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}.log"
        
        echo -e "${GREEN}✓ Completed: $BENCH_NAME with $BACKEND${NC}"
        echo -e "${GREEN}  Results: $OUTPUT_FILE${NC}"
        echo ""
    done
done

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}All benchmarks completed!${NC}"
echo -e "${BLUE}Results saved to: $RESULTS_DIR${NC}"
echo -e "${BLUE}======================================${NC}"

# Generate summary
echo ""
echo -e "${GREEN}Generating summary...${NC}"
SUMMARY_FILE="$RESULTS_DIR/summary_${TIMESTAMP}.txt"

echo "ForL0 vs Heap State Backend Benchmark Summary" > "$SUMMARY_FILE"
echo "Generated at: $(date)" >> "$SUMMARY_FILE"
echo "===========================================" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

for BENCHMARK in "${BENCHMARKS[@]}"; do
    # Extract benchmark name from full class path
    # e.g., org.apache.flink.state.benchmark.ValueStateBenchmark.valueAdd -> ValueStateBenchmark.valueAdd
    BENCH_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $(NF-1) "." $NF}')
    METHOD_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $NF}')
    echo "=== $METHOD_NAME ===" >> "$SUMMARY_FILE"
    
    for BACKEND in "${BACKENDS[@]}"; do
        OUTPUT_FILE="$RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}.csv"
        if [ -f "$OUTPUT_FILE" ]; then
            echo "" >> "$SUMMARY_FILE"
            echo "Backend: $BACKEND" >> "$SUMMARY_FILE"
            # Extract key metrics: Benchmark name, Score, Unit
            tail -n +2 "$OUTPUT_FILE" | awk -F',' '{print "  " $1 ": " $5 " " $7}' >> "$SUMMARY_FILE"
        fi
    done
    echo "" >> "$SUMMARY_FILE"
done

cat "$SUMMARY_FILE"
echo ""
echo -e "${GREEN}Summary saved to: $SUMMARY_FILE${NC}"

# Generate comparison chart
echo ""
echo -e "${GREEN}Generating comparison chart...${NC}"
CHART_SCRIPT="$SCRIPT_DIR/../scripts/plot_comparison.py"
if [ -f "$CHART_SCRIPT" ]; then
    python3 "$CHART_SCRIPT" "$SUMMARY_FILE" "$RESULTS_DIR/comparison_chart_${TIMESTAMP}.png"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Chart generated: $RESULTS_DIR/comparison_chart_${TIMESTAMP}.png${NC}"
    else
        echo -e "${YELLOW}⚠ Chart generation failed${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Chart script not found: $CHART_SCRIPT${NC}"
fi
