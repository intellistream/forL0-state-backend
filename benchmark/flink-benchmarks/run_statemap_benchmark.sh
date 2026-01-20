#!/bin/bash
# StateMap Benchmark Runner
# Compares ForL0StateMap vs CopyOnWriteStateMap performance

set -e

# Color output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Parse command line arguments
ENABLE_PROFILING=false
ENABLE_VTUNE=false
VTUNE_ANALYSIS_TYPE="hotspots"
SKIP_BUILD=false
QUICK_MODE=false
while getopts "pva:sq" opt; do
  case $opt in
    p) ENABLE_PROFILING=true ;;
    v) ENABLE_VTUNE=true ;;
    a) VTUNE_ANALYSIS_TYPE=$OPTARG ;;
    s) SKIP_BUILD=true ;;
    q) QUICK_MODE=true ;;
    *) echo "Usage: $0 [-p] [-v] [-a analysis_type] [-s] [-q]"
       echo "  -p: Enable async-profiler for flame graph generation"
       echo "  -v: Enable VTune profiling"
       echo "  -a: VTune analysis type (hotspots|uarch-exploration|memory-access), default: hotspots"
       echo "  -s: Skip Maven build (use existing benchmarks.jar)"
       echo "  -q: Quick mode (less iterations, faster but less accurate)"
       exit 1 ;;
  esac
done

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}StateMap Benchmark${NC}"
echo -e "${BLUE}ForL0StateMap vs CopyOnWriteStateMap${NC}"
if [ "$ENABLE_PROFILING" = true ]; then
    echo -e "${YELLOW}Async-Profiler: ENABLED${NC}"
fi
if [ "$ENABLE_VTUNE" = true ]; then
    echo -e "${YELLOW}VTune Profiler: ENABLED ($VTUNE_ANALYSIS_TYPE)${NC}"
fi
if [ "$SKIP_BUILD" = true ]; then
    echo -e "${YELLOW}Build: SKIPPED (using existing jar)${NC}"
fi
if [ "$QUICK_MODE" = true ]; then
    echo -e "${YELLOW}Mode: QUICK (wi=3, i=5, f=1)${NC}"
else
    echo -e "${GREEN}Mode: FULL (wi=5, i=10, f=3)${NC}"
fi
echo -e "${BLUE}======================================${NC}"
echo ""

# Navigate to flink-benchmarks directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Check async-profiler if profiling is enabled
PROFILER_ARGS=""
if [ "$ENABLE_PROFILING" = true ]; then
    if [ -n "$ASYNC_PROFILER_HOME" ]; then
        ASYNC_PROFILER_LIB="$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so"
    fi
    
    if [ -z "$ASYNC_PROFILER_LIB" ]; then
        echo -e "${RED}Error: Neither ASYNC_PROFILER_HOME nor ASYNC_PROFILER_LIB is set${NC}"
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

# Build
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${GREEN}Step 1: Building benchmarks...${NC}"
    mvn clean package -DskipTests -q -B
    echo -e "${GREEN}✓ Build completed${NC}"
    echo ""
else
    echo -e "${YELLOW}Skipping build, checking for existing jar...${NC}"
    if [ ! -f "target/benchmarks.jar" ]; then
        echo -e "${RED}Error: target/benchmarks.jar not found!${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Found existing benchmarks.jar${NC}"
    echo ""
fi

# Define benchmark methods (StateMap interface methods)
BENCHMARKS=(
    "org.apache.flink.runtime.state.heap.StateMapBenchmark.mapPut$"
    "org.apache.flink.runtime.state.heap.StateMapBenchmark.mapGet$"
    "org.apache.flink.runtime.state.heap.StateMapBenchmark.mapTransform$"
    "org.apache.flink.runtime.state.heap.StateMapBenchmark.mapPutAndGetOld$"
    "org.apache.flink.runtime.state.heap.StateMapBenchmark.mapContainsKey$"
)

# Define map types (different from state backend types)
MAP_TYPES=("FORL0" "COPYONWRITE")

# Output directory
RESULTS_DIR="../../results/statemap-benchmark"
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
    
    for MAP_TYPE in "${MAP_TYPES[@]}"; do
        echo -e "${GREEN}MapType: $MAP_TYPE${NC}"
        OUTPUT_FILE="$RESULTS_DIR/${BENCH_NAME}_${MAP_TYPE}_${TIMESTAMP}.csv"
        
        # JMH args based on mode
        if [ "$QUICK_MODE" = true ]; then
            JMH_ARGS="-wi 3 -i 5 -f 1 -t 1"
        else
            JMH_ARGS="-wi 5 -i 10 -f 3 -t 1"
        fi
        
        # Base JVM args for better profiling
        JVM_ARGS="-XX:+UseG1GC -XX:+AlwaysPreTouch -XX:-UseBiasedLocking"
        
        # Add profiling-specific JVM args
        if [ "$ENABLE_VTUNE" = true ] || [ "$ENABLE_PROFILING" = true ]; then
            JVM_ARGS="$JVM_ARGS -XX:+PreserveFramePointer -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:-OmitStackTraceInFastThrow"
        fi
        
        # Start VTune in background if enabled
        VTUNE_PID=""
        if [ "$ENABLE_VTUNE" = true ]; then
            VTUNE_DIR="/home/user/vtune-results"
            mkdir -p "$VTUNE_DIR"
            VTUNE_RESULT_DIR="$VTUNE_DIR/statemap_${VTUNE_ANALYSIS_TYPE}_${MAP_TYPE}_${TIMESTAMP}"
            
            echo -e "${YELLOW}  VTune will attach in 20s for $VTUNE_ANALYSIS_TYPE analysis${NC}"
            
            # Start JMH in background and get PID
            java -jar target/benchmarks.jar "$BENCHMARK" \
                -p "mapType=$MAP_TYPE" \
                -rf csv -rff "$OUTPUT_FILE" \
                $JMH_ARGS \
                -jvmArgs "$JVM_ARGS" \
                $PROFILER_ARGS \
                2>&1 | tee "$RESULTS_DIR/${BENCH_NAME}_${MAP_TYPE}_${TIMESTAMP}.log" &
            
            JMH_PID=$!
            
            # Wait for JMH to start and warm up
            sleep 20
            
            # Get actual Java process PID (JMH spawns a new JVM)
            JAVA_PID=$(pgrep -P $JMH_PID java || pgrep -f "benchmarks.jar")
            
            if [ -n "$JAVA_PID" ]; then
                echo -e "${YELLOW}  Starting VTune on PID: $JAVA_PID${NC}"
                
                # Start VTune profiling
                vtune -collect $VTUNE_ANALYSIS_TYPE \
                      -knob sampling-mode=hw \
                      -knob enable-stack-collection=true \
                      -knob stack-size=4096 \
                      -result-dir "$VTUNE_RESULT_DIR" \
                      -target-pid $JAVA_PID \
                      -duration 60 \
                      > "$VTUNE_RESULT_DIR.log" 2>&1 &
                
                VTUNE_PID=$!
                echo -e "${YELLOW}  VTune started (PID: $VTUNE_PID), profiling for 60s${NC}"
            fi
            
            # Wait for JMH to complete
            wait $JMH_PID
            
            # Wait for VTune to complete if still running
            if [ -n "$VTUNE_PID" ]; then
                wait $VTUNE_PID 2>/dev/null || true
                echo -e "${GREEN}  VTune profiling completed: $VTUNE_RESULT_DIR${NC}"
            fi
        else
            # Normal execution without VTune
            java -jar target/benchmarks.jar "$BENCHMARK" \
                -p "mapType=$MAP_TYPE" \
                -rf csv -rff "$OUTPUT_FILE" \
                $JMH_ARGS \
                -jvmArgs "$JVM_ARGS" \
                $PROFILER_ARGS \
                2>&1 | tee "$RESULTS_DIR/${BENCH_NAME}_${MAP_TYPE}_${TIMESTAMP}.log"
        fi
        
        echo -e "${GREEN}✓ Completed: $BENCH_NAME with $MAP_TYPE${NC}"
        echo ""
    done
done

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}All StateMap benchmarks completed!${NC}"
echo -e "${BLUE}Results saved to: $RESULTS_DIR${NC}"
echo -e "${BLUE}======================================${NC}"

# Generate summary
echo ""
echo -e "${GREEN}Generating summary...${NC}"
SUMMARY_FILE="$RESULTS_DIR/summary_${TIMESTAMP}.txt"

echo "StateMap Benchmark Summary" > "$SUMMARY_FILE"
echo "ForL0StateMap vs CopyOnWriteStateMap" >> "$SUMMARY_FILE"
echo "Generated at: $(date)" >> "$SUMMARY_FILE"
echo "===========================================" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

for BENCHMARK in "${BENCHMARKS[@]}"; do
    BENCH_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $(NF-1) "." $NF}')
    METHOD_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $NF}')
    echo "=== $METHOD_NAME ===" >> "$SUMMARY_FILE"
    
    for MAP_TYPE in "${MAP_TYPES[@]}"; do
        OUTPUT_FILE="$RESULTS_DIR/${BENCH_NAME}_${MAP_TYPE}_${TIMESTAMP}.csv"
        if [ -f "$OUTPUT_FILE" ]; then
            echo "  MapType: $MAP_TYPE" >> "$SUMMARY_FILE"
            tail -n +2 "$OUTPUT_FILE" | awk -F',' '{print "    " $1 ": " $5 " " $7}' >> "$SUMMARY_FILE"
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
CHART_SCRIPT="$SCRIPT_DIR/../scripts/plot_statemap_comparison.py"
if [ -f "$CHART_SCRIPT" ]; then
    python3 "$CHART_SCRIPT" "$RESULTS_DIR" "$RESULTS_DIR/comparison_chart_${TIMESTAMP}.png" "$TIMESTAMP"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Chart generated: $RESULTS_DIR/comparison_chart_${TIMESTAMP}.png${NC}"
    else
        echo -e "${YELLOW}⚠ Chart generation failed${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Chart script not found: $CHART_SCRIPT${NC}"
    echo -e "${YELLOW}  You can use: python3 ../scripts/plot_comparison.py $RESULTS_DIR chart.png $TIMESTAMP${NC}"
fi
