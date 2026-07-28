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
ENABLE_VTUNE=false
SKIP_BUILD=false
QUICK_MODE=false
while getopts "pvsq" opt; do
  case $opt in
    p) ENABLE_PROFILING=true ;;
    v) ENABLE_VTUNE=true ;;
    s) SKIP_BUILD=true ;;
    q) QUICK_MODE=true ;;
    *) echo "Usage: $0 [-p] [-v] [-s] [-q]"; echo "  -p: Enable async-profiler for flame graph generation"; echo "  -v: Enable VTune for memory-access profiling"; echo "  -s: Skip Maven build (use existing benchmarks.jar)"; echo "  -q: Quick mode (less iterations, faster but less accurate)"; exit 1 ;;
  esac
done

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}ForL0 vs Heap State Backend Benchmark${NC}"
echo -e "${BLUE}Excluding TTL tests${NC}"
if [ "$ENABLE_PROFILING" = true ]; then
    echo -e "${YELLOW}Profiling: async-profiler ENABLED${NC}"
fi
if [ "$ENABLE_VTUNE" = true ]; then
    echo -e "${YELLOW}Profiling: VTune memory-access ENABLED${NC}"
    echo -e "${YELLOW}  - Each benchmark × backend will be profiled separately${NC}"
    echo -e "${YELLOW}  - VTune will skip warmup and profile measurement phase only${NC}"
fi
if [ "$SKIP_BUILD" = true ]; then
    echo -e "${YELLOW}Build: SKIPPED (using existing jar)${NC}"
fi
if [ "$QUICK_MODE" = true ]; then
    echo -e "${YELLOW}Mode: QUICK (wi=4, i=6, f=2)${NC}"
else
    echo -e "${GREEN}Mode: FULL (wi=5, i=10, f=3)${NC}"
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
    
    RESULTS_DIR="${FORL0_RESULTS_DIR:-${SCRIPT_DIR}/../results}"
    mkdir -p "$RESULTS_DIR/profiles"
    RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"
    PROFILER_ARGS="-prof async:libPath=$ASYNC_PROFILER_LIB;output=flamegraph;dir=${RESULTS_DIR}/profiles"
    echo -e "${GREEN}Using async-profiler: $ASYNC_PROFILER_LIB${NC}"
fi

# Check VTune if enabled
VTUNE_ENABLED=false
VTUNE_CMD=""
VTUNE_RESULTS_DIR=""
if [ "$ENABLE_VTUNE" = true ]; then
    # Try to find vtune in PATH first
    VTUNE_CMD=$(command -v vtune 2>/dev/null || true)
    
    # If not in PATH, try standard Intel installation paths
    if [ -z "$VTUNE_CMD" ]; then
        VTUNE_SEARCH_PATHS=(
            "/opt/intel/oneapi/vtune/latest/bin64/vtune"
            "/opt/intel/vtune_profiler/latest/bin64/vtune"
            "$HOME/intel/oneapi/vtune/latest/bin64/vtune"
        )
        
        for path in "${VTUNE_SEARCH_PATHS[@]}"; do
            if [ -f "$path" ] && [ -x "$path" ]; then
                VTUNE_CMD="$path"
                break
            fi
        done
    fi
    
    # Check if we found vtune
    if [ -z "$VTUNE_CMD" ]; then
        echo -e "${RED}Error: VTune not found${NC}"
        echo "Searched in:"
        echo "  - PATH"
        echo "  - /opt/intel/oneapi/vtune/latest/bin64/vtune"
        echo "  - /opt/intel/vtune_profiler/latest/bin64/vtune"
        echo "  - ~/intel/oneapi/vtune/latest/bin64/vtune"
        echo ""
        echo "Please either:"
        echo "  1. Add VTune to PATH: export PATH=/opt/intel/oneapi/vtune/latest/bin64:\$PATH"
        echo "  2. Or install Intel VTune Profiler"
        exit 1
    fi
    
    # Verify vtune works
    VTUNE_VERSION=$("$VTUNE_CMD" --version 2>&1 | head -1)
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: VTune found but failed to run: $VTUNE_CMD${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}VTune found: $VTUNE_VERSION${NC}"
    echo -e "${GREEN}VTune path: $VTUNE_CMD${NC}"
    
    VTUNE_RESULTS_DIR="/home/user/vtune-results/state-benchmark"
    mkdir -p "$VTUNE_RESULTS_DIR"
    VTUNE_ENABLED=true
    echo -e "${GREEN}VTune results will be saved to: $VTUNE_RESULTS_DIR${NC}"
    echo ""
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
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapGet"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapPutAll"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapRemove"
    "org.apache.flink.state.benchmark.MapStateBenchmark.mapUpdate"
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
        if [ "$QUICK_MODE" = true ]; then
            JMH_ARGS="-wi 4 -i 6 -f 2 -t 1"
        else
            JMH_ARGS="-wi 5 -i 10 -f 3 -t 1"
        fi
        
        # If VTune is enabled, run benchmark with VTune profiling
        if [ "$VTUNE_ENABLED" = true ]; then
            VTUNE_RESULT_DIR="$VTUNE_RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}"
            
            # Add JVM options for better profiling
            JVM_OPTS="-XX:+PreserveFramePointer -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:+AlwaysPreTouch -XX:-UseBiasedLocking"
            
            # Calculate warmup time to skip (default: wi=5, each ~10s = 50s, add 10s buffer)
            if [ "$QUICK_MODE" = true ]; then
                WARMUP_WAIT=50  # wi=4, ~40s + 10s buffer
            else
                WARMUP_WAIT=60  # wi=5, ~50s + 10s buffer
            fi
            
            # Start JMH in background with log output to a file for monitoring
            echo -e "${YELLOW}Starting JMH benchmark in background...${NC}"
            echo -e "${YELLOW}  Benchmark: $BENCH_NAME${NC}"
            echo -e "${YELLOW}  Backend: $BACKEND${NC}"
            echo -e "${YELLOW}  JMH Args: $JMH_ARGS${NC}"
            
            java $JVM_OPTS \
                -jar target/benchmarks.jar "$BENCHMARK" \
                -p "backendType=$BACKEND" \
                -rf csv -rff "$OUTPUT_FILE" \
                $JMH_ARGS \
                $PROFILER_ARGS \
                2>&1 | tee "$RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}.log" &
            
            JMH_PID=$!
            
            # Wait for JMH to start and complete warmup
            echo -e "${YELLOW}Waiting ${WARMUP_WAIT}s for JMH warmup to complete...${NC}"
            echo -e "${YELLOW}  (VTune will profile only the measurement iterations)${NC}"
            sleep $WARMUP_WAIT
            
            # Get the actual Java process PID (JMH spawns a forked JVM)
            JAVA_PID=$(pgrep -P $JMH_PID java 2>/dev/null || pgrep -f "forked.jmh" 2>/dev/null || pgrep -f "benchmarks.jar" 2>/dev/null | tail -1)
            
            if [ -z "$JAVA_PID" ]; then
                echo -e "${RED}Error: Could not find Java process PID${NC}"
                echo -e "${YELLOW}JMH might have completed too fast or failed to start${NC}"
                kill $JMH_PID 2>/dev/null || true
                continue
            fi
            
            echo -e "${GREEN}Found Java process PID: $JAVA_PID${NC}"
            echo -e "${YELLOW}Starting VTune memory-access profiling (60s)...${NC}"
            echo -e "${YELLOW}  This will capture the measurement phase only${NC}"
            
            # Run VTune memory-access analysis
            "$VTUNE_CMD" -collect memory-access \
                  -result-dir "$VTUNE_RESULT_DIR" \
                  -target-pid $JAVA_PID \
                  -duration 60 \
                  -knob analyze-mem-objects=true \
                  > "$VTUNE_RESULT_DIR.log" 2>&1
            
            VTUNE_EXIT=$?
            
            if [ $VTUNE_EXIT -eq 0 ]; then
                echo -e "${GREEN}✓ VTune profiling completed${NC}"
                echo -e "${GREEN}  Result: $VTUNE_RESULT_DIR${NC}"
                
                # Generate summary report
                echo -e "${YELLOW}Generating summary report...${NC}"
                "$VTUNE_CMD" -report summary -r "$VTUNE_RESULT_DIR" > "$VTUNE_RESULT_DIR/summary.txt" 2>&1 || true
                
                # Set source search directories for better source code viewing
                "$VTUNE_CMD" -report summary \
                      -r "$VTUNE_RESULT_DIR" \
                      -source-search-dir "$SCRIPT_DIR/../../src/main/java" \
                      -source-search-dir "$SCRIPT_DIR/src/main/java" \
                      > /dev/null 2>&1 || true
            else
                echo -e "${RED}✗ VTune profiling failed (exit code: $VTUNE_EXIT)${NC}"
                echo -e "${YELLOW}  Check log: $VTUNE_RESULT_DIR.log${NC}"
            fi
            
            # Wait for JMH to complete
            echo -e "${YELLOW}Waiting for JMH to complete...${NC}"
            wait $JMH_PID 2>/dev/null || true
        else
            # Normal run without VTune
            java -jar target/benchmarks.jar "$BENCHMARK" \
                -p "backendType=$BACKEND" \
                -rf csv -rff "$OUTPUT_FILE" \
                $JMH_ARGS \
                -jvmArgs "-XX:+UseG1GC -XX:+AlwaysPreTouch -XX:-UseBiasedLocking" \
                $PROFILER_ARGS \
                2>&1 | tee "$RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}.log"
        fi
        
        echo -e "${GREEN}✓ Completed: $BENCH_NAME with $BACKEND${NC}"
        echo -e "${GREEN}  Results: $OUTPUT_FILE${NC}"
        echo ""
    done
done

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}All benchmarks completed!${NC}"
echo -e "${BLUE}Results saved to: $RESULTS_DIR${NC}"
if [ "$VTUNE_ENABLED" = true ]; then
    echo -e "${BLUE}VTune results saved to: $VTUNE_RESULTS_DIR${NC}"
fi
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

# Display VTune results info if enabled
if [ "$VTUNE_ENABLED" = true ]; then
    echo ""
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}VTune Memory-Access Profiling Results${NC}"
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${GREEN}Results directory: $VTUNE_RESULTS_DIR${NC}"
    echo ""
    echo -e "${YELLOW}To view results in VTune Web UI:${NC}"
    echo -e "  1. Start VTune web server:"
    echo -e "     $VTUNE_CMD-backend --web-port 18081 --data-directory $VTUNE_RESULTS_DIR"
    echo -e "  2. Open browser: http://localhost:18081"
    echo -e "  3. Configure source directories in Project Properties:"
    echo -e "     - $SCRIPT_DIR/../../src/main/java"
    echo -e "     - $SCRIPT_DIR/src/main/java"
    echo ""
    echo -e "${YELLOW}To view summary reports:${NC}"
    for RESULT_DIR in "$VTUNE_RESULTS_DIR"/*_${TIMESTAMP}; do
        if [ -f "$RESULT_DIR/summary.txt" ]; then
            echo -e "  cat $RESULT_DIR/summary.txt"
        fi
    done
    echo ""
fi

# Generate comparison chart
echo ""
echo -e "${GREEN}Generating comparison chart...${NC}"
CHART_SCRIPT="$SCRIPT_DIR/../scripts/plot_comparison.py"
if [ -f "$CHART_SCRIPT" ]; then
    # Use CSV directory mode with timestamp for error bars support
    python3 "$CHART_SCRIPT" "$RESULTS_DIR" "$RESULTS_DIR/comparison_chart_${TIMESTAMP}.png" "$TIMESTAMP"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Chart generated: $RESULTS_DIR/comparison_chart_${TIMESTAMP}.png${NC}"
    else
        echo -e "${YELLOW}⚠ Chart generation failed${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Chart script not found: $CHART_SCRIPT${NC}"
fi

