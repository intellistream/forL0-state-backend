#!/bin/bash
# Quick VTune Profiling Script for StateMap Comparison
# Usage: ./profile_with_vtune.sh [hotspots|uarch-exploration|memory-access]

set -e

# Color output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Parse analysis type
ANALYSIS_TYPE="${1:-hotspots}"

if [[ ! "$ANALYSIS_TYPE" =~ ^(hotspots|uarch-exploration|memory-access)$ ]]; then
    echo -e "${RED}Error: Invalid analysis type: $ANALYSIS_TYPE${NC}"
    echo "Usage: $0 [hotspots|uarch-exploration|memory-access]"
    echo ""
    echo "Analysis types:"
    echo "  hotspots           - CPU hotspots with call stacks (recommended)"
    echo "  uarch-exploration  - Microarchitecture analysis (CPI, branch prediction, etc.)"
    echo "  memory-access      - Memory hierarchy analysis (cache misses, bandwidth, etc.)"
    exit 1
fi

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}VTune Profiling for StateMap${NC}"
echo -e "${BLUE}Analysis Type: ${YELLOW}$ANALYSIS_TYPE${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Check VTune availability
if ! command -v vtune &> /dev/null; then
    echo -e "${RED}Error: VTune not found in PATH${NC}"
    echo "Please ensure Intel VTune is installed and available"
    echo "Try: export PATH=/opt/intel/oneapi/vtune/latest/bin64:\$PATH"
    exit 1
fi

VTUNE_VERSION=$(vtune --version 2>&1 | head -1)
echo -e "${GREEN}VTune found: $VTUNE_VERSION${NC}"
echo ""

# Navigate to script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure benchmarks.jar exists
if [ ! -f "target/benchmarks.jar" ]; then
    echo -e "${YELLOW}benchmarks.jar not found, building...${NC}"
    mvn clean package -DskipTests -q -B
    echo -e "${GREEN}✓ Build completed${NC}"
    echo ""
fi

# Setup directories
VTUNE_DIR="/home/user/vtune-results"
RESULTS_ROOT="${FORL0_RESULTS_DIR:-${SCRIPT_DIR}/../results}"
RESULTS_DIR="${RESULTS_ROOT}/state-benchmark"
mkdir -p "$VTUNE_DIR"
mkdir -p "$RESULTS_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Define benchmarks to run (State Backend level benchmarks)
BENCHMARKS=(
    "org.apache.flink.state.benchmark.StateBenchmarkBase.valueUpdate"
    "org.apache.flink.state.benchmark.StateBenchmarkBase.valueGet"
)

BACKEND_TYPES=("FORL0" "HEAP")

echo -e "${GREEN}Running ${#BENCHMARKS[@]} benchmarks x ${#BACKEND_TYPES[@]} backend types${NC}"
echo ""

# Run each benchmark
for BENCHMARK in "${BENCHMARKS[@]}"; do
    METHOD_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $NF}')
    
    for BACKEND_TYPE in "${BACKEND_TYPES[@]}"; do
        echo -e "${BLUE}========================================${NC}"
        echo -e "${BLUE}Profiling: $METHOD_NAME - $BACKEND_TYPE${NC}"
        echo -e "${BLUE}========================================${NC}"
        
        VTUNE_RESULT_DIR="$VTUNE_DIR/state_${ANALYSIS_TYPE}_${METHOD_NAME}_${BACKEND_TYPE}_${TIMESTAMP}"
        OUTPUT_FILE="$RESULTS_DIR/${METHOD_NAME}_${BACKEND_TYPE}_vtune_${TIMESTAMP}.csv"
        
        echo -e "${YELLOW}Starting JMH benchmark...${NC}"
        
        # Start JMH in background with profiling-friendly JVM options
        java -XX:+PreserveFramePointer \
             -XX:+UnlockDiagnosticVMOptions \
             -XX:+DebugNonSafepoints \
             -XX:-OmitStackTraceInFastThrow \
             -XX:+UseG1GC \
             -jar target/benchmarks.jar "$BENCHMARK" \
             -p "backendType=$BACKEND_TYPE" \
             -rf csv -rff "$OUTPUT_FILE" \
             -wi 5 -i 10 -f 1 -t 1 \
             2>&1 | tee "$RESULTS_DIR/${METHOD_NAME}_${BACKEND_TYPE}_vtune_${TIMESTAMP}.log" &
        
        JMH_PID=$!
        
        # Wait for JMH to start and warm up (30 seconds)
        echo -e "${YELLOW}Waiting 30s for JMH warmup...${NC}"
        sleep 30
        
        # Get the actual Java process PID (JMH spawns a forked JVM)
        JAVA_PID=$(pgrep -P $JMH_PID java 2>/dev/null || pgrep -f "forked.jmh" 2>/dev/null || pgrep -f "benchmarks.jar" 2>/dev/null | tail -1)
        
        if [ -z "$JAVA_PID" ]; then
            echo -e "${RED}Error: Could not find Java process PID${NC}"
            kill $JMH_PID 2>/dev/null || true
            continue
        fi
        
        echo -e "${GREEN}Found Java process PID: $JAVA_PID${NC}"
        echo -e "${YELLOW}Starting VTune profiling (60s)...${NC}"
        
        # Build VTune command based on analysis type
        VTUNE_CMD="vtune -collect $ANALYSIS_TYPE \
                  -result-dir $VTUNE_RESULT_DIR \
                  -target-pid $JAVA_PID \
                  -duration 60"
        
        # Add analysis-specific options
        case "$ANALYSIS_TYPE" in
            hotspots)
                VTUNE_CMD="$VTUNE_CMD -knob sampling-mode=hw -knob enable-stack-collection=true -knob stack-size=4096"
                ;;
            uarch-exploration)
                VTUNE_CMD="$VTUNE_CMD -knob sampling-interval=1"
                ;;
            memory-access)
                VTUNE_CMD="$VTUNE_CMD -knob analyze-mem-objects=true"
                ;;
        esac
        
        # Run VTune
        eval $VTUNE_CMD > "$VTUNE_RESULT_DIR.log" 2>&1
        
        VTUNE_EXIT=$?
        
        if [ $VTUNE_EXIT -eq 0 ]; then
            echo -e "${GREEN}✓ VTune profiling completed${NC}"
            echo -e "${GREEN}  Result: $VTUNE_RESULT_DIR${NC}"
            
            # Generate summary report
            echo -e "${YELLOW}Generating summary report...${NC}"
            vtune -report summary -r "$VTUNE_RESULT_DIR" > "$VTUNE_RESULT_DIR/summary.txt" 2>&1 || true
            
            # Set source search directories for better source code viewing
            vtune -report summary \
                  -r "$VTUNE_RESULT_DIR" \
                  -source-search-dir /home/user/projects/forL0-state-backend/src/main/java \
                  -source-search-dir /home/user/projects/forL0-state-backend/benchmark/flink-benchmarks/src/main/java \
                  > /dev/null 2>&1 || true
        else
            echo -e "${RED}✗ VTune profiling failed (exit code: $VTUNE_EXIT)${NC}"
            echo -e "${YELLOW}  Check log: $VTUNE_RESULT_DIR.log${NC}"
        fi
        
        # Wait for JMH to complete (or kill it if VTune finished first)
        echo -e "${YELLOW}Waiting for JMH to complete...${NC}"
        wait $JMH_PID 2>/dev/null || true
        
        echo -e "${GREEN}✓ Completed: $METHOD_NAME - $MAP_TYPE${NC}"
        echo ""
    done
done

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}All profiling completed!${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""
echo -e "${GREEN}VTune results directory: $VTUNE_DIR${NC}"
echo -e "${GREEN}Benchmark results: $RESULTS_DIR${NC}"
echo ""
echo -e "${YELLOW}To view results in VTune Web UI:${NC}"
echo -e "  1. Ensure VTune web server is running:"
echo -e "     vtune-backend --web-port 18081 --data-directory $VTUNE_DIR"
echo -e "  2. Open browser: http://localhost:18081"
echo -e "  3. Configure source directories in Project Properties:"
echo -e "     - /home/user/projects/forL0-state-backend/src/main/java"
echo -e "     - /home/user/projects/forL0-state-backend/benchmark/flink-benchmarks/src/main/java"
echo ""
echo -e "${YELLOW}To view summary reports:${NC}"
for RESULT_DIR in "$VTUNE_DIR"/statemap_*_${TIMESTAMP}; do
    if [ -f "$RESULT_DIR/summary.txt" ]; then
        echo -e "  cat $RESULT_DIR/summary.txt"
    fi
done
