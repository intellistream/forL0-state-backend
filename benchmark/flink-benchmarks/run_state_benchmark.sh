#!/bin/bash
# ForL0 vs Heap State Backend Benchmark Runner
# This script runs all state benchmarks EXCEPT TTL-related tests

set -e

# Color output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}ForL0 vs Heap State Backend Benchmark${NC}"
echo -e "${BLUE}Excluding TTL tests${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Navigate to flink-benchmarks directory
cd "$(dirname "$0")"

# Clean and build
echo -e "${GREEN}Step 1: Building benchmarks...${NC}"
mvn clean package -DskipTests -q
echo -e "${GREEN}✓ Build completed${NC}"
echo ""

# Define benchmark classes (excluding TTL)
BENCHMARKS=(
    "org.apache.flink.state.benchmark.ValueStateBenchmark"
    "org.apache.flink.state.benchmark.ListStateBenchmark"
    "org.apache.flink.state.benchmark.MapStateBenchmark"
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
for BENCHMARK in "${BENCHMARKS[@]}"; do
    BENCH_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $NF}')
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Running: $BENCH_NAME${NC}"
    echo -e "${BLUE}========================================${NC}"
    
    for BACKEND in "${BACKENDS[@]}"; do
        echo -e "${GREEN}Backend: $BACKEND${NC}"
        OUTPUT_FILE="$RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}.csv"
        
        # Run benchmark
        java -jar target/benchmarks.jar "$BENCHMARK" \
            -p "backendType=$BACKEND" \
            -rf csv -rff "$OUTPUT_FILE" \
            -wi 3 -i 5 -f 1 -t 1 \
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
    BENCH_NAME=$(echo "$BENCHMARK" | awk -F'.' '{print $NF}')
    echo "=== $BENCH_NAME ===" >> "$SUMMARY_FILE"
    
    for BACKEND in "${BACKENDS[@]}"; do
        OUTPUT_FILE="$RESULTS_DIR/${BENCH_NAME}_${BACKEND}_${TIMESTAMP}.csv"
        if [ -f "$OUTPUT_FILE" ]; then
            echo "" >> "$SUMMARY_FILE"
            echo "Backend: $BACKEND" >> "$SUMMARY_FILE"
            # Extract key metrics (skip header, get score column)
            tail -n +2 "$OUTPUT_FILE" | awk -F',' '{print "  " $1 ": " $5 " " $6}' >> "$SUMMARY_FILE"
        fi
    done
    echo "" >> "$SUMMARY_FILE"
done

cat "$SUMMARY_FILE"
echo ""
echo -e "${GREEN}Summary saved to: $SUMMARY_FILE${NC}"
