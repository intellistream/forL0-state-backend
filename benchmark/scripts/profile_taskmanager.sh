#!/bin/bash
# Profile Flink TaskManager with Intel VTune
# Usage: ./profile_taskmanager.sh [uarch|memory] [duration_seconds] [output_name]

set -e

# Default values
ANALYSIS_TYPE=${1:-uarch}
DURATION=${2:-60}
OUTPUT_NAME=${3:-vtune_tm_$(date +%Y%m%d_%H%M%S)}
OUTPUT_DIR="${HOME}/vtune-results/${OUTPUT_NAME}"

# Check if VTune is available
if ! command -v vtune &> /dev/null; then
    echo "ERROR: VTune not found. Please set up Intel oneAPI environment:"
    echo "  source /opt/intel/oneapi/setvars.sh"
    exit 1
fi

# Find TaskManager process
TM_PID=$(pgrep -f TaskManagerRunner | head -1)
if [ -z "$TM_PID" ]; then
    echo "ERROR: TaskManager process not found"
    echo "Please start Flink cluster first: \$FLINK_HOME/bin/start-cluster.sh"
    exit 1
fi

echo "============================================"
echo "VTune Profiler for Flink TaskManager"
echo "============================================"
echo "TaskManager PID: $TM_PID"
echo "Analysis Type:   $ANALYSIS_TYPE"
echo "Duration:        ${DURATION}s"
echo "Output Dir:      $OUTPUT_DIR"
echo "============================================"

# Set analysis type
case $ANALYSIS_TYPE in
    uarch|u)
        ANALYSIS="uarch-exploration"
        DESC="Microarchitecture Exploration"
        ;;
    memory|mem|m)
        ANALYSIS="memory-access"
        DESC="Memory Access Analysis"
        ;;
    hotspots|hot|h)
        ANALYSIS="hotspots"
        DESC="Hotspots Analysis"
        ;;
    *)
        echo "ERROR: Unknown analysis type: $ANALYSIS_TYPE"
        echo "Supported: uarch, memory, hotspots"
        exit 1
        ;;
esac

echo ""
echo "Starting $DESC..."
echo "Press Ctrl+C to stop early"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Start profiling
vtune -collect $ANALYSIS \
    -target-pid $TM_PID \
    -duration $DURATION \
    -result-dir "$OUTPUT_DIR" \
    -finalization-mode=full

echo ""
echo "============================================"
echo "Profiling completed!"
echo "============================================"
echo "Result directory: $OUTPUT_DIR"
echo ""
echo "View results:"
echo "  vtune-gui $OUTPUT_DIR"
echo ""
echo "Generate text report:"
echo "  vtune -report summary -result-dir $OUTPUT_DIR"
echo "  vtune -report hotspots -result-dir $OUTPUT_DIR"
echo ""
