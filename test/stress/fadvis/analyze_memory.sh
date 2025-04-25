#!/bin/bash

# This script helps analyze memory usage of a process, focusing on pagecache
# Usage: ./analyze_memory.sh <PID>

if [ $# -ne 1 ]; then
    echo "Usage: $0 <PID>"
    exit 1
fi

PID=$1
OUTPUT_DIR="memory_analysis_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"

echo "Analyzing memory usage for process $PID"
echo "Results will be saved to $OUTPUT_DIR"

# Check if process exists
if ! ps -p $PID > /dev/null; then
    echo "Process $PID does not exist"
    exit 1
fi

# Get basic process info
ps -o pid,ppid,rss,vsz,pmem,pcpu,cmd -p $PID > "$OUTPUT_DIR/process_info.txt"

# Get detailed memory maps
cat /proc/$PID/maps > "$OUTPUT_DIR/memory_maps.txt" 2>/dev/null

# Get memory status from /proc
cat /proc/$PID/status > "$OUTPUT_DIR/proc_status.txt" 2>/dev/null

# Get smaps (detailed memory mapping info including RSS, PSS, etc.)
cat /proc/$PID/smaps > "$OUTPUT_DIR/smaps.txt" 2>/dev/null

# Extract key memory metrics from smaps
echo "Extracting key memory metrics..."
echo "RSS (Resident Set Size) - Total memory used by process in RAM:" > "$OUTPUT_DIR/memory_summary.txt"
grep -A 25 "^Rss:" "$OUTPUT_DIR/smaps.txt" | grep "^Rss:" | awk '{sum += $2} END {print sum " kB"}' >> "$OUTPUT_DIR/memory_summary.txt"

echo "\nPSS (Proportional Set Size) - RSS adjusted for sharing:" >> "$OUTPUT_DIR/memory_summary.txt"
grep -A 25 "^Pss:" "$OUTPUT_DIR/smaps.txt" | grep "^Pss:" | awk '{sum += $2} END {print sum " kB"}' >> "$OUTPUT_DIR/memory_summary.txt"

echo "\nSwap - Memory swapped out to disk:" >> "$OUTPUT_DIR/memory_summary.txt"
grep -A 25 "^Swap:" "$OUTPUT_DIR/smaps.txt" | grep "^Swap:" | awk '{sum += $2} END {print sum " kB"}' >> "$OUTPUT_DIR/memory_summary.txt"

echo "\nCached - Memory in pagecache:" >> "$OUTPUT_DIR/memory_summary.txt"
grep -A 25 "^Referenced:" "$OUTPUT_DIR/smaps.txt" | grep "^Referenced:" | awk '{sum += $2} END {print sum " kB"}' >> "$OUTPUT_DIR/memory_summary.txt"

echo "\nAnonymous - Non-file backed memory:" >> "$OUTPUT_DIR/memory_summary.txt"
grep -A 25 "^Anonymous:" "$OUTPUT_DIR/smaps.txt" | grep "^Anonymous:" | awk '{sum += $2} END {print sum " kB"}' >> "$OUTPUT_DIR/memory_summary.txt"

echo "\nLazy Free - Memory waiting to be freed:" >> "$OUTPUT_DIR/memory_summary.txt"
grep -A 25 "^LazyFree:" "$OUTPUT_DIR/smaps.txt" | grep "^LazyFree:" | awk '{sum += $2} END {print sum " kB"}' >> "$OUTPUT_DIR/memory_summary.txt"

# Get overall system memory stats
free -m > "$OUTPUT_DIR/system_memory.txt"
cat /proc/meminfo >> "$OUTPUT_DIR/system_memory.txt"

echo "Analysis complete. Results saved to $OUTPUT_DIR"
echo "Summary:"
cat "$OUTPUT_DIR/memory_summary.txt"
