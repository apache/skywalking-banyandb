#!/bin/bash
# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

echo "=== Starting I/O Load Generation ==="
echo "Duration: ${LOAD_DURATION}s"
echo "Concurrency: ${LOAD_CONCURRENCY}"
echo "Data size: ${TEST_DATA_SIZE}"

# Create test directory
TEST_DIR="/data/test_$$"
mkdir -p $TEST_DIR
cd $TEST_DIR

# Function to generate fadvise patterns
generate_fadvise_patterns() {
    echo "Generating fadvise patterns..."
    
    # Create a test file
    dd if=/dev/urandom of=fadvise_test.dat bs=1M count=10 2>/dev/null
    
    # Use a simple C program to call fadvise (compile inline)
    cat > fadvise_test.c << 'EOF'
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

int main() {
    int fd = open("fadvise_test.dat", O_RDONLY);
    if (fd < 0) return 1;
    
    // Sequential access
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    
    // Will need
    posix_fadvise(fd, 0, 4096, POSIX_FADV_WILLNEED);
    
    // Don't need
    posix_fadvise(fd, 0, 4096, POSIX_FADV_DONTNEED);
    
    // Random access
    posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
    
    close(fd);
    printf("fadvise calls completed\n");
    return 0;
}
EOF
    
    gcc -o fadvise_test fadvise_test.c 2>/dev/null || echo "Could not compile fadvise test"
    
    # Run the test multiple times
    for i in $(seq 1 10); do
        ./fadvise_test 2>/dev/null || true
        sleep 0.1
    done
}

# Function to generate cache miss patterns
generate_cache_patterns() {
    echo "Generating cache miss patterns..."
    
    # Create large file
    dd if=/dev/zero of=cache_test.dat bs=1M count=100 2>/dev/null
    
    # Drop caches if we have permission
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || echo "Cannot drop caches (need root)"
    
    # Read file to trigger cache misses
    cat cache_test.dat > /dev/null
    
    # Random reads to cause cache misses
    for i in $(seq 1 20); do
        dd if=cache_test.dat of=/dev/null bs=4K count=1 skip=$((RANDOM % 1000)) 2>/dev/null
    done
}

# Function to generate various I/O patterns using fio
generate_fio_patterns() {
    echo "Generating fio I/O patterns..."
    
    # Sequential write
    fio --name=seq_write --ioengine=sync --rw=write \
        --bs=4k --size=10M --numjobs=1 --runtime=${LOAD_DURATION} \
        --time_based --filename=fio_seq.dat 2>/dev/null || true
    
    # Random read
    fio --name=rand_read --ioengine=sync --rw=randread \
        --bs=4k --size=10M --numjobs=${LOAD_CONCURRENCY} --runtime=${LOAD_DURATION} \
        --time_based --filename=fio_rand.dat 2>/dev/null || true
    
    # Mixed workload
    fio --name=mixed --ioengine=sync --rw=randrw --rwmixread=70 \
        --bs=4k --size=10M --numjobs=4 --runtime=${LOAD_DURATION} \
        --time_based --filename=fio_mixed.dat 2>/dev/null || true
}

# Function to generate memory pressure
generate_memory_pressure() {
    echo "Generating memory pressure..."
    
    # Use stress-ng for memory operations
    stress-ng --vm 2 --vm-bytes 256M --timeout ${LOAD_DURATION}s 2>/dev/null || true
    
    # Allocate and free memory in a loop
    for i in $(seq 1 10); do
        dd if=/dev/zero of=mem_test_$i.dat bs=1M count=50 2>/dev/null
        rm -f mem_test_$i.dat
        sleep 1
    done
}

# Function to generate concurrent file operations
generate_concurrent_ops() {
    echo "Generating concurrent file operations..."
    
    for i in $(seq 1 ${LOAD_CONCURRENCY}); do
        (
            # Each concurrent task
            for j in $(seq 1 10); do
                # Create file
                echo "Task $i iteration $j" > concurrent_$i_$j.txt
                
                # Read file
                cat concurrent_$i_$j.txt > /dev/null
                
                # Append to file
                echo "Append $j" >> concurrent_$i_$j.txt
                
                # Delete file
                rm -f concurrent_$i_$j.txt
                
                sleep 0.1
            done
        ) &
    done
    
    # Wait for all background jobs
    wait
}

# Main execution
echo "Starting load generation at $(date)"

# Run all test patterns
generate_fadvise_patterns
generate_cache_patterns
generate_fio_patterns
generate_memory_pressure
generate_concurrent_ops

# Cleanup
cd /
rm -rf $TEST_DIR

echo "=== Load Generation Complete ==="
echo "Check /output/metrics.json for captured eBPF metrics"