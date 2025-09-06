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

echo "=== Basic I/O Test ==="

TEST_DIR="/data/basic_io_test"
mkdir -p $TEST_DIR
cd $TEST_DIR

# Test 1: Simple file operations
echo "Test 1: Simple file operations"
for i in {1..100}; do
    echo "Test data $i" > file_$i.txt
    cat file_$i.txt > /dev/null
    rm file_$i.txt
done

# Test 2: Sequential reads
echo "Test 2: Sequential reads"
dd if=/dev/urandom of=sequential.dat bs=1M count=10 2>/dev/null
for i in {1..10}; do
    dd if=sequential.dat of=/dev/null bs=1M 2>/dev/null
done

# Test 3: Multiple file descriptors
echo "Test 3: Multiple file descriptors"
for i in {1..10}; do
    exec {fd}<>multi_fd_$i.txt
    echo "FD test $i" >&$fd
    exec {fd}>&-
done

# Cleanup
cd /
rm -rf $TEST_DIR

echo "Basic I/O test completed"