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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations
# under the License.

# Test script for death rattle detection
# This script demonstrates how to trigger death rattle detection

set -e

DEATH_RATTLE_FILE="/tmp/death-rattle"
MESSAGE="Test death rattle triggered at $(date)"

echo "Creating death rattle file: $DEATH_RATTLE_FILE"
echo "$MESSAGE" > "$DEATH_RATTLE_FILE"

echo "Death rattle file created. FODC should detect this."
echo "To test, run FODC in another terminal:"
echo "  ./build/bin/dev/fodc-cli --death-rattle-path=$DEATH_RATTLE_FILE"

# Wait a bit, then remove the file
sleep 5
echo "Removing death rattle file..."
rm -f "$DEATH_RATTLE_FILE"

