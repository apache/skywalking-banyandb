#!/bin/bash


# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# List of service IDs
# service_0 to service_4
service_ids=("ImRlZmF1bHQiOjpzZXJ2aWNlXzQ=.1" "ImRlZmF1bHQiOjpzZXJ2aWNlXzM=.1" "ImRlZmF1bHQiOjpzZXJ2aWNlXzI=.1" "ImRlZmF1bHQiOjpzZXJ2aWNlXzE=.1" "ImRlZmF1bHQiOjpzZXJ2aWNlXzA=.1")

error_num=0
# Iterate over each service ID
for service_id in "${service_ids[@]}"; do
  echo "Checking service ID: $service_id"

  # Run the swctl command
  swctl_result=$(swctl --display json metrics linear --name=service_resp_time --service-id="$service_id")

  # Check if swctl result is not an empty list
  if [ "$swctl_result" == "[]" ]; then
    echo "The swctl result is an empty list for service ID: $service_id. Skipping jq processing."
    error_num=$((error_num + 1))
  else
    # Filter with jq
    result=$(echo "$swctl_result" | jq 'map(select(.IsEmptyValue == true))')

    # Check if the result is an empty list
    if [ "$result" != "[]" ]; then
      echo "The result is not an empty list. Some items have IsEmptyValue set to true for service ID: $service_id."
      error_num=$((error_num + 1))
    fi
  fi

  trace_result=$(swctl t ls --service-id="$service_id" | jq -e '.traces | length > 0')

  if [ "$trace_result" == "false" ]; then
    echo "No traces found for service ID: $service_id."
    error_num=$((error_num + 1))
  fi

  echo
done

if [ "$error_num" -gt 0 ]; then
  echo "Some service IDs failed the check."
  exit 1
fi
echo "All service IDs passed the check."
