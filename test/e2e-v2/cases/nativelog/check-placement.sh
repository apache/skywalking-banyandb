#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Local placement in shard 0 with more producers than shards.
#
# The log group has one shard and there are five producers. Each must keep its
# own lines on itself, in shard 0, and never send them to another node. A
# query through the liaison would also succeed if a producer forwarded its
# lines elsewhere, so placement is read from the query trace: the liaison asks
# every data node, and the trace has one span per node, data-<node_id>, with
# the number of rows that node returned. For each producer's rows, only the
# producer itself may return any. The group has no replicas, so a row on a
# node is a row that node stored.
#
#   check-placement.sh <liaison-http-host:port>

set -euo pipefail
http_addr=${1:?usage: check-placement.sh <liaison-http-host:port>}
api="http://${http_addr}/api/v1"
deadline=$((SECONDS + 300))
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

wait_liaison
since=$(now_utc -5)
module=$(create_marker_group)

for node in "${producers[@]}"; do
	while :; do
		verdict=$(placement "$node" "$module" "$since")
		[[ "$verdict" == "local" ]] && break
		echo "${node}: ${verdict}" >&2
		[[ $SECONDS -ge $deadline ]] && break
		sleep 5
	done
	if [[ "$verdict" == "local" ]]; then
		verdicts+=("${node}: local")
	else
		verdicts+=("${node}: \"${verdict}\"")
	fi
done

echo "shardNum: $(curl -s -m 10 "${api}/group/schema/_monitoring_log" | jq -r '.group.resourceOpts.shardNum')"
echo "placement:"
printf '  %s\n' "${verdicts[@]}"
