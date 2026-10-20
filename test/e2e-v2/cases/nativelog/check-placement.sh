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

# span_counts prints {node: rows} for every data node span in a traced response.
span_counts() {
	jq -c '[.. | objects
	  | select((.message? // "") | type == "string" and startswith("data-"))
	  | {(.message | ltrimstr("data-")): ([.tags[]? | select(.key == "resp_count") | .value | tonumber] | add // 0)}]
	  | add // {}'
}

# placement prints "local" when the producer's marker is among its rows and
# only the producer returned any of them, and what was seen otherwise.
placement() {
	local node=$1 response marker counts
	response=$(query_log "$(node_eq "$node")" true "$since")
	marker=$(echo "$response" | rows | jq -s --arg m "$module" 'any(.[]; .module == $m)')
	counts=$(echo "$response" | span_counts)
	echo "$counts" | jq -r --arg n "$node" --argjson marker "$marker" '
	  (to_entries | map(select(.key != $n and .value > 0))) as $elsewhere
	  | if (.[$n] // 0) > 0 and ($elsewhere | length) == 0 and $marker then "local"
	    else "own=\(.[$n] // 0) marker=\($marker) elsewhere=\($elsewhere | map("\(.key):\(.value)") | join(";")) spans=\(keys | join(";"))"
	    end'
}

for node in "${producers[@]}"; do
	while :; do
		verdict=$(placement "$node")
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
