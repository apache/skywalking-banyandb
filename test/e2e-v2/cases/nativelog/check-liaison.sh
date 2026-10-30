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
# The liaison route (phase 2).
#
# The liaison has no storage, so it sends its logs to the data node that owns
# shard 0 of the single-shard log group: all of them land on that one node.
# Placement is read from the query trace, as in check-placement.sh.
#
# That data node is then paused. It stops answering without closing its
# connections, so the liaison's client removes it only when its health check
# fails, and the liaison must follow it to another data node. A new marker
# group makes the liaison log `initialized`, which must be stored there.
#
#   check-liaison.sh <liaison-http-host:port>

set -euo pipefail
http_addr=${1:?usage: check-liaison.sh <liaison-http-host:port>}
api="http://${http_addr}/api/v1"
deadline=$((SECONDS + 300))
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# stored_on prints the data nodes that returned the liaison's rows since $1,
# and whether the marker module $2 is among them; it prints an error line when
# the query failed.
stored_on() {
	local response
	response=$(query_log "$(node_eq "$liaison")" true "$1")
	if ! echo "$response" | jq -e '.elements' >/dev/null 2>&1; then
		echo "error: $(echo "$response" | head -c 300)"
		return
	fi
	echo "nodes=$(echo "$response" | span_counts | jq -r 'to_entries | map(select(.value > 0) | .key) | sort | join(",")') marker=$(echo "$response" | rows | jq -s --arg m "$2" 'any(.[]; .module == $m)')"
}

# await_marker polls until the marker is stored, and prints the last answer.
await_marker() {
	local seen
	while :; do
		seen=$(stored_on "$1" "$2")
		[[ "$seen" == *"marker=true" ]] && break
		echo "$seen" >&2
		[[ $SECONDS -ge $deadline ]] && break
		sleep 5
	done
	echo "$seen"
}

wait_liaison
since=$(now_utc -5)
module=$(create_marker_group)
before=$(await_marker "$since" "$module")
owner=$(echo "$before" | sed -n 's/^nodes=\([^ ]*\) .*/\1/p')

paused=""
if [[ -n "$owner" && "$owner" != *,* ]]; then
	paused=$(container_of "${owner%%:*}")
	docker pause "$paused" >/dev/null
	trap 'docker unpause "$paused" >/dev/null 2>&1 || true' EXIT
fi
since=$(now_utc -1)
module=$(create_marker_group)
after=$(await_marker "$since" "$module")
next=$(echo "$after" | sed -n 's/^nodes=\([^ ]*\) .*/\1/p')

echo "liaisonRowsOnOneDataNode: $([[ -n "$owner" && "$owner" != *,* ]] && echo true || echo "\"${before}\"")"
echo "markerStored: $([[ "$before" == *"marker=true" ]] && echo true || echo false)"
echo "afterPause:"
echo "  ownerPaused: $([[ -n "$paused" ]] && echo true || echo false)"
echo "  storedOnAnotherDataNode: $([[ -n "$next" && "$next" != *,* && "$next" != "$owner" ]] && echo true || echo "\"${after}\"")"
echo "  markerStored: $([[ "$after" == *"marker=true" ]] && echo true || echo false)"
