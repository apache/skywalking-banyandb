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

# The producers' identities and events (TC2), and a data node at the default
# (TC3b).
#
# A group created through the liaison makes every data node, and the liaison,
# log one `initialized` line under the group's module. The rows of that event
# must come from exactly the five producing data nodes and the liaison, one
# each, under its own node_id and node_type. data5 runs with native logging
# off: it logs the same line to its console, and stores nothing.
#
#   check-identities.sh <liaison-http-host:port>

set -euo pipefail
http_addr=${1:?usage: check-identities.sh <liaison-http-host:port>}
api="http://${http_addr}/api/v1"
deadline=$((SECONDS + 300))
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

wait_liaison
# The rows are read from this instant on, so the scan holds the marker lines
# and little else, far below the query limit.
since=$(now_utc -5)
module=$(create_marker_group)
want=$(printf '%s\n' "${producers[@]}" "$liaison" | sort | paste -sd, -)

# Polled until every producer's line is in, and then read once more, so a sixth
# identity arriving late is still seen.
while :; do
	marked=$(query_log "" false "$since" | rows | jq -sc --arg m "$module" '[.[] | select(.error or (.module == $m and .message == "initialized"))]')
	if echo "$marked" | jq -e 'any(.[]; .error)' >/dev/null; then
		echo "query failed: $(echo "$marked" | jq -c 'first(.[] | select(.error))')" >&2
	else
		got=$(echo "$marked" | jq -r '[.[].node_id] | unique | join(",")')
		[[ "$got" == "$want" ]] && break
		echo "stored producers so far: ${got:-none}" >&2
	fi
	if [[ $SECONDS -ge $deadline ]]; then
		break
	fi
	sleep 5
done
sleep 5
marked=$(query_log "" false "$since" | rows | jq -sc --arg m "$module" '[.[] | select(.error or (.module == $m and .message == "initialized"))]')
if echo "$marked" | jq -e 'any(.[]; .error)' >/dev/null; then
	echo "query failed: $(echo "$marked" | jq -c 'first(.[] | select(.error))')" >&2
	exit 1
fi

# data5 printed the line: without that, its absence from storage proves nothing.
# Read all output so an early grep exit cannot break docker logs under pipefail.
disabled_logged=false
if docker logs "$(container_of data5)" 2>&1 | grep -F "\"module\":\"${module}\"" >/dev/null; then
	disabled_logged=true
fi
disabled_rows=$(query_log "$(node_eq "$disabled")" | rows | jq -s 'if any(.[]; .error) then "query failed" else length end')

echo "producers:"
echo "$marked" | jq -r '[.[].node_id] | unique | .[] | "  - " + .'
echo "nodeTypes: $(echo "$marked" | jq -r '[.[].node_type] | unique | join(",")')"
echo "initializedPerProducer: $(echo "$marked" | jq -r 'group_by(.node_id) | map(length) | unique | join(",")')"
echo "disabledNodeLogged: ${disabled_logged}"
echo "disabledNodeRows: ${disabled_rows}"
