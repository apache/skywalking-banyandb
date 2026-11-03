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

# A data node whose storage refuses its writes (TC5 on a data node).
#
# data6 collects its own logs, and its stream storage is read-only. A marker
# group makes it log one more line. That line MUST be refused and counted as
# dropped with reason publish_failed, never as written, and the node MUST
# store no row at all.
#
#   check-refusal.sh <liaison-http-host:port>

set -euo pipefail
http_addr=${1:?usage: check-refusal.sh <liaison-http-host:port>}
api="http://${http_addr}/api/v1"
deadline=$((SECONDS + 300))
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

node=${REFUSAL_NODE:-$refusing}
container=$(container_of "${node%%:*}")

# native_metric prints the value of one native log metric on the node, or
# "absent" when the node does not publish it.
native_metric() {
	docker exec "$container" wget -qO- http://127.0.0.1:2121/metrics |
		awk -v name="$1" '$1 == name { print $2; found = 1 } END { if (!found) print "absent" }'
}
dropped_name='banyandb_logging_native_log_dropped_total{reason="publish_failed"}'
written_name='banyandb_logging_native_log_written_total'

wait_liaison
before=$(native_metric "$dropped_name")
[[ "$before" == "absent" ]] && before=0
module=$(create_marker_group)

# The marker line exists: without it, a count that does not rise proves nothing.
logged=false
rose=false
while [[ $SECONDS -lt $deadline ]]; do
	if [[ "$logged" == false ]] && docker logs "$container" 2>&1 | grep -q "\"module\":\"${module}\""; then
		logged=true
	fi
	now=$(native_metric "$dropped_name")
	if [[ "$logged" == true && "$now" != "absent" ]] && awk -v a="$now" -v b="$before" 'BEGIN { exit !(a > b) }'; then
		rose=true
		break
	fi
	echo "${node}: marker logged=${logged}, publish_failed ${before} -> ${now}" >&2
	sleep 5
done

rows=$(query_log "$(node_eq "$node")" | rows | jq -s 'if any(.[]; .error) then "query failed" else length end')

echo "refusingNode: ${node}"
echo "markerLogged: ${logged}"
echo "publishFailedRose: ${rose}"
echo "writtenTotal: $(native_metric "$written_name")"
echo "storedRows: ${rows}"
