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

# A data node resumes after it is restarted (TC5 on a data node).
#
# data1 is stopped and started again on its own data. Once the cluster serves
# again, a new marker must be stored under data1's own identity, and on data1:
# the restart must not cost the node its collection, nor move its logs.
#
#   check-restart.sh <liaison-http-host:port>

set -euo pipefail
http_addr=${1:?usage: check-restart.sh <liaison-http-host:port>}
api="http://${http_addr}/api/v1"
deadline=$((SECONDS + 300))
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

node=data1:17912
container=$(container_of data1)
wait_liaison
docker restart "$container" >/dev/null
until [[ "$(docker inspect -f '{{.State.Health.Status}}' "$container")" == "healthy" ]]; do
	if [[ $SECONDS -ge $deadline ]]; then
		echo "data1 did not become healthy after the restart" >&2
		exit 1
	fi
	sleep 2
done

since=$(now_utc -5)
module=""
stored=false
while [[ $SECONDS -lt $deadline ]]; do
	# The group is created once the cluster takes it: right after the restart
	# the liaison may still count data1 as gone.
	[[ -z "$module" ]] && module=$(create_marker_group)
	found=$(query_log "$(node_eq "$node")" false "$since" | rows |
		jq -s --arg m "$module" 'if any(.[]; .error) then "error" else (any(.[]; .module == $m) | tostring) end')
	if [[ "$found" == '"true"' ]]; then
		stored=true
		break
	fi
	echo "data1's marker after the restart: ${found}" >&2
	sleep 5
done

echo "restarted: ${node}"
echo "markerStoredAfterRestart: ${stored}"
