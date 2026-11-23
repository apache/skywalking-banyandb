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

# A producer's destination goes away and comes back.
#
# data0, the node backup0 writes to, is paused. It stops answering without
# closing its connection, so backup0's writes time out, and the events in them
# are lost. backup0 has no metrics endpoint: it reports each loss on its
# stderr. Once data0 resumes, backup0, which keeps logging every 5 seconds,
# stores new lines without being restarted.
#
#   check-recovery.sh <liaison-http-host:port>

set -euo pipefail
http_addr=${1:?usage: check-recovery.sh <liaison-http-host:port>}
api="http://${http_addr}/api/v1"
deadline=$((SECONDS + 300))
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

wait_liaison
producer=$(container_of backup0)
destination=$(container_of data0)
started=$(docker inspect -f '{{.State.StartedAt}}' "$producer")

docker pause "$destination" >/dev/null
# Longer than the 5s write timeout, and than several backup runs.
sleep 30
docker unpause "$destination" >/dev/null
resumed=$(now_utc)

# All output is read, so an early grep exit cannot break docker logs under
# pipefail.
loss_reported=false
if docker logs "$producer" 2>&1 | grep -F "native log events dropped as" >/dev/null; then
	loss_reported=true
fi

stored=false
while :; do
	count=$(query_log "$(node_eq "$backup")" false "$resumed" | rows | jq -s 'if any(.[]; .error) then -1 else length end')
	if [[ "$count" -gt 0 ]]; then
		stored=true
		break
	fi
	echo "rows of ${backup} since ${resumed}: ${count}" >&2
	[[ $SECONDS -ge $deadline ]] && break
	sleep 5
done

restarted=true
if [[ "$(docker inspect -f '{{.State.StartedAt}}' "$producer")" == "$started" ]]; then
	restarted=false
fi

echo "lossReported: ${loss_reported}"
echo "storedAfterResume: ${stored}"
echo "producerRestarted: ${restarted}"
