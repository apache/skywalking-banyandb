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

# Helpers of the producer cases. The query helpers are those of the native log
# case; the names below are this case's.

# shellcheck source=../nativelog/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/../nativelog/common.sh"

# Each producer's node_id is its POD_NAME; a data node's is its host and port.
backup=backup0
backup_host=data0:17912
lifecycle=lifecycle0
lifecycle_host=data1:17912
# disabled is a backup at the default, native logging off.
disabled=backup1

# The events each producer logs once it runs, and that the case looks for.
backup_module=BACKUP-SCHEDULER
backup_event="backup to file:///tmp/backups-e2e will run with schedule: @every 5s"
lifecycle_module=LIFECYCLE.SCHEDULER.LIFECYCLE
lifecycle_event=start

# has_event prints true when node $1 stored message $3 under module $2 since
# $4; it prints an error line when the query failed.
has_event() {
	query_log "$(node_eq "$1")" false "$4" | rows |
		jq -rs --arg m "$2" --arg e "$3" 'if any(.[]; .error) then "error: \(first(.[] | select(.error)).error)" else any(.[]; .module == $m and .message == $e) end'
}

# stored_on prints the data nodes that returned node $1's rows since $2, from
# the query trace; it prints an error line when the query failed. The group has
# no replicas, so a row on a node is a row that node stored.
stored_on() {
	local response
	response=$(query_log "$(node_eq "$1")" true "$2")
	if ! echo "$response" | jq -e '.elements' >/dev/null 2>&1; then
		echo "error: $(echo "$response" | head -c 300)"
		return
	fi
	echo "$response" | span_counts | jq -r 'to_entries | map(select(.value > 0) | .key) | sort | join(",")'
}
