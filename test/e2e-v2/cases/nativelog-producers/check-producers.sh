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

# The producers' identities, events and placement, and a producer at the
# default.
#
# backup0 and lifecycle0 store their own lines under their own node_id and
# node_type, each on the data node it sits beside and on no other. backup1 runs
# with native logging off: it logs the same kind of line to its console, and
# stores nothing. lifecycle0 reports its counters on its own /metrics.
#
#   check-producers.sh <liaison-http-host:port> <lifecycle-http-host:port>

set -euo pipefail
http_addr=${1:?usage: check-producers.sh <liaison-http-host:port> <lifecycle-http-host:port>}
lifecycle_addr=${2:?usage: check-producers.sh <liaison-http-host:port> <lifecycle-http-host:port>}
api="http://${http_addr}/api/v1"
deadline=$((SECONDS + 300))
# shellcheck source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

wait_liaison
since=$(now_utc -1800)

# Polled until both events are in.
while :; do
	backup_seen=$(has_event "$backup" "$backup_module" "$backup_event" "$since")
	lifecycle_seen=$(has_event "$lifecycle" "$lifecycle_module" "$lifecycle_event" "$since")
	[[ "$backup_seen" == true && "$lifecycle_seen" == true ]] && break
	echo "backup event: ${backup_seen}; lifecycle event: ${lifecycle_seen}" >&2
	[[ $SECONDS -ge $deadline ]] && break
	sleep 5
done

# Every stored row is a producer's: no BanyanDB node here collects its own.
identities=$(query_log "" false "$since" | rows | jq -sc 'if any(.[]; .error) then [first(.[] | select(.error)) | "error: " + .error] else [.[] | "\(.node_id):\(.node_type)"] | unique end')

# backup1 logged its line: without that, its absence from storage proves
# nothing. All output is read, so an early grep exit cannot break docker logs
# under pipefail.
disabled_logged=false
if docker logs "$(container_of backup1)" 2>&1 | grep -F "backups-off will run" >/dev/null; then
	disabled_logged=true
fi
disabled_rows=$(query_log "$(node_eq "$disabled")" false "$since" | rows | jq -s 'if any(.[]; .error) then "query failed" else length end')

# Polled too: the counters are published once a second, so the first rows can
# be queryable before the counter that counts them.
while :; do
	lifecycle_written=$(curl -s -m 10 "http://${lifecycle_addr}/metrics" | awk '$1 == "banyandb_logging_native_log_written_total" { print ($2 > 0) ? "true" : "false" }')
	[[ "$lifecycle_written" == true ]] && break
	echo "lifecycle written: ${lifecycle_written:-missing}" >&2
	[[ $SECONDS -ge $deadline ]] && break
	sleep 2
done

echo "producers:"
echo "$identities" | jq -r '.[] | "  - " + .'
echo "backupEvent: ${backup_seen}"
echo "lifecycleEvent: ${lifecycle_seen}"
echo "backupStoredOn: $(stored_on "$backup" "$since")"
echo "lifecycleStoredOn: $(stored_on "$lifecycle" "$since")"
echo "disabledLogged: ${disabled_logged}"
echo "disabledRows: ${disabled_rows}"
echo "lifecycleWritten: ${lifecycle_written:-missing}"
