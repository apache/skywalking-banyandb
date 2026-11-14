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

# Waits until trace-mocker has finished writing and the hot node has flushed trace and
# measure segments to disk, so the one-shot lifecycle run has something to migrate.
# trace-mocker only produces traces; OAP derives metrics from them and may also write records
# derived from them (older, still on the hot node) into the stream group sw_records. Those are
# not guaranteed, so report-events.sh adds current stream rows.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"

mocker="$(container_of trace-mocker)"
# mocker_done succeeds once trace-mocker exited 0 and aborts the script if it exited otherwise.
mocker_done() {
  state="$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$mocker")"
  case "$state" in
    "exited 0") echo "trace-mocker finished" ;;
    exited*) echo "trace-mocker failed: $state" >&2; docker logs "$mocker" 2>&1 | tail -50 >&2; exit 1 ;;
    *) return 1 ;;
  esac
}
# trace-mocker writes a fixed batch and exits (about 160s in CI); give it six minutes. A mocker
# that is still running afterwards would keep writing while the lifecycle run snapshots, so that
# is a failure rather than something to carry on with.
if ! poll 36 10 mocker_done; then
  echo "trace-mocker still running after 6 minutes: $state" >&2
  docker logs "$mocker" 2>&1 | tail -50 >&2
  exit 1
fi

hot="$(container_of data-hot1)"
# Both globs must match: busybox ls exits non-zero when any argument has no match.
# The measure glob names sw_metricsMinute: sw_metadata alone would match any measure group
# long before OAP has derived metrics from the traces.
if ! poll 60 5 docker exec "$hot" sh -c 'ls -d /tmp/trace/data/*/seg-* /tmp/measure/data/sw_metricsMinute/seg-* >/dev/null 2>&1'; then
  echo "hot node never produced trace/minute-metrics segments" >&2
  exit 1
fi
echo "hot node has trace and minute-metrics segments"
list_segments "$hot"
# Let the last writes flush (default flush timeouts: measure 5s, trace 1s) before snapshotting.
sleep 10
