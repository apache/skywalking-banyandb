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

# Stops the warm data node and checks that the dry run degrades instead of failing, then
# restarts it and checks that coverage comes back. File-based discovery keeps the stopped node
# registered (it only moves to the connection pool's evict queue), so the degraded report still
# lists it in dataNodes while answeredNodes shrinks to the hot node and missingNodes /
# unreachableNodes name the warm node. --strict-coverage exits 2 with a "coverage gap" while a
# node is missing and 0 once every node answered, which is what the polling waits for. The output is
# one yaml document for expected/degraded.yaml: both raw reports nested under their phase, the
# strict exit codes, the number of warm-node rows in the degraded report (0: a stopped node
# contributes nothing), and the export session snapshots left behind (none: the dry run is
# read-only).
# Every exit path restarts the warm node, so the script is safe to re-run.
#
#   degraded.sh <liaison host:grpc-port>
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"

liaison="${1:?usage: degraded.sh <liaison host:grpc-port>}"
warm="$(container_of data-warm1)"
hot="$(container_of data-hot1)"
on_exit() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    docker logs --tail 200 "$warm" >&2 2>&1 || true
  fi
  rm -f "$strict_stderr"
  docker start "$warm" >/dev/null 2>&1 || true
}
trap on_exit EXIT

# strict_exits succeeds when a --strict-coverage dry run exits with code $2 (phase $1 is
# only for the log); a non-zero code must come from the coverage check itself, which bydbctl
# reports as "coverage gap" on stderr, and not from some other preflight failure that happens
# to exit 2 as well. The measured code is kept in strict_rc so the report echoes what was
# observed rather than the literal the poll waited for. The liaison evicts an unreachable node
# and re-admits a restarted one within seconds; the callers poll 12 x 5s and the verify retry
# re-runs the whole script when that is not enough, which keeps the worst case inside the job timeout.
# Each attempt overwrites strict_stderr, so a poll that gives up can show the last attempt's stderr.
strict_rc=
strict_stderr="$(mktemp)"
strict_exits() {
  local rc=0
  bydbctl data export --dry-run --nodes "$liaison" --strict-coverage -o yaml >/dev/null 2>"$strict_stderr" || rc=$?
  echo "$1: --strict-coverage exit $rc" >&2
  strict_rc="$rc"
  if [ "$rc" != "$2" ]; then
    return 1
  fi
  if [ "$rc" -ne 0 ] && ! grep -q "coverage gap" "$strict_stderr"; then
    echo "$1: exit $rc without a coverage gap on stderr:" >&2
    cat "$strict_stderr" >&2
    return 1
  fi
}

# poll_strict polls strict_exits and, when it gives up, prints the stderr of the last attempt.
poll_strict() {
  if ! poll 12 5 strict_exits "$1" "$2"; then
    echo "$1: --strict-coverage never exited $2; bydbctl stderr of the last attempt:" >&2
    cat "$strict_stderr" >&2
    return 1
  fi
}

# report prints the plain dry-run report indented by two spaces so it nests under a key. The
# plain run must succeed even while a node is missing: that is the degradation under test.
report() {
  bydbctl data export --dry-run --nodes "$liaison" -o yaml | sed 's/^/  /'
}

docker stop "$warm" >/dev/null
poll_strict "warm stopped" 2
stopped="$(report)"
echo "stopped:"
echo "$stopped"
echo "strictCoverageExitWhileStopped: $strict_rc"
# contains in the expected file cannot assert that a row is absent, so count the stopped node's rows.
echo "stoppedWarmRows: $(grep -c '^[ -]*node: data-warm1:17912$' <<<"$stopped" || true)"

docker start "$warm" >/dev/null
poll_strict "warm restarted" 0
echo "recovered:"
report
echo "strictCoverageExitAfterRestart: $strict_rc"
# The table rendering has no yaml to compare; it only has to succeed.
bydbctl data export --dry-run --nodes "$liaison" -o table >/dev/null

# The inner `true` swallows ls's no-match exit, so a non-zero status means docker exec itself failed.
leftovers=
for c in "$hot" "$warm"; do
  if ! found="$(docker exec "$c" sh -c 'ls -d /tmp/*/export-snapshots/* 2>/dev/null; true')"; then
    echo "listing export snapshots in container $c failed" >&2
    exit 1
  fi
  while IFS= read -r line; do
    if [ -n "$line" ]; then
      leftovers="${leftovers:+$leftovers, }$line"
    fi
  done <<<"$found"
done
echo "leftoverSnapshots: [$leftovers]"
