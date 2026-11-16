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

# Runs a real export in FORMAT (csv or native) of one catalog (or a --schema-only export of all
# four) into a fresh directory on the host, validates the artifact with the export checker built in
# setup, then checks that the data nodes hold no export snapshot any more (the session was
# released). A CSV catalog export is checked row by row (manifest, per-file bytes/crc32, the header
# of every file against the exported schema, every row and cell, rows per resource against the
# manifest and against the rows the snapshot stored, every measure's rows against a count query
# through the liaison, and the rows of trace-mocker, OAP and report-events.sh printed for the
# expectations); a native one container by container (checker native: every file's bytes and
# crc32, every container's volumes verifying as one set, every shard container holding whole
# parts, the segment metadata of every segment unit). A --schema-only export is checked the same
# way in both formats. The output is one yaml document for expected/export-<catalog>.yaml.
# Every attempt exports into a new directory, and an export that fails after creating its
# session has that session released here, so the verify retry can re-run the script. The
# artifact is removed on success and kept below $SW_INFRA_E2E_LOG_DIR on failure.
#
#   FORMAT=<csv|native> export.sh <liaison host:grpc-port> <stream|measure|trace|property|schema-only>
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"
require_format

usage="usage: FORMAT=<csv|native> export.sh <liaison host:grpc-port> <stream|measure|trace|property|schema-only>"
liaison="${1:?$usage}"
mode="${2:?$usage}"
hot="$(container_of data-hot1)"
warm="$(container_of data-warm1)"
# The e2e log directory may not exist yet while verify cases run.
log_root="${SW_INFRA_E2E_LOG_DIR:-/tmp}"
mkdir -p "$log_root"
work="$(mktemp -d "$log_root/$FORMAT-export-$mode.XXXXXX")"
ok=false
on_exit() {
  if [ "$ok" = true ]; then
    rm -rf "$work"
  else
    echo "$FORMAT export check failed; artifact and logs kept in $work" >&2
  fi
}
trap on_exit EXIT

if [ "$mode" = schema-only ]; then
  scope=("${ALL_CATALOGS[@]}" --schema-only)
else
  scope=(--selector "catalog=$mode")
fi
scope+=(--format "$FORMAT")

out="$work/artifact"
rc=0
bydbctl data export --nodes "$liaison" --output "$out" "${scope[@]}" >"$work/export.stdout" 2>"$work/export.log" || rc=$?
echo "exitCode: $rc"
check_rc=0
if [ "$rc" -ne 0 ]; then
  echo "bydbctl data export ${scope[*]} exited $rc:" >&2
  tail -50 "$work/export.stdout" "$work/export.log" >&2
else
  # The checker exits non-zero when a check failed; its yaml still goes to stdout.
  if [ "$FORMAT" = native ] && [ "$mode" != schema-only ]; then
    export-checker native "$out" 2>"$work/check.log" || check_rc=$?
  else
    export-checker "$mode" "$out" "$liaison" 2>"$work/check.log" || check_rc=$?
  fi
  cat "$work/check.log" >&2
fi

leftovers="$(leftover_snapshots "$hot" "$warm")"
echo "leftoverSnapshots: [$leftovers]"
# Every check of every group counts, including the groups the expected file does not list.
echo "checkerExitCode: $check_rc"

# A kept session (after a failure, or a release that failed after a successful export) is
# named in release-session hints: release it, after the leftovers above were reported, so the
# next attempt is not refused as a second concurrent export.
for id in $(grep -oE 'release-session --id [0-9a-f]{32}' "$work/export.log" | awk '{print $3}' | sort -u); do
  bydbctl data release-session --id "$id" --nodes "$liaison" >&2 || true
done

# Keep the artifact for the e2e log unless every check passed.
if [ "$rc" -eq 0 ] && [ "$check_rc" -eq 0 ] && [ -z "$leftovers" ]; then
  ok=true
fi
