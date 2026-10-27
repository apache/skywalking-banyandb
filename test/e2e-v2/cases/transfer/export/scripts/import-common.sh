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


# Shared state and helpers of the import cases (import-noschema.sh, import-conflict.sh,
# import-dryrun.sh, import-roundtrip.sh). Source after common.sh, do not execute.
#
# The cases run in order against one destination cluster and must never import twice (stream
# and trace rows are not idempotent), while infra-e2e retries any failing case. So every step
# that changes the destination or freezes an input leaves a marker in IMPORT_WORK, a fixed
# directory the setup step empties once per run:
#   A.done          artifact A (the full export of the source) is complete and never re-exported;
#   noschema.done   the schema-less case passed; noschema.out holds its report, replayed on retry;
#   conflict.done   the conflict case passed; conflict.out holds its report, replayed on retry;
#   import.done     the real import exited 0; dryrun.out holds the dry-run case's report;
#   import.failed   the destination may hold a partial import: every later attempt prints this
#                   file (a report no expectation matches) instead of importing again.
# IMPORT_WORK lives under the e2e log directory so the artifacts and logs are uploaded with it.

IMPORT_WORK="${SW_INFRA_E2E_LOG_DIR:-/tmp}/transfer-import"
ARTIFACT_A="$IMPORT_WORK/A"
# The destination liaison's host ports, pinned in docker-compose.yml and plan-dst.yaml.
DST_GRPC=127.0.0.1:27912
DST_HTTP=http://127.0.0.1:27913
ALL_CATALOGS=(--selector catalog=stream --selector catalog=measure --selector catalog=trace --selector catalog=property)

# table_section prints the import decision table of a bydbctl data import output: everything up
# to and including the line starting with "RESULT" (or to the end of the output). The dry run
# prints only this; the real import prints the same bytes first and its progress and verify
# output afterwards. Adjust the awk end condition here if the rendering changes.
table_section() {
  awk '
    { print }
    /^RESULT/ { exit }
  ' "$1"
}

# release_hinted_sessions releases the export sessions an export named in its release-session
# hints (log file $1, liaison $2): after a failure, or after a successful export whose release
# failed. Then a retry is not refused as a second export.
release_hinted_sessions() {
  local id
  for id in $(grep -oE 'release-session --id [0-9a-f]{32}' "$1" | awk '{print $3}' | sort -u); do
    bydbctl data release-session --id "$id" --nodes "$2" >&2 || true
  done
}

# export_all exports every catalog through liaison $1 into the new directory $2, logging next
# to it. It always releases the sessions the export hinted at, and on failure it prints the log
# tail and returns 1.
export_all() {
  local rc=0
  rm -rf "$2"
  bydbctl data export --nodes "$1" --output "$2" "${ALL_CATALOGS[@]}" >"$2.stdout" 2>"$2.log" || rc=$?
  release_hinted_sessions "$2.log" "$1"
  if [ "$rc" -ne 0 ]; then
    echo "export through $1 into $2 exited $rc:" >&2
    tail -50 "$2.stdout" "$2.log" >&2
    return 1
  fi
}

# freeze_artifact_a exports the source cluster (liaison $1) into ARTIFACT_A once per run.
freeze_artifact_a() {
  if [ -f "$IMPORT_WORK/A.done" ]; then
    return 0
  fi
  mkdir -p "$IMPORT_WORK"
  export_all "$1" "$ARTIFACT_A"
  touch "$IMPORT_WORK/A.done"
}

# dst_schema_digest prints a checksum of the destination's whole schema: a --schema-only export
# into the new directory $1, its schema lines sorted (the order resources are listed in does not
# matter). Equal digests mean that no resource was created, updated or deleted in between.
dst_schema_digest() {
  rm -rf "$1"
  if ! bydbctl data export --nodes "$DST_GRPC" --output "$1" --schema-only "${ALL_CATALOGS[@]}" >"$1.log" 2>&1; then
    echo "schema-only export of the destination failed:" >&2
    tail -50 "$1.log" >&2
    return 1
  fi
  cat "$1"/schema/*.jsonl | LC_ALL=C sort | cksum
}

# dst_data_rows prints the rows the destination holds according to an export dry run, leaving
# out _deletion_task, the internal property group every cluster has. The yaml rendering sorts the
# keys of a row (estRows comes before group), so the awk does not rely on key order: it keeps
# each list item's group and estRows and adds the count when the item ends, at the next item,
# the next top-level key or the end of the output.
dst_data_rows() {
  bydbctl data export --dry-run --nodes "$DST_GRPC" "${ALL_CATALOGS[@]}" -o yaml | sum_est_rows
}

# sum_est_rows sums the estRows of the dry-run report rows on stdin, _deletion_task left out.
sum_est_rows() {
  awk '
    function flush() { if (g != "_deletion_task") n += e; g = ""; e = 0 }
    /^ *- / || /^[^ -]/ { flush() }
    /^ *-? *group:/ { g = $NF }
    /^ *-? *estRows:/ { e = $NF }
    END { flush(); print n + 0 }
  '
}

# replay_failed prints the permanent failure record and exits when an earlier attempt left the
# destination with a partial import.
replay_failed() {
  if [ -f "$IMPORT_WORK/import.failed" ]; then
    cat "$IMPORT_WORK/import.failed"
    exit 1
  fi
}

# mark_failed records a permanent failure: $1 is a one-line reason.
mark_failed() {
  printf 'importFailed: %s\n' "$1" >"$IMPORT_WORK/import.failed"
  echo "permanent failure: $1 (see $IMPORT_WORK)" >&2
}

# bool prints true when the command succeeds and false otherwise.
bool() {
  if "$@"; then echo true; else echo false; fi
}
