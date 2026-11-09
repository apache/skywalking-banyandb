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


# Import case 2, a schema conflict refuses the whole import. Runs after the schema-less case
# (import-noschema.sh), which froze artifact A, then pre-creates on the empty destination the group sw_records exactly
# as A exports it and its stream event with one extra tag (export-checker conflict-schema writes
# both; an "already exists" from an earlier attempt is fine). Importing A without a conflict
# policy must exit 2 (preflight refused, nothing written) as a dry run and for real, print the
# same decision table both times, list the stream as conflicting, leave the
# destination schema as it was (compared through --schema-only exports) and write no row.
# The output is one yaml document for expected/import-conflict.yaml. Once it passed, the report
# is replayed; an import that wrote something (exit 0 or >= 3) is a permanent failure.
#
#   import-conflict.sh <source liaison host:grpc-port>
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"
# shellcheck source=import-common.sh
source "$DIR/import-common.sh"

src="${1:?usage: import-conflict.sh <source liaison host:grpc-port>}"
mkdir -p "$IMPORT_WORK"
replay_failed
if [ -f "$IMPORT_WORK/conflict.done" ]; then
  cat "$IMPORT_WORK/conflict.out"
  exit 0
fi
if [ ! -f "$IMPORT_WORK/noschema.done" ]; then
  echo "the schema-less case has not passed; artifact A or the destination is not ready" >&2
  exit 1
fi
freeze_artifact_a "$src"

work="$IMPORT_WORK/conflict"
mkdir -p "$work"
export-checker conflict-schema "$ARTIFACT_A" "$work" >&2

# create_once creates a $1 (group|stream) from file $2 on the destination; it already existing
# from an earlier attempt is fine.
create_once() {
  local out
  if out="$(bydbctl "$1" create -a "$DST_HTTP" -f "$2" 2>&1)"; then
    echo "$out" >&2
    return 0
  fi
  if grep -qi 'already exist' <<<"$out"; then
    echo "$1 from $2 already exists on the destination" >&2
    return 0
  fi
  echo "creating $1 from $2 failed: $out" >&2
  return 1
}
create_once group "$work/group.json"
create_once stream "$work/stream.json"

before="$(dst_schema_digest "$work/schema-before")"
dry_rc=0
bydbctl data import --nodes "$DST_GRPC" --input "$ARTIFACT_A" --dry-run >"$work/dryrun.stdout" 2>"$work/dryrun.stderr" || dry_rc=$?
echo "conflict dry run exited $dry_rc" >&2
rc=0
bydbctl data import --nodes "$DST_GRPC" --input "$ARTIFACT_A" >"$work/import.stdout" 2>"$work/import.stderr" || rc=$?
echo "conflict import exited $rc" >&2
tail -n 80 "$work/import.stdout" "$work/import.stderr" >&2
if [ "$rc" -ne 1 ] && [ "$rc" -ne 2 ]; then
  mark_failed "the conflict import exited $rc instead of 2"
  replay_failed
fi
after="$(dst_schema_digest "$work/schema-after")"
# dst_data_rows counts flushed parts only, so rows a faulty import wrote may still sit in a
# memtable; poll past the flush window and stop at the first non-zero count.
rows=0
for _ in 1 2 3 4 5 6; do
  sleep 2
  rows="$(dst_data_rows)"
  [ "$rows" = 0 ] || break
done
# Exit 2 also covers refusals unrelated to the pre-created stream (any preflight error, another
# CONFLICT/INVALID/MISSING resource); only the expected refusal may be frozen as passed.
conflict="$(bool grep -q 'stream/sw_records/event conflicts with the target' "$work/import.stdout")"
table_section "$work/dryrun.stdout" >"$work/dryrun.table"
table_section "$work/import.stdout" >"$work/import.table"

report="$work/report"
{
  echo "dryRunExitCode: $dry_rc"
  echo "importExitCode: $rc"
  echo "tablesIdentical: $(bool cmp -s "$work/dryrun.table" "$work/import.table")"
  echo "conflictOnPrecreatedStream: $conflict"
  echo "schemaUnchanged: $(bool test "$before" = "$after")"
  echo "dstDataRows: $rows"
} >"$report"
cat "$report"
if [ "$dry_rc" -eq 2 ] && [ "$rc" -eq 2 ] && [ "$conflict" = true ] && [ "$before" = "$after" ] && [ "$rows" = 0 ] &&
  cmp -s "$work/dryrun.table" "$work/import.table"; then
  mv "$report" "$IMPORT_WORK/conflict.out"
  touch "$IMPORT_WORK/conflict.done"
fi
