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

# Import case 1, an import without the artifact's schema into the empty destination is refused.
# Freezes artifact A (a full export of the source) on first use, then runs the import of A with
# --include-schema=false twice, as a dry run and for real: the destination has none of A's
# groups and the import does not create them, so both must exit 2 (preflight refused, nothing
# written), print the same decision table naming the missing resources under PROBLEMS, leave the
# destination schema as it was (compared through --schema-only exports) and write no row.
# The output is one yaml document for expected/import-noschema.yaml. Once it passed, the report
# is replayed; an import that wrote something (exit 0 or >= 3) is a permanent failure.
#
#   import-noschema.sh <source liaison host:grpc-port>
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"
# shellcheck source=import-common.sh
source "$DIR/import-common.sh"

src="${1:?usage: import-noschema.sh <source liaison host:grpc-port>}"
mkdir -p "$IMPORT_WORK"
replay_failed
if [ -f "$IMPORT_WORK/noschema.done" ]; then
  cat "$IMPORT_WORK/noschema.out"
  exit 0
fi
freeze_artifact_a "$src"

work="$IMPORT_WORK/noschema"
mkdir -p "$work"
before="$(dst_schema_digest "$work/schema-before")"

# run_import runs the schema-less import of A with the extra flags $@ into $work/<name>.*,
# printing its exit code.
run_import() {
  local name="$1" rc=0
  shift
  bydbctl data import --nodes "$DST_GRPC" --input "$ARTIFACT_A" --include-schema=false "$@" \
    >"$work/$name.stdout" 2>"$work/$name.stderr" || rc=$?
  echo "schema-less $name exited $rc" >&2
  tail -40 "$work/$name.stdout" "$work/$name.stderr" >&2
  echo "$rc"
}
dry_rc="$(run_import dryrun --dry-run)"
rc="$(run_import import)"
if [ "$rc" -ne 1 ] && [ "$rc" -ne 2 ]; then
  mark_failed "the schema-less import exited $rc instead of 2"
  replay_failed
fi
table_section "$work/dryrun.stdout" >"$work/dryrun.table"
table_section "$work/import.stdout" >"$work/import.table"
after="$(dst_schema_digest "$work/schema-after")"
rows=0
for _ in 1 2 3 4 5 6; do
  sleep 2
  rows="$(dst_data_rows)"
  [ "$rows" = 0 ] || break
done
missing="$(bool grep -q 'do not exist on the target, and the import does not create them' "$work/import.table")"

report="$work/report"
{
  echo "dryRunExitCode: $dry_rc"
  echo "importExitCode: $rc"
  echo "tablesIdentical: $(bool cmp -s "$work/dryrun.table" "$work/import.table")"
  echo "missingResourcesReported: $missing"
  echo "schemaUnchanged: $(bool test "$before" = "$after")"
  echo "dstDataRows: $rows"
} >"$report"
cat "$report"
if [ "$dry_rc" -eq 2 ] && [ "$rc" -eq 2 ] && [ "$missing" = true ] && [ "$before" = "$after" ] && [ "$rows" = 0 ] &&
  cmp -s "$work/dryrun.table" "$work/import.table"; then
  mv "$report" "$IMPORT_WORK/noschema.out"
  touch "$IMPORT_WORK/noschema.done"
fi
