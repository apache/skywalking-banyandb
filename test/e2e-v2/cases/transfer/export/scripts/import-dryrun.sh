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


# Import case 2, the dry run predicts the real import. With plan-dst.yaml (its overwrite rule
# resolves the conflict import-conflict.sh set up) and --force-ttl, runs the import once as a
# --dry-run and once for real; the decision table the real run prints first must be byte for byte
# the dry run's (see table_section), the dry run must leave the destination schema unchanged, and
# the table rows whose STATUS column is expired (units older than the destination's TTL, which
# --force-ttl imports anyway) must be exactly those the importer's TTL gate predicts from artifact
# A's manifest (checker ttl-rows, printed as ttlExpected). Which units have expired depends on the
# day (the source's hour/day metrics may or may not have moved to warm), so the count is computed
# rather than pinned. The output is one yaml document for expected/import-dryrun.yaml.
# The real import runs at most once per e2e run: exit 0 marks it done and later attempts replay
# the report; an exit that may have written rows (3, 4, 5) is a permanent failure; exits 1 and 2
# wrote nothing and are retried.
#
#   import-dryrun.sh
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"
# shellcheck source=import-common.sh
source "$DIR/import-common.sh"

mkdir -p "$IMPORT_WORK"
replay_failed
if [ -f "$IMPORT_WORK/import.done" ]; then
  cat "$IMPORT_WORK/dryrun.out"
  exit 0
fi
if [ ! -f "$IMPORT_WORK/conflict.done" ]; then
  echo "the conflict case has not passed; artifact A or the destination is not ready" >&2
  exit 1
fi

work="$IMPORT_WORK/dryrun"
mkdir -p "$work"
plan="$DIR/../plan-dst.yaml"

before="$(dst_schema_digest "$work/schema-before")"
ttl_before="$(export-checker ttl-rows "$ARTIFACT_A")"
dry_rc=0
bydbctl data import --dry-run --plan "$plan" --input "$ARTIFACT_A" --force-ttl >"$work/dryrun.stdout" 2>"$work/dryrun.stderr" || dry_rc=$?
after="$(dst_schema_digest "$work/schema-after")"
# Taken last, right before the real import, so the window in which a unit can cross the TTL
# boundary unguarded is as short as possible.
ttl_after="$(export-checker ttl-rows "$ARTIFACT_A")"
echo "dry run exited $dry_rc" >&2
cat "$work/dryrun.stdout" >&2
tail -n 50 "$work/dryrun.stderr" >&2
if [ "$dry_rc" -ne 0 ]; then
  echo "dryRunExitCode: $dry_rc"
  exit 1
fi
# A unit that crossed the TTL boundary while the dry run ran makes the prediction ambiguous;
# nothing was imported yet, so the attempt is retried.
if [ "$ttl_before" != "$ttl_after" ]; then
  echo "the predicted expired rows changed during the dry run ($ttl_before, then $ttl_after); retrying" >&2
  echo "$ttl_after"
  exit 1
fi

rc=0
bydbctl data import --plan "$plan" --input "$ARTIFACT_A" --force-ttl >"$work/import.stdout" 2>"$work/import.stderr" || rc=$?
# Diagnostic only: a failure here must not stop the script before a successful import is marked done.
ttl_import="$(export-checker ttl-rows "$ARTIFACT_A")" || ttl_import="unknown"
echo "import exited $rc" >&2
tail -n 80 "$work/import.stdout" "$work/import.stderr" >&2
case "$rc" in
  0) ;;
  1 | 2)
    echo "importExitCode: $rc"
    exit 1
    ;;
  *)
    mark_failed "the real import exited $rc"
    replay_failed
    ;;
esac

table_section "$work/dryrun.stdout" >"$work/dryrun.table"
table_section "$work/import.stdout" >"$work/import.table"
identical=true
if ! cmp -s "$work/dryrun.table" "$work/import.table"; then
  identical=false
  echo "the decision tables differ (dry run vs real import):" >&2
  diff "$work/dryrun.table" "$work/import.table" >&2 || true
  if [ "$ttl_after" != "$ttl_import" ]; then
    echo "a unit crossed the TTL boundary between the dry run and the real import ($ttl_after, then $ttl_import)" >&2
  fi
fi

report="$work/report"
{
  echo "dryRunExitCode: $dry_rc"
  echo "importExitCode: $rc"
  echo "tablesIdentical: $identical"
  echo "tableHasResult: $(bool grep -q '^RESULT' "$work/dryrun.table")"
  echo "schemaUnchangedByDryRun: $(bool test "$before" = "$after")"
  # Table rows whose STATUS column is expired.
  echo "ttlRows: $(grep -cE '^(stream|measure|trace|property) .* expired ' "$work/dryrun.table" || true)"
  echo "$ttl_after"
} >"$report"
mv "$report" "$IMPORT_WORK/dryrun.out"
touch "$IMPORT_WORK/import.done"
cat "$IMPORT_WORK/dryrun.out"
