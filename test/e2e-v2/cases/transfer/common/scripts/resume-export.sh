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

# The export resume cases, in FORMAT, one per invocation and in this order:
#
#   sigint        export the source as CSV into A once (the reference of the CSV checks, taken
#                 before E so both see the same data), then export every catalog into E at
#                 EXPORT_RATE per node in the background, wait until
#                 the progress file records a finished unit while the export still runs, SIGINT it:
#                 exit 3, the progress file holds the session id, no manifest.json, and the
#                 release-session hint is printed.
#   kill          resume at the same rate and kill -9 it once one more unit is done while a unit is
#                 in flight with files written: the progress file stays, with files below nodes/ it
#                 does not list (a .tmp or a volume of an unfinished unit). The unit in flight is
#                 recorded only with the next write of the progress file, so it is not waited for.
#                 When an earlier attempt let the export finish, nothing is left to interrupt: the
#                 attempt exports E again from scratch and kills that run instead.
#   summary       the same command without --resume: exit 1, the resume summary (DONE and LEFT with
#                 counts and sizes) on stdout and the --resume hint on stderr, and E is unchanged (a digest of
#                 every file and its size).
#   binding       --resume with --include-schema=false: exit 6 naming the options, E unchanged.
#                 (--max-file-size may change on a resume; it is not a binding.)
#   resumed       --resume without a rate limit: exit 0, manifest.json written, the progress file
#                 gone, every file below nodes/ listed by the manifest (no .tmp, no orphan volume),
#                 the session released (release-session of its id finds it on no node), and the
#                 export checker accepts E (native: checker native; CSV: checker compare with A, an
#                 uninterrupted CSV export of the source). The run reports as resumed exactly the
#                 units done before it and rewrites none of their files (same size, modification
#                 time and inode): a resume that exported a done unit again would pass every other
#                 check, since the file names are deterministic. The manifest lists every unit of
#                 every node the recorded plan lists, no more and no fewer: a resume that dropped
#                 a unit left would pass the native checker, which checks what the manifest lists.
#   session-lost  a new export into L, SIGINT once its plan is recorded, release-session of its id,
#                 then --resume: exit 6 with the hint to release the session, delete what is in L and export again.
#
# Each prints one yaml document for expected/resume-export-<case>.yaml. See resume-common.sh for
# the markers that make a retry safe; an interrupted export is resumable, so the steps before a
# case passes may be repeated.
#
#   FORMAT=<csv|native> resume-export.sh <source liaison host:grpc-port> <case>
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"
# shellcheck source=resume-common.sh
source "$DIR/resume-common.sh"

usage="usage: FORMAT=<csv|native> resume-export.sh <source liaison host:grpc-port> <sigint|kill|summary|binding|resumed|session-lost>"
src="${1:?$usage}"
case_name="export-${2:?$usage}"
begin_case "$case_name"
report="$CASE_DIR/report"

case "$2" in
  sigint)
    # A failed earlier attempt may have left a session: release it so the export is not refused
    # as a second one, then start over.
    freeze_reference "$src"
    probe export "$EXPORT_E"
    release_session "$P_SESSION" "$src"
    rm -rf "$EXPORT_E"
    EXEC=exec export_cmd "$src" "$EXPORT_E" --node-rate-limit "$EXPORT_RATE" >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" &
    pid=$! BG_PID=$!
    waited=true
    wait_for 600 'probe export "$EXPORT_E"; [ "$P_DONE" -ge 1 ]' "$pid" || waited=false
    stop_process INT "$pid"
    rc="$STOP_RC"
    tail -n 40 "$CASE_DIR/stdout" "$CASE_DIR/stderr" >&2
    probe export "$EXPORT_E"
    hint=false
    if [ -n "$P_SESSION" ] && grep -q "release-session --id $P_SESSION" "$CASE_DIR/stderr"; then
      hint=true
    fi
    {
      echo "stoppedWithUnitsDone: $waited"
      echo "exitCode: $rc"
      echo "progressFile: $P_PROGRESS"
      echo "sessionRecorded: $(bool grep -qE '^[0-9a-f]{32}$' <<<"$P_SESSION")"
      echo "unitsDone: $(bool test "$P_DONE" -ge 1)"
      echo "manifest: $P_MANIFEST"
      echo "releaseSessionHint: $hint"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  kill)
    probe export "$EXPORT_E"
    resume=(--resume)
    if [ "$P_PROGRESS" != true ]; then
      # An earlier attempt let the export finish (or never left a progress file): export E again
      # from scratch and kill that run.
      echo "E holds no export to resume (manifest: $P_MANIFEST); exporting it again from scratch" >&2
      release_session "$P_SESSION" "$src"
      rm -rf "$EXPORT_E"
      resume=()
      P_DONE=0
    fi
    done_before="$P_DONE"
    EXEC=exec export_cmd "$src" "$EXPORT_E" ${resume[@]+"${resume[@]}"} --node-rate-limit "$EXPORT_RATE" >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" &
    pid=$! BG_PID=$!
    waited=true
    wait_for 600 'probe export "$EXPORT_E"; [ "$P_DONE" -gt "$done_before" ] && [ "$P_UNLISTED" -gt 0 ]' "$pid" || waited=false
    stop_process KILL "$pid"
    rc="$STOP_RC"
    tail -n 40 "$CASE_DIR/stdout" "$CASE_DIR/stderr" >&2
    probe export "$EXPORT_E"
    {
      echo "killedWithUnitInFlight: $waited"
      echo "exitCode: $rc"
      echo "progressFile: $P_PROGRESS"
      echo "unlistedFilesLeft: $(bool test "$P_UNLISTED" -gt 0)"
      echo "manifest: $P_MANIFEST"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  summary)
    probe listing "$EXPORT_E"
    before="$P_LISTING"
    rc=0
    export_cmd "$src" "$EXPORT_E" >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" || rc=$?
    cat "$CASE_DIR/stderr" >&2
    probe listing "$EXPORT_E"
    {
      echo "exitCode: $rc"
      echo "summaryDone: $(bool grep -qE '^DONE +[0-9]+ group\(s\), [0-9]+ unit\(s\), [0-9]+ file\(s\), [0-9.]+ [KMGT]?i?B' "$CASE_DIR/stdout")"
      echo "summaryLeft: $(bool grep -qE '^LEFT +[0-9]+ group\(s\), [1-9][0-9]* unit\(s\)(, [0-9]+ shard\(s\))?, est\. [0-9.]+ [KMGT]?i?B' "$CASE_DIR/stdout")"
      echo "resumeHint: $(bool grep -q 'add --resume to continue' "$CASE_DIR/stderr")"
      echo "unchanged: $(bool test "$before" = "$P_LISTING")"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  binding)
    probe listing "$EXPORT_E"
    before="$P_LISTING"
    rc=0
    export_cmd "$src" "$EXPORT_E" --resume --include-schema=false >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" || rc=$?
    cat "$CASE_DIR/stderr" >&2
    probe listing "$EXPORT_E"
    {
      echo "exitCode: $rc"
      echo "namesBinding: $(bool grep -q 'includeSchema: recorded true (default), now false' "$CASE_DIR/stderr")"
      echo "unchanged: $(bool test "$before" = "$P_LISTING")"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  resumed)
    # Finishing the export is done once: a retry after it only repeats the checks. Before it, the
    # session, the units done and a digest of their files are kept for the checks.
    if [ ! -f "$CASE_DIR/finished" ]; then
      probe export "$EXPORT_E"
      echo "$P_SESSION" >"$CASE_DIR/session"
      echo "$P_DONE" >"$CASE_DIR/done-before"
      export-checker resume-state done-files "$EXPORT_E" >"$CASE_DIR/done-files"
      export-checker resume-state plan-units "$EXPORT_E" >"$CASE_DIR/plan-units"
      probe files "$EXPORT_E" "$CASE_DIR/done-files"
      echo "$P_FILES" >"$CASE_DIR/done-files-before"
      rc=0
      export_cmd "$src" "$EXPORT_E" --resume >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" || rc=$?
      tail -n 60 "$CASE_DIR/stdout" "$CASE_DIR/stderr" >&2
      if [ "$rc" -ne 0 ]; then
        probe export "$EXPORT_E"
        if [ "$P_PROGRESS" = false ]; then
          # The export finished and failed after it: a retry finds no progress file to resume.
          fail_for_good "exitCode: $rc" "progressFile: false"
        fi
        echo "exitCode: $rc"
        exit 1
      fi
      touch "$CASE_DIR/finished"
    fi
    session="$(cat "$CASE_DIR/session" 2>/dev/null || true)"
    probe export "$EXPORT_E"
    rel_rc=0
    rel="no session id was recorded before the export finished"
    if [ -n "$session" ]; then
      rel="$(bydbctl data release-session --id "$session" --nodes "$src" 2>&1)" || rel_rc=$?
    fi
    echo "$rel" >&2
    # The units the run reports as resumed, and the files of the units done before it.
    resumed_units="$(grep -hoE '^RESUMED +[0-9]+' "$CASE_DIR/stderr" "$CASE_DIR/stdout" | grep -oE '[0-9]+$' | head -n 1 || true)"
    probe files "$EXPORT_E" "$CASE_DIR/done-files"
    done_files="$P_FILES"
    units_rc=0
    export-checker resume-state manifest-units "$EXPORT_E" >"$CASE_DIR/manifest-units" || units_rc=$?
    if ! diff "$CASE_DIR/plan-units" "$CASE_DIR/manifest-units" >&2; then
      units_rc=1
    fi
    check_rc=0
    if [ "$FORMAT" = native ]; then
      export-checker native "$EXPORT_E" >"$CASE_DIR/check.out" 2>"$CASE_DIR/check.log" || check_rc=$?
    else
      freeze_reference "$src"
      export-checker compare "$ARTIFACT_A" "$EXPORT_E" >"$CASE_DIR/check.out" 2>"$CASE_DIR/check.log" || check_rc=$?
    fi
    cat "$CASE_DIR/check.out" "$CASE_DIR/check.log" >&2
    {
      echo "exitCode: 0"
      echo "resumedLine: $(bool grep -qE '^RESUMED +[1-9][0-9]* ' "$CASE_DIR/stderr" "$CASE_DIR/stdout")"
      echo "resumedTheUnitsDone: $(bool test "$resumed_units" = "$(cat "$CASE_DIR/done-before")")"
      echo "doneFilesUnchanged: $(bool test -s "$CASE_DIR/done-files" -a "$done_files" = "$(cat "$CASE_DIR/done-files-before")")"
      echo "manifest: $P_MANIFEST"
      echo "unitsAsPlanned: $(bool test "$units_rc" -eq 0 -a -s "$CASE_DIR/plan-units")"
      echo "progressFile: $P_PROGRESS"
      echo "tmpFiles: $P_TMP"
      echo "unlistedFiles: $P_UNLISTED"
      echo "sessionReleasedExitCode: $rel_rc"
      echo "sessionNotHeld: $(bool grep -q "export session $session is not held on any data node" <<<"$rel")"
      echo "checkerExitCode: $check_rc"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  session-lost)
    probe export "$EXPORT_L"
    release_session "$P_SESSION" "$src"
    rm -rf "$EXPORT_L"
    EXEC=exec export_cmd "$src" "$EXPORT_L" --node-rate-limit "$EXPORT_RATE" >"$CASE_DIR/first.stdout" 2>"$CASE_DIR/first.stderr" &
    pid=$! BG_PID=$!
    waited=true
    wait_for 600 'probe export "$EXPORT_L"; [ "$P_PLAN" = true ]' "$pid" || waited=false
    stop_process INT "$pid"
    int_rc="$STOP_RC"
    probe export "$EXPORT_L"
    session="$P_SESSION"
    rel_rc=0
    bydbctl data release-session --id "$session" --nodes "$src" >&2 || rel_rc=$?
    rc=0
    export_cmd "$src" "$EXPORT_L" --resume >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" || rc=$?
    cat "$CASE_DIR/stderr" >&2
    {
      echo "interruptedWithPlan: $waited"
      echo "interruptExitCode: $int_rc"
      echo "releaseExitCode: $rel_rc"
      echo "resumeExitCode: $rc"
      echo "exportAgainHint: $(bool grep -qF "to start over: release the session with: bydbctl data release-session --id $session; then delete what is in $EXPORT_L (the progress file $EXPORT_L/.export-progress.json included) and export again" "$CASE_DIR/stderr")"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  *)
    echo "$usage" >&2
    exit 2
    ;;
esac
