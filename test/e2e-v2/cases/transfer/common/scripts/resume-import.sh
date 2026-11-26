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

# The import resume cases, importing E, the export the export cases resumed, into the empty
# destination, one per invocation and in this order:
#
#   interrupt   import E slowed down (import_slow: native --part-rate-limit, CSV --write-rate-limit
#               in small batches) in the background, SIGINT it once its progress file
#               (<E>/.import-progress.json) records more done than when it started: exit 3 (a CSV
#               import first drains the batches in flight). Then resume it the same way and kill -9
#               it once more is done while something is in flight (native: a stream or trace part,
#               the payload reconciliation decides; CSV: a started item with rows sent after its
#               cursor and not acknowledged, the ambiguous window), and check after the kill that it
#               still is. The progress file has the fixed name, next to no other file of that name
#               but its side files (-cursor.json, -plan.json, .lock) and the temporary
#               files of an atomic write the kill interrupted (..import-progress[-<side>].json.<n>.tmp, listed
#               on stdout). An import that finished before it could be stopped leaves nothing to
#               interrupt: the case then fails for good, saying so.
#   summary     the same command without --resume: exit 1, the resume summary on stdout and the
#               --resume hint on stderr; then --dry-run: exit 0 and the resume summary on
#               stdout (DONE and LEFT work items, the AMBIGUOUS section naming each payload's action
#               or policy: a native part in flight, or the rows a CSV item sent after its cursor,
#               "rows after ... up to ... (<k> row(s) sent)"). The progress file and its side files
#               are unchanged.
#   resumed     --resume --verify: exit 0 and VERIFY ok (the rows pushed again are warnings only;
#               a trace group that samples spans in merges is "checked partially", and the VERIFY
#               line then counts such groups as "not fully checked"),
#               RESUMED naming as done the items complete before it, the stream and trace rows
#               pushed over every run at least those of one uninterrupted import and, per (catalog,
#               group, stage), above them by no more than the rows recorded as possible duplicates
#               of that group (CSV: equal to them), the progress file kept (and
#               named on a "progress: <file>" line) with every planned item complete and nothing in
#               flight, and its cursor side file deleted.
#   again       the finished import run again: without --resume it exits 0 and asks nothing, says
#               "already imported into this cluster at <time>", pushes nothing and skips the count
#               verify (a NOTE says why); with an explicit --verify it verifies the node counts
#               against the baseline the first run recorded and passes. The progress file and its side files are left as they were.
#   roundtrip   the destination re-exported as CSV compared with A, a CSV export of the source, row
#               by row and by queries (the import-roundtrip.sh checks): nothing lost. The comparison
#               is of distinct rows, so the resumed case bounds what was stored twice.
#   reconcile   native only: the resumed run printed at least one "reconcile ...: landed" or
#               "not landed" decision for the part in flight when the import was killed.
#   reset       every group the import wrote into is deleted on the destination with bydbctl group
#               delete --force (a wiped destination of the same names), then the finished import
#               run again: exit 6, the destination lacks those groups and "looks reset or is
#               another cluster", with the --checkpoint and --restart hints and no "already
#               imported"; the refusal is the data check of the progress file; --dry-run refuses
#               it the same way. The progress file and its side files
#               are left as they were. It empties the destination, so it comes after every check
#               of what the import stored.
#   restart     --restart (--verify=false): the progress file (the one the resumed case finished,
#               byte for byte) and its plan side file (with the baseline) are kept as <name>.<time>.bak after a
#               WARN, and a fresh import of every item into the reset destination runs and
#               finishes. It is the last case.
#
# Each prints one yaml document for expected/resume-import-<case>.yaml (roundtrip:
# expected/import-roundtrip.yaml). The import stages leave markers in the case directory, so a
# retry continues the import from where the last attempt left it instead of starting again. The
# resumed and restart imports run until one attempt succeeds: an attempt that fails after it
# changed the progress file has decided or imported something a retry would not repeat, so the
# case fails for good (fail_for_good) instead; every attempt keeps its own output files
# (stdout.<n>, stderr.<n>), and stdout and stderr are copies of the attempt that succeeded.
#
#   FORMAT=<csv|native> resume-import.sh <source liaison host:grpc-port> <case>
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"
# shellcheck source=resume-common.sh
source "$DIR/resume-common.sh"

usage="usage: FORMAT=<csv|native> resume-import.sh <source liaison host:grpc-port> <interrupt|summary|resumed|again|roundtrip|reconcile|reset|restart>"
src="${1:?$usage}"
case_name="import-${2:?$usage}"
begin_case "$case_name"
report="$CASE_DIR/report"
# shellcheck disable=SC2207
slow=($(import_slow))

# in_flight is the state in which the resumed run is killed: native, a stream or trace part in
# flight; CSV, an item started and not finished with rows sent after its cursor and not
# acknowledged (the ambiguous window a resume lists).
in_flight() {
  if [ "$FORMAT" = native ]; then
    [ "$P_INFLIGHT_STREAM_PARTS" -gt 0 ]
  else
    [ "$P_STARTED" -gt 0 ] && [ "$P_SENT_WINDOW" -gt 0 ]
  fi
}

# kill_slow are the flags of the kill stage of import-interrupt. Native: the stream and trace parts
# that stage waits for come after every measure and property part, so the part rate is derived
# from the bytes of those parts in the manifest of E, for them to pass in about a minute (at least
# 4KiB/s) however much the source holds; the limiter, its burst one 64KiB chunk, then still holds
# the stream or trace part back while it is in flight. CSV: import_slow.
kill_slow() {
  if [ "$FORMAT" != native ]; then
    import_slow
    return
  fi
  local rate
  rate="$(python3 - "$EXPORT_E/manifest.json" <<'EOF'
import json
import sys

def sizes(v):
    if isinstance(v, dict):
        if "path" in v and "bytes" in v:
            yield v["bytes"]
            return
        for x in v.values():
            yield from sizes(x)
    elif isinstance(v, list):
        for x in v:
            yield from sizes(x)

with open(sys.argv[1]) as f:
    m = json.load(f)
total = sum(sum(sizes(u)) for u in m.get("units") or [] if (u.get("catalog") or "").lower() in ("measure", "property"))
print(max(4096, -(-total // 60)))
EOF
)"
  echo "--part-rate-limit ${rate}/s --part-chunk-size 64KiB --parallelism 1"
}

# kill_condition is in_flight with more rows pushed than when the run started. The rows pushed
# only grow; the landed payloads do not (a complete item keeps none).
kill_condition() {
  probe import "$(import_progress)"
  [ "$P_ROWS" -gt "$rows_before" ] && in_flight
}

# progress_moved tells whether the progress file records more done than at the start of the run.
progress_moved() {
  probe import "$(import_progress)"
  [ "$P_COMPLETE" -gt "$complete_before" ] || [ "$P_ROWS" -gt "$rows_before" ]
}

# fail_if_finished fails the case when the import already completed every planned item: there is
# nothing left to interrupt, and the destination already holds the data, so a retry cannot help.
fail_if_finished() {
  probe import "$(import_progress)"
  if [ "$P_PLAN_ITEMS" -gt 0 ] && [ "$P_COMPLETE" -eq "$P_PLAN_ITEMS" ]; then
    echo "the import of E finished before it could be interrupted ($1); the run has to start over with an empty destination" >&2
    fail_for_good "finishedBeforeInterrupted: true"
  fi
}

# exists tells whether some file matches the glob $1.
exists() {
  compgen -G "$1" >/dev/null
}

# run_once runs import_cmd with the arguments as attempt <n> of the current case, into stdout.<n>
# and stderr.<n>; on success it copies them to stdout and stderr. A failed attempt that left the
# progress file of E as it found it changed nothing and may be retried; one that changed it fails
# the case for good.
run_once() {
  local n rc=0 before
  n="$(next_attempt stderr)"
  before="$(progress_digest "$(import_progress)")"
  import_cmd "$@" >"$CASE_DIR/stdout.$n" 2>"$CASE_DIR/stderr.$n" || rc=$?
  tail -n 80 "$CASE_DIR/stdout.$n" "$CASE_DIR/stderr.$n" >&2
  if [ "$rc" -ne 0 ]; then
    if [ "$before" != "$(progress_digest "$(import_progress)")" ]; then
      fail_for_good "exitCode: $rc" "progressChangedByTheFailedAttempt: true"
    fi
    echo "exitCode: $rc"
    exit 1
  fi
  cp "$CASE_DIR/stdout.$n" "$CASE_DIR/stdout"
  cp "$CASE_DIR/stderr.$n" "$CASE_DIR/stderr"
}

# remember_progress keeps what the progress file records as done, for progress_moved.
remember_progress() {
  probe import "$(import_progress)"
  complete_before="$P_COMPLETE" rows_before="$P_ROWS"
}

case "$2" in
  interrupt)
    # Stage 1: interrupt the first run. A retry finds the progress file and resumes instead.
    if [ ! -f "$CASE_DIR/sigint.done" ]; then
      fail_if_finished "first run"
      resume=()
      if [ "$P_PROGRESS" = true ]; then
        resume=(--resume)
      fi
      # Only what this run adds counts: a retry finds the progress of an earlier attempt, and a
      # signal sent before bydbctl installed its handler would be ignored.
      remember_progress
      EXEC=exec import_cmd "${slow[@]}" ${resume[@]+"${resume[@]}"} >"$CASE_DIR/sigint.stdout" 2>"$CASE_DIR/sigint.stderr" &
      pid=$! BG_PID=$!
      waited=true
      wait_for 600 progress_moved "$pid" || waited=false
      stop_process INT "$pid"
      rc="$STOP_RC"
      tail -n 40 "$CASE_DIR/sigint.stdout" "$CASE_DIR/sigint.stderr" >&2
      echo "$rc" >"$CASE_DIR/sigint.rc"
      if [ "$waited" != true ] || [ "$rc" != 3 ]; then
        echo "sigintExitCode: $rc"
        echo "stoppedWithSomethingDone: $waited"
        exit 1
      fi
      touch "$CASE_DIR/sigint.done"
    fi
    # Stage 2: kill -9 the resumed run while something is in flight.
    if [ ! -f "$CASE_DIR/kill.done" ]; then
      fail_if_finished "resumed run"
      remember_progress
      # shellcheck disable=SC2207
      kill_flags=($(kill_slow))
      echo "kill stage flags: ${kill_flags[*]}" >&2
      EXEC=exec import_cmd "${kill_flags[@]}" --resume >"$CASE_DIR/kill.stdout" 2>"$CASE_DIR/kill.stderr" &
      pid=$! BG_PID=$!
      waited=true
      wait_for 600 kill_condition "$pid" || waited=false
      stop_process KILL "$pid"
      rc="$STOP_RC"
      tail -n 40 "$CASE_DIR/kill.stdout" "$CASE_DIR/kill.stderr" >&2
      # What was in flight when the condition held may have finished before the kill: check again.
      still=false
      if [ "$waited" = true ]; then
        probe import "$(import_progress)"
        if in_flight; then
          still=true
        fi
      fi
      if [ "$waited" != true ] || [ "$rc" != 137 ] || [ "$still" != true ]; then
        echo "killExitCode: $rc"
        echo "killedWithSomethingInFlight: $waited"
        echo "stillInFlightAfterKill: $still"
        exit 1
      fi
      touch "$CASE_DIR/kill.done"
    fi
    probe import "$(import_progress)"
    # Every file of E named like the progress file, temporary files of an atomic write of it
    # (".<name>.<n>.tmp") included: the fixed name, its side files and such temporary files only.
    named="$(cd "$EXPORT_E" && ls -A | grep -E '^\.\.?import-progress' || true)"
    temps="$(grep -xE '\.\.import-progress(-(cursor|plan))?\.json\.[0-9]+\.tmp' <<<"$named" || true)"
    others="$(grep -vxE '\.import-progress(-(cursor|plan))?\.json|\.import-progress\.json\.lock|\.\.import-progress(-(cursor|plan))?\.json\.[0-9]+\.tmp' <<<"$named" || true)"
    if [ -n "$temps" ]; then
      echo "temporary files of the progress file the kill left: $temps" >&2
    fi
    {
      echo "sigintExitCode: $(cat "$CASE_DIR/sigint.rc")"
      echo "killed: true"
      echo "progressFile: $P_PROGRESS"
      echo "progressFileName: $(bool test -z "$others")"
    } >"$report"
    if [ -n "$others" ]; then
      echo "files named like the progress file: $others" >&2
    fi
    finish_case "$case_name" "$report" true
    ;;

  summary)
    progress="$(import_progress)"
    before="$(progress_digest "$progress")"
    rc=0
    import_cmd "${slow[@]}" >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" || rc=$?
    cat "$CASE_DIR/stderr" >&2
    dry_rc=0
    import_cmd --dry-run >"$CASE_DIR/dry.stdout" 2>"$CASE_DIR/dry.stderr" || dry_rc=$?
    cat "$CASE_DIR/dry.stderr" >&2
    # The AMBIGUOUS header and the payload lines under it: "  <payload>: <action> [(<source>)]",
    # where a reconciled payload adds ", else <policy> (<source>)".
    ambiguous="$(awk '/^AMBIGUOUS [0-9]+ payload/ { on = 1; next } on && /^  / { print; next } { on = 0 }' "$CASE_DIR/dry.stdout")"
    {
      echo "exitCode: $rc"
      echo "resumeHint: $(bool grep -q 'add --resume to continue' "$CASE_DIR/stderr")"
      echo "summaryShown: $(bool grep -qE '^(DONE|LEFT) +[0-9]+ group\(s\)' "$CASE_DIR/stdout")"
      echo "ambiguousShown: $(bool grep -qE '^AMBIGUOUS [0-9]+ payload' "$CASE_DIR/stdout")"
      echo "dryRunExitCode: $dry_rc"
      echo "resumeLine: $(bool grep -qF "RESUME   $progress" "$CASE_DIR/dry.stdout")"
      echo "summaryDone: $(bool grep -qE '^DONE +[0-9]+ group\(s\), [0-9]+ work item\(s\)' "$CASE_DIR/dry.stdout")"
      echo "summaryLeft: $(bool grep -qE '^LEFT +[0-9]+ group\(s\), [0-9]+ work item\(s\)' "$CASE_DIR/dry.stdout")"
      echo "ambiguousListed: $(bool test -n "$ambiguous")"
      echo "policiesNamed: $(bool grep -qE ': (reconcile|re-push|block|warn|push|skip)( \([^)]*\))?(, else (block|warn|push|skip) \([^)]*\))?$' <<<"$ambiguous")"
      # What is ambiguous: native, a part of a source on its target nodes; CSV, the rows an item
      # sent after its cursor, at least one.
      if [ "$FORMAT" = native ]; then
        echo "ambiguousNamed: $(bool grep -qE '^  [^ ]+ part [^ ]+ of [^ ]+ on [^ ]+: ' <<<"$ambiguous")"
      else
        echo "ambiguousNamed: $(bool grep -qE '^  [^ ]+ rows (after [^ ]+ record [0-9]+|from its first row) up to ([^ ]+ )?record [0-9]+ \([1-9][0-9]* row\(s\) sent\): ' <<<"$ambiguous")"
      fi
      echo "progressUnchanged: $(bool test "$before" = "$(progress_digest "$progress")")"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  resumed)
    # The resumed import runs until an attempt succeeds (run_once); a retry after it only repeats
    # the checks. Before the first attempt, the number of complete items is kept for the checks.
    if [ ! -f "$CASE_DIR/finished" ]; then
      if [ ! -f "$CASE_DIR/complete-before" ]; then
        probe import "$(import_progress)"
        echo "$P_COMPLETE" >"$CASE_DIR/complete-before"
      fi
      run_once --resume --verify
      touch "$CASE_DIR/finished"
    fi
    resumed_items="$(grep -oE '^RESUMED +[0-9]+ of [0-9]+ work item' "$CASE_DIR/stdout" | grep -oE '[0-9]+' | head -n 1 || true)"
    probe import "$(import_progress)"
    {
      echo "exitCode: 0"
      echo "resumedLine: $(bool test -n "$resumed_items")"
      echo "resumedTheItemsDone: $(bool test "$resumed_items" = "$(cat "$CASE_DIR/complete-before")")"
      echo "verifyOk: $(bool grep -qE '^VERIFY +ok' "$CASE_DIR/stdout")"
      # A group checked partially is counted once on the VERIFY line; with none it is a plain
      # "VERIFY ok". Partially checked are the trace groups whose row counts sample spans in
      # merges.
      not_fully="$(grep -oE '^VERIFY +ok \([0-9]+ group\(s\) not fully checked\)' "$CASE_DIR/stdout" | grep -oE '[0-9]+' || true)"
      partial="$({
        sed -nE 's#^  - trace ([^/ ]+)/[^ ]+ checked partially: .*#\1#p' "$CASE_DIR/stdout"
      } | sort -u | grep -c . || true)"
      echo "partialCounted: $(bool test "${not_fully:-0}" = "$partial")"
      echo "nothingLost: $(bool test "$P_ST_ONCE" -gt 0 -a "$P_ST_PUSHED" -ge "$P_ST_ONCE")"
      echo "duplicatesBounded: $(bool test "$P_ST_BOUNDS_OK" = true -a "$((P_ST_PUSHED - P_ST_ONCE))" -le "$P_ST_DUPLICATES")"
      echo "progressKept: $P_PROGRESS"
      echo "progressLine: $(bool grep -qF "progress: $(import_progress) (kept after the import" "$CASE_DIR/stdout")"
      echo "cursorRemoved: $(bool test ! -e "$(import_side "$(import_progress)" cursor)")"
      echo "allComplete: $(bool test "$P_PLAN_ITEMS" -gt 0 -a "$P_COMPLETE" -eq "$P_PLAN_ITEMS")"
      echo "nothingInFlight: $(bool test "$P_INFLIGHT" -eq 0)"
    } >"$report"
    echo "stream and trace rows: pushed $P_ST_PUSHED, one import $P_ST_ONCE, possible duplicates $P_ST_DUPLICATES" >&2
    finish_case "$case_name" "$report" true
    ;;

  again)
    progress="$(import_progress)"
    before="$(progress_digest "$progress")"
    rc=0
    import_cmd >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" || rc=$?
    vrc=0
    import_cmd --verify >"$CASE_DIR/verify.stdout" 2>"$CASE_DIR/verify.stderr" || vrc=$?
    tail -n 40 "$CASE_DIR/stderr" "$CASE_DIR/verify.stderr" >&2
    already='^already imported into this cluster at [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2}) \(liaisons [^,]+, data nodes [^)]+\); nothing is pushed'
    {
      echo "exitCode: $rc"
      echo "alreadyImported: $(bool grep -qE "$already" "$CASE_DIR/stdout")"
      echo "nothingPushed: $(bool test -z "$(grep -hE '^(IMPORTED|RESUMED) ' "$CASE_DIR/stdout" "$CASE_DIR/verify.stdout" || true)")"
      echo "askedNothing: $(bool test -z "$(grep -h 'add --resume' "$CASE_DIR/stderr" "$CASE_DIR/verify.stderr" || true)")"
      echo "verifySkipped: $(bool grep -q '^NOTE verify skipped: ' "$CASE_DIR/stdout")"
      echo "verifyExitCode: $vrc"
      echo "verifyAlreadyImported: $(bool grep -qE "$already" "$CASE_DIR/verify.stdout")"
      echo "verifyAgainstBaseline: $(bool grep -q '^verify: against the baseline the first run recorded' "$CASE_DIR/verify.stdout")"
      echo "verifyOk: $(bool grep -qE '^VERIFY +ok' "$CASE_DIR/verify.stdout")"
      echo "progressUnchanged: $(bool test "$before" = "$(progress_digest "$progress")")"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  roundtrip)
    freeze_reference "$src"
    rm -rf "$CASE_DIR/B"
    export_rc=0
    bydbctl data export --nodes "$DST_GRPC" --output "$CASE_DIR/B" "${ALL_CATALOGS[@]}" </dev/null >"$CASE_DIR/B.stdout" 2>"$CASE_DIR/B.log" || export_rc=$?
    for id in $(grep -ohE 'release-session --id [0-9a-f]{32}' "$CASE_DIR/B.stdout" "$CASE_DIR/B.log" | awk '{print $3}' | sort -u); do
      release_session "$id" "$DST_GRPC"
    done
    {
      echo "exportExitCode: $export_rc"
      if [ "$export_rc" -eq 0 ]; then
        compare_rc=0
        export-checker compare "$ARTIFACT_A" "$CASE_DIR/B" 2>"$CASE_DIR/compare.log" || compare_rc=$?
        echo "compareExitCode: $compare_rc"
        queries_rc=0
        export-checker queries "$src" "$DST_GRPC" "$ARTIFACT_A" 2>"$CASE_DIR/queries.log" || queries_rc=$?
        echo "queriesExitCode: $queries_rc"
      fi
    } >"$report"
    tail -n 50 "$CASE_DIR/B.log" "$CASE_DIR/compare.log" "$CASE_DIR/queries.log" >&2 2>/dev/null || true
    finish_case "$case_name" "$report" test "$(grep -cE '^(compareExitCode|queriesExitCode): 0$' "$report" || true)" = 2
    ;;

  reconcile)
    resumed="$RESUME_WORK/import-resumed/stdout"
    grep -E '^reconcile ' "$resumed" >&2 || true
    {
      echo "decisions: $(bool grep -qE '^reconcile .* on [^ ]+: (landed|not landed) \(' "$resumed")"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  reset)
    progress="$(import_progress)"
    # Stage 1: delete every group the import wrote into and wait until each is gone. A retry
    # deletes what is left and waits again.
    if [ ! -f "$CASE_DIR/deleted.done" ]; then
      probe import "$progress"
      if [ -z "$P_GROUPS" ]; then
        echo "the progress file names no group" >&2
        exit 1
      fi
      for g in $P_GROUPS; do
        if dst_group get -g "$g" >/dev/null 2>&1; then
          dst_group delete -g "$g" --force >&2 || true
        fi
      done
      gone() {
        local g
        for g in $P_GROUPS; do
          if dst_group get -g "$g" >/dev/null 2>&1; then
            return 1
          fi
        done
      }
      deadline=$((SECONDS + 300))
      until gone; do
        if [ "$SECONDS" -ge "$deadline" ]; then
          echo "the groups $P_GROUPS are still on the destination" >&2
          exit 1
        fi
        sleep 2
      done
      touch "$CASE_DIR/deleted.done"
    fi
    # Stage 2: the finished import run again, and as a dry run, over the reset destination.
    before="$(progress_digest "$progress")"
    rc=0
    import_cmd >"$CASE_DIR/stdout" 2>"$CASE_DIR/stderr" || rc=$?
    dry_rc=0
    import_cmd --dry-run >"$CASE_DIR/dry.stdout" 2>"$CASE_DIR/dry.stderr" || dry_rc=$?
    tail -n 40 "$CASE_DIR/stderr" "$CASE_DIR/dry.stderr" >&2
    {
      echo "exitCode: $rc"
      echo "looksReset: $(bool grep -q 'the target looks reset or is another cluster' "$CASE_DIR/stderr")"
      echo "lacksGroups: $(bool grep -qE 'but the target lacks [^;]*group/' "$CASE_DIR/stderr")"
      echo "checkpointHint: $(bool grep -qF 'to import this artifact into another cluster give --checkpoint <another path>' "$CASE_DIR/stderr")"
      echo "restartHint: $(bool grep -qF 'to import it again into this cluster give --restart' "$CASE_DIR/stderr")"
      echo "notAlreadyImported: $(bool test -z "$(grep -hE '^(already imported|IMPORTED|RESUMED) ' "$CASE_DIR/stdout" "$CASE_DIR/dry.stdout" || true)")"
      echo "dryRunExitCode: $dry_rc"
      echo "dryRunLooksReset: $(bool grep -q 'the target looks reset or is another cluster' "$CASE_DIR/dry.stderr")"
      echo "progressUnchanged: $(bool test "$before" = "$(progress_digest "$progress")")"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  restart)
    progress="$(import_progress)"
    # The fresh import runs until an attempt succeeds (run_once): an attempt that failed after it
    # kept the progress file aside fails the case for good, so E is never imported a third time.
    # Before the first attempt, the checksum of the progress file the resumed case finished is
    # kept, to find the .bak copy of it.
    if [ ! -f "$CASE_DIR/finished" ]; then
      if [ ! -f "$CASE_DIR/progress-before" ]; then
        cksum <"$progress" >"$CASE_DIR/progress-before"
      fi
      run_once --restart --verify=false
      touch "$CASE_DIR/finished"
    fi
    kept=false
    for bak in "$progress".[0-9]*Z.bak; do
      if [ -f "$bak" ] && [ "$(cksum <"$bak")" = "$(cat "$CASE_DIR/progress-before")" ]; then
        kept=true
      fi
    done
    probe import "$progress"
    {
      echo "exitCode: 0"
      echo "restartWarned: $(bool grep -qE '^WARN --restart: .+ is kept as .+\.bak and the import starts over; ' "$CASE_DIR/stdout")"
      echo "progressBak: $kept"
      echo "planBak: $(bool exists "$(import_side "$progress" plan).[0-9]*Z.bak")"
      echo "notResumed: $(bool test -z "$(grep -E '^(RESUMED |already imported )' "$CASE_DIR/stdout" || true)")"
      echo "imported: $(bool grep -qE '^IMPORTED ' "$CASE_DIR/stdout")"
      echo "freshProgressFinished: $(bool test "$P_FINISHED" = true -a "$P_PLAN_ITEMS" -gt 0 -a "$P_COMPLETE" -eq "$P_PLAN_ITEMS")"
    } >"$report"
    finish_case "$case_name" "$report" true
    ;;

  *)
    echo "$usage" >&2
    exit 2
    ;;
esac
