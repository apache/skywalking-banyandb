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

# Shared state and helpers of the resume cases (resume-export.sh, resume-import.sh). Source after
# common.sh, do not execute. FORMAT (csv or native) is the format of the export under test.
#
# The cases run in order and build on each other: the export is interrupted, killed and resumed
# in RESUME_WORK/E, then E is imported into the destination, interrupted, killed and resumed. An
# infra-e2e retry re-runs a failing case, so every case that passed leaves <case>.done and its
# report <case>.out in RESUME_WORK (a fixed directory the setup empties once per run) and replays
# that report; a case only runs once the case before it passed. The steps that change state for
# good (the export finishing, the import stages) leave their own markers, so a retry repeats the
# checks without repeating the step.

require_format
RESUME_WORK="${SW_INFRA_E2E_LOG_DIR:-/tmp}/transfer-resume-$FORMAT"
# E is the export that is interrupted, resumed, and then imported; L the export whose session is
# lost; A a CSV export of the source, the reference for the CSV checks.
EXPORT_E="$RESUME_WORK/E"
EXPORT_L="$RESUME_WORK/L"
ARTIFACT_A="$RESUME_WORK/A"
# Slow enough that an export of the source takes many seconds per node, so that it can be
# stopped with units done and units left. The source is small (a few hundred KiB of native parts
# in a handful of units) and a limiter's burst is one second's worth, so the rate must be low.
EXPORT_RATE=4KiB/s
DST_GRPC=127.0.0.1:27912
DST_HTTP=127.0.0.1:27913

# BG_PID is the bydbctl a case runs in the background; it is killed when the case exits early.
BG_PID=
trap 'if [ -n "$BG_PID" ]; then kill -KILL "$BG_PID" 2>/dev/null || true; fi' EXIT

# The case order; each needs the one before it. import-reconcile runs in the native suite only:
# the CSV suite skips it. import-reset deletes the imported groups on the destination, so it comes
# after every check of what the import stored, and import-restart then imports E afresh.
RESUME_CASES=(export-sigint export-kill export-summary export-binding export-resumed export-session-lost
  import-interrupt import-summary import-resumed import-again import-roundtrip import-reconcile import-reset import-restart)

# bool prints true when the command succeeds and false otherwise.
bool() {
  if "$@"; then echo true; else echo false; fi
}

# yaml_lines prints the lines of file $1 that are neither comments nor blank.
yaml_lines() {
  grep -vE '^[[:space:]]*(#|$)' "$1" || true
}

# begin_case replays the report of case $1 and exits when it passed already, replays its failure
# record and fails when it failed for good (fail_for_good), fails when the case before it has not
# passed, and otherwise creates RESUME_WORK/$1 and sets CASE_DIR to it and CASE_NAME to $1.
begin_case() {
  local i prev=
  mkdir -p "$RESUME_WORK"
  if [ -f "$RESUME_WORK/$1.done" ]; then
    cat "$RESUME_WORK/$1.out"
    exit 0
  fi
  if [ -f "$RESUME_WORK/$1.failed" ]; then
    cat "$RESUME_WORK/$1.failed"
    echo "case $1 failed for good in an earlier attempt; its logs are in $RESUME_WORK/$1" >&2
    exit 1
  fi
  for i in "${RESUME_CASES[@]}"; do
    if [ "$i" = "$1" ]; then
      break
    fi
    if [ "$i" = import-reconcile ] && [ "$FORMAT" != native ]; then
      continue
    fi
    prev="$i"
  done
  if [ -n "$prev" ] && [ ! -f "$RESUME_WORK/$prev.done" ]; then
    echo "case $prev has not passed; case $1 cannot run yet" >&2
    exit 1
  fi
  CASE_DIR="$RESUME_WORK/$1"
  CASE_NAME="$1"
  mkdir -p "$CASE_DIR"
}

# fail_for_good prints its arguments, one report line each, records them as the failure of the
# current case and exits 1: the case failed in a way a retry cannot repair (the destination or
# the progress file changed for good), so every later attempt replays this record, which no
# expectation matches, instead of running the case again.
fail_for_good() {
  printf '%s\n' "$@" >"$RESUME_WORK/$CASE_NAME.failed"
  cat "$RESUME_WORK/$CASE_NAME.failed"
  echo "case $CASE_NAME failed for good; its logs are in $CASE_DIR" >&2
  exit 1
}

# next_attempt prints the number of the next attempt of the current case: 1, then one more for
# every <name>.<n> file of $1 in CASE_DIR. Every attempt keeps its own output files.
next_attempt() {
  local n=1
  while [ -e "$CASE_DIR/$1.$n" ]; do
    n=$((n + 1))
  done
  echo "$n"
}

# progress_digest prints a digest of the progress file $1 and of every file next to it named
# like it (the -cursor and -plan side files, the .bak copies), but its lock: whatever a
# run writes to any of them changes the digest.
progress_digest() {
  local f
  for f in "$1" "$1".* "${1%.json}"-*; do
    if [ ! -f "$f" ] || [ "$f" = "$1.lock" ]; then
      continue
    fi
    printf '%s ' "${f##*/}"
    cksum <"$f"
  done
}

# finish_case prints the report $2 of case $1 and records the case as passed when the remaining
# arguments (a command) succeed and the report equals expected/resume-$1.yaml, when there is one:
# a case recorded as passed only replays its report on a retry, so it must not pass with a line
# the verify step then rejects.
finish_case() {
  local name="$1" report="$2" expected
  shift 2
  expected="$(dirname "${BASH_SOURCE[0]}")/../expected/resume-$name.yaml"
  cat "$report"
  if [ -f "$expected" ] && [ "$(yaml_lines "$expected")" != "$(yaml_lines "$report")" ]; then
    echo "the report of $name differs from $expected" >&2
    return 0
  fi
  if "$@"; then
    cp "$report" "$RESUME_WORK/$name.out"
    touch "$RESUME_WORK/$name.done"
  fi
}

# probe evaluates `export-checker resume-state <args>` (export, files, listing or import state)
# into P_* variables.
probe() {
  local out
  out="$(export-checker resume-state "$@")" || return 1
  eval "$out"
}

# wait_for polls the shell condition $2 every 0.2s for up to $1 seconds while process $3 runs;
# it fails when the time is up or the process ended first.
wait_for() {
  local deadline=$((SECONDS + $1)) cond="$2" pid="$3"
  while [ "$SECONDS" -lt "$deadline" ]; do
    if eval "$cond"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "the process ended before: $cond" >&2
      return 1
    fi
    sleep 0.2
  done
  echo "timed out after $1s waiting for: $cond" >&2
  return 1
}

# stop_process sends signal $1 to process $2 and sets STOP_RC to its exit code once it ended
# (not through $(...): a subshell cannot wait for the script's own background job). bash ignores
# SIGINT in a background job of a script, but Go installs its handler anyway once bydbctl asks
# for the signal. A process still running 60s after the signal is killed, and STOP_RC is 124.
stop_process() {
  local deadline=$((SECONDS + 60))
  STOP_RC=0
  kill "-$1" "$2" 2>/dev/null || true
  while kill -0 "$2" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do
    sleep 0.2
  done
  if kill -0 "$2" 2>/dev/null; then
    echo "process $2 still ran 60s after SIG$1; killed" >&2
    kill -KILL "$2" 2>/dev/null || true
    wait "$2" || true
    STOP_RC=124
  else
    wait "$2" || STOP_RC=$?
  fi
  BG_PID=
}

# release_session releases export session $1 on the source behind liaison $2, if there is one.
release_session() {
  if [ -n "$1" ]; then
    bydbctl data release-session --id "$1" --nodes "$2" >&2 || true
  fi
}

# export_cmd runs bydbctl data export of every catalog of the source (liaison $1) into $2 in
# FORMAT; further arguments are passed on. stdin is closed, so a resume never asks. Started in
# the background, call it with EXEC=exec: a background function runs in a subshell, and a signal
# sent to $! must reach bydbctl, not that subshell (which, as a background job, ignores SIGINT).
export_cmd() {
  ${EXEC:-} bydbctl data export --nodes "$1" --output "$2" --format "$FORMAT" "${ALL_CATALOGS[@]}" "${@:3}" </dev/null
}

# freeze_reference exports the source (liaison $1) as CSV into ARTIFACT_A once per run.
freeze_reference() {
  local rc=0
  if [ -f "$RESUME_WORK/reference.done" ]; then
    return 0
  fi
  rm -rf "$ARTIFACT_A"
  bydbctl data export --nodes "$1" --output "$ARTIFACT_A" "${ALL_CATALOGS[@]}" </dev/null >"$ARTIFACT_A.stdout" 2>"$ARTIFACT_A.log" || rc=$?
  for id in $(grep -ohE 'release-session --id [0-9a-f]{32}' "$ARTIFACT_A.stdout" "$ARTIFACT_A.log" | awk '{print $3}' | sort -u); do
    release_session "$id" "$1"
  done
  if [ "$rc" -ne 0 ]; then
    echo "the CSV reference export exited $rc:" >&2
    tail -n 50 "$ARTIFACT_A.log" >&2
    return 1
  fi
  touch "$RESUME_WORK/reference.done"
}

# import_side prints the side file $2 (cursor or plan) of the progress file $1:
# <name>-<side>.json, <name> being the file without .json.
import_side() {
  echo "${1%.json}-$2.json"
}

# import_progress prints the import progress file of E: the default one, <E>/.import-progress.json,
# so the cases also cover where the importer puts it. Its side files are <name>-cursor.json and
# <name>-plan.json (the plan and the baseline), and its lock <file>.lock.
import_progress() {
  echo "$EXPORT_E/.import-progress.json"
}

# import_cmd runs bydbctl data import of E into the destination; further arguments are passed
# on. stdin is closed, so a resume never asks. In the background, call it with EXEC=exec (see
# export_cmd).
import_cmd() {
  ${EXEC:-} bydbctl data import --nodes "$DST_GRPC" --input "$EXPORT_E" --force-ttl "$@" </dev/null
}

# dst_group runs bydbctl group <args> against the destination liaison's HTTP address.
dst_group() {
  bydbctl group "$@" --config=- -a "http://$DST_HTTP" </dev/null
}

# import_slow are the flags that slow an import of E down so it can be stopped half way. Native:
# parts by bytes; the part limiter's burst is one chunk, and all the parts of E fit in less than
# the default 1MiB chunk, so the smallest chunk keeps the burst well below what E holds. CSV: rows
# by rows in small batches, folded into the progress file every 8 batches, with up to 4 batches
# in flight: the importer reads the acknowledgements only once 4 batches are in flight, so while
# the rate limiter holds the next batch back, the cursor side file records rows sent after the
# cursor and not acknowledged, which a kill leaves as the ambiguous window.
#
# The work items are pushed in catalog name order, so the native stream and trace parts the kill
# stage of import-interrupt waits for come after every measure and property part: that stage
# derives its part rate from the bytes of those parts instead (kill_slow in resume-import.sh).
import_slow() {
  if [ "$FORMAT" = native ]; then
    echo "--part-rate-limit 4KiB/s --part-chunk-size 64KiB --parallelism 1"
  else
    echo "--write-rate-limit 50/s --write-batch-size 10 --write-inflight-batches 4 --checkpoint-every-batches 8 --parallelism 1"
  fi
}
