#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to you under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
FIXTURE=${FIXTURE:-"$ROOT/.scratch/trace-pipeline-merge-performance/generated-fixture-2x-v2-timestamps"}
OUTPUT=${OUTPUT:-"$ROOT/.scratch/trace-pipeline-merge-performance/baseline-report"}
IMAGE=${IMAGE:-golang:1.25.12}
DATA_CPUS=${DATA_CPUS:-0-1}
DATA_CPU_LIMIT=${DATA_CPU_LIMIT:-2}
DATA_MEMORY=${DATA_MEMORY:-4g}
DATA_GOMAXPROCS=${DATA_GOMAXPROCS:-2}
CONTROLLER_CPU=${CONTROLLER_CPU:-2}
REPETITIONS=${REPETITIONS:-5}
PILOT_ACCELERATION=${PILOT_ACCELERATION:-}
CONTROLLED_ALTERNATING=${CONTROLLED_ALTERNATING:-1}
CONTROLLED_REPETITIONS=${CONTROLLED_REPETITIONS:-10}
CONTROLLED_SEED=${CONTROLLED_SEED:-"$OUTPUT/controlled-seed"}
DEFAULT_CONTROLLED_PLUGIN="$ROOT/.scratch/trace-pipeline-merge-performance/plugins/alwayskeepsampler.so"
CONTROLLED_PLUGIN=${CONTROLLED_PLUGIN:-"$DEFAULT_CONTROLLED_PLUGIN"}
FULL_PIPELINE=${FULL_PIPELINE:-disabled}
BIN="$ROOT/.scratch/trace-pipeline-merge-performance/bin/trace-merge-benchmark"
COMMIT=$(git -C "$ROOT" rev-parse HEAD)

readarray -t META < <(python3 - "$FIXTURE/fixture.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
print(d['coreManifest']['sha256'])
print(d['scheduleSHA256'])
print(d['rowCount'])
print(d['writeCount'])
print(d['coreCompressedBytes'] + sum(d['indexCompressedBytes'].values()))
print(d.get('writeIntensity', 1))
print(d['logicalLedgerSHA256']['core'])
print(d['logicalLedgerSHA256']['latency'])
print(d['logicalLedgerSHA256']['start_time'])
import datetime
day_start=datetime.datetime.fromisoformat(d['dayStart'].replace('Z','+00:00'))
day_start_ns=int(day_start.timestamp()*1_000_000_000)
print(day_start_ns-int(d['mergeGrace']))
print(day_start_ns+int(d['dayDuration'])+int(d['mergeGrace']))
PY
)
FIXTURE_SHA=${META[0]}
SCHEDULE_SHA=${META[1]}
EXPECTED_ROWS=${META[2]}
WRITE_COUNT=${META[3]}
FIXTURE_INPUT_BYTES=${META[4]}
WRITE_INTENSITY=${META[5]}
CORE_LEDGER_SHA=${META[6]}
LATENCY_LEDGER_SHA=${META[7]}
START_TIME_LEDGER_SHA=${META[8]}
SEGMENT_MIN_TIME_NANOS=${META[9]}
SEGMENT_MAX_TIME_NANOS=${META[10]}

mkdir -p "$OUTPUT" "$(dirname "$BIN")"
go build -o "$BIN" "$ROOT/banyand/cmd/trace-merge-benchmark"
if [[ ("$CONTROLLED_ALTERNATING" == "1" || "$FULL_PIPELINE" == "retain-all") && "$CONTROLLED_PLUGIN" == "$DEFAULT_CONTROLLED_PLUGIN" ]]; then
  mkdir -p "$(dirname "$CONTROLLED_PLUGIN")"
  go build -buildmode=plugin -o "$CONTROLLED_PLUGIN" "$ROOT/test/plugins/_alwayskeepsampler"
fi
BINARY_SHA=$(sha256sum "$BIN" | awk '{print $1}')
git -C "$ROOT" diff --binary HEAD -- . ':(exclude).scratch' >"$OUTPUT/source.patch"
SOURCE_PATCH_SHA=$(sha256sum "$OUTPUT/source.patch" | awk '{print $1}')
if git -C "$ROOT" status --porcelain --untracked-files=all -- . ':(exclude).scratch' | grep '^??' >/dev/null; then
  echo "untracked source files are not captured by source.patch" >&2
  exit 2
fi

# Phase 1 environment capture: record the Docker image digest, the source
# filesystem, the storage device, and the clone method so the suite documents
# the exact execution envelope used to collect the measurement.
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  docker pull "$IMAGE" >/dev/null
fi
IMAGE_DIGEST=$(docker image inspect --format '{{if .RepoDigests}}{{index .RepoDigests 0}}{{else}}{{.Id}}{{end}}' "$IMAGE")
DATA_FILESYSTEM=$(df -T "$OUTPUT" 2>/dev/null | awk 'NR==2 {print $2}' || true)
DATA_STORAGE_DEVICE=$(df --output=source "$OUTPUT" 2>/dev/null | tail -n1 | tr -d ' ' || true)
DATA_CLONE_METHOD="cp -a"
if [[ -n "$CONTROLLED_PLUGIN" && -f "$CONTROLLED_PLUGIN" ]]; then
  PLUGIN_SHA=$(sha256sum "$CONTROLLED_PLUGIN" | awk '{print $1}')
else
  PLUGIN_SHA=""
fi

active_container=""
cleanup() {
  if [[ -n "$active_container" ]]; then
    docker rm -f "$active_container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

run_once() {
  local run_id=$1 mode=$2 acceleration=$3 attribution=$4
  local run_dir="$OUTPUT/$run_id"
  local container="banyandb-trace-baseline-${run_id//[^a-zA-Z0-9_.-]/-}"
  active_container=$container
  docker rm -f "$container" >/dev/null 2>&1 || true
  mkdir -p "$run_dir"
  docker run --rm -v "$FIXTURE":/fixture:ro -v "$run_dir":/run "$IMAGE" bash -c '
    find /run -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
    mkdir -p /run/source /run/data/sidx/latency /run/data/sidx/start_time /run/profiles
    cp -a /fixture/shard/. /run/source/
    chmod 0777 /run'
  local attribution_flag=()
  if [[ "$attribution" == true ]]; then attribution_flag=(--attribution); fi
  local plugin_flag=()
  if [[ "$FULL_PIPELINE" == "retain-all" ]]; then
    plugin_flag=(--plugin=/workspace/${CONTROLLED_PLUGIN#"$ROOT/"} --plugin-sha256="$PLUGIN_SHA" \
      --segment-min-time-nanos="$SEGMENT_MIN_TIME_NANOS" --segment-max-time-nanos="$SEGMENT_MAX_TIME_NANOS")
  fi
  docker run -d --name "$container" --cpuset-cpus="$DATA_CPUS" --cpus="$DATA_CPU_LIMIT" --memory="$DATA_MEMORY" --memory-swap="$DATA_MEMORY" --pids-limit=512 \
    -e GOMAXPROCS="$DATA_GOMAXPROCS" -e IMAGE_DIGEST="$IMAGE_DIGEST" \
    -v "$ROOT":/workspace:ro -v "$run_dir":/run -w /workspace "$IMAGE" \
    /workspace/.scratch/trace-pipeline-merge-performance/bin/trace-merge-benchmark serve \
    --root=/run/data --socket=/run/control.sock --output=/run/report.json --profiles=/run/profiles \
    --commit="$COMMIT" --fixture-sha256="$FIXTURE_SHA" --schedule-sha256="$SCHEDULE_SHA" --run-id="$run_id" \
    --mode="$mode" --acceleration="$acceleration" --expected-rows="$EXPECTED_ROWS" --max-input-part-id="$WRITE_COUNT" \
    --expected-core-ledger="$CORE_LEDGER_SHA" --expected-latency-ledger="$LATENCY_LEDGER_SHA" \
    --expected-start-time-ledger="$START_TIME_LEDGER_SHA" \
    --image-digest="$IMAGE_DIGEST" --filesystem="$DATA_FILESYSTEM" --storage-device="$DATA_STORAGE_DEVICE" \
    --clone-method="$DATA_CLONE_METHOD" --binary-sha256="$BINARY_SHA" \
    "${plugin_flag[@]}" \
    "${attribution_flag[@]}" >"$run_dir/container.id"
  if ! docker run --rm --cpuset-cpus="$CONTROLLER_CPU" --cpus=1 --memory=1g --memory-swap=1g --pids-limit=128 \
    -v "$ROOT":/workspace:ro -v "$run_dir":/run -w /workspace "$IMAGE" \
    /workspace/.scratch/trace-pipeline-merge-performance/bin/trace-merge-benchmark drive \
    --socket=/run/control.sock --source=/run/source --data=/run/data \
    --schedule=/workspace/${FIXTURE#"$ROOT/"}/schedule.json --output=/run/report.json \
    --mode="$mode" --acceleration="$acceleration" --controller-cpu="$CONTROLLER_CPU" >"$run_dir/controller.log" 2>&1; then
    docker logs "$container" >"$run_dir/data-node.log" 2>&1 || true
    return 1
  fi
  docker logs "$container" >"$run_dir/data-node.log" 2>&1 || true
  docker stop -t 20 "$container" >/dev/null
  docker run --rm -v "$run_dir":/run "$IMAGE" chmod -R 0777 /run
  active_container=""
}

# run_controlled_once executes a single controlled mature-merge comparison run
# against the frozen controlled seed. Pipeline mode is either "disabled" or
# "retain-all"; the script alternates them to populate the Phase 1 stability
# gate.
run_controlled_once() {
  local run_id=$1 pipeline=$2
  local run_dir="$OUTPUT/$run_id"
  local container="banyandb-trace-controlled-${run_id//[^a-zA-Z0-9_.-]/-}"
  local seed_container="banyandb-trace-seed-${run_id//[^a-zA-Z0-9_.-]/-}"
  active_container=$container
  docker rm -f "$container" >/dev/null 2>&1 || true
  docker rm -f "$seed_container" >/dev/null 2>&1 || true
  mkdir -p "$run_dir"
  if [[ ! -f "$CONTROLLED_SEED/seed.json" ]]; then
    echo "controlled seed manifest $CONTROLLED_SEED/seed.json is missing; capture the seed first" >&2
    return 1
  fi
  docker run --rm -v "$CONTROLLED_SEED":/seed:ro -v "$run_dir":/run "$IMAGE" bash -c '
    mkdir -p /run/data/sidx/latency /run/data/sidx/start_time
    cp -a /seed/shard/. /run/data/
    cp /seed/seed.json /run/seed.json
    chmod 0777 /run'
  local plugin_flag=()
  local plugin_identity_flag=()
  if [[ "$pipeline" == "retain-all" ]]; then
    plugin_flag=(--plugin=/workspace/${CONTROLLED_PLUGIN#"$ROOT/"})
    plugin_identity_flag=(--plugin-sha256="$PLUGIN_SHA")
  fi
  docker run -d --name "$container" --cpuset-cpus="$DATA_CPUS" --cpus="$DATA_CPU_LIMIT" --memory="$DATA_MEMORY" --memory-swap="$DATA_MEMORY" --pids-limit=512 \
    -e GOMAXPROCS="$DATA_GOMAXPROCS" -e IMAGE_DIGEST="$IMAGE_DIGEST" \
    -v "$ROOT":/workspace:ro -v "$run_dir":/run -w /workspace "$IMAGE" \
    /workspace/.scratch/trace-pipeline-merge-performance/bin/trace-merge-benchmark run-controlled \
    --seed-manifest=/run/seed.json --data=/run/data --output=/run/report.json \
    --run-id="$run_id" --pipeline="$pipeline" --commit="$COMMIT" \
    --image-digest="$IMAGE_DIGEST" --filesystem="$DATA_FILESYSTEM" --storage-device="$DATA_STORAGE_DEVICE" \
    --clone-method="$DATA_CLONE_METHOD" --binary-sha256="$BINARY_SHA" \
    --profiles=/run/profiles \
    "${plugin_identity_flag[@]}" \
    "${plugin_flag[@]}" >"$run_dir/container.id"
  if ! docker wait "$container" >"$run_dir/wait.code"; then
    docker logs "$container" >"$run_dir/data-node.log" 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
    return 1
  fi
  if [[ $(cat "$run_dir/wait.code") != "0" ]]; then
    docker logs "$container" >"$run_dir/data-node.log" 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
    echo "controlled run $run_id exited with status $(cat "$run_dir/wait.code")" >&2
    return 1
  fi
  if [[ ! -s "$run_dir/report.json" ]]; then
    docker logs "$container" >"$run_dir/data-node.log" 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
    echo "controlled run $run_id produced no report" >&2
    return 1
  fi
  docker logs "$container" >"$run_dir/data-node.log" 2>&1 || true
  docker rm -f "$container" >/dev/null 2>&1 || true
  docker run --rm -v "$run_dir":/run "$IMAGE" chmod -R 0777 /run
  active_container=""
}

if [[ "$FULL_PIPELINE" != "disabled" && "$FULL_PIPELINE" != "retain-all" ]]; then
  echo "FULL_PIPELINE must be disabled or retain-all" >&2
  exit 1
fi

if [[ -n "$PILOT_ACCELERATION" ]]; then
  run_once pilot serial 1 false
  echo "pilot report: $OUTPUT/pilot/report.json"
  exit 0
fi

if [[ "$CONTROLLED_ALTERNATING" == "1" ]]; then
  if [[ ! -f "$CONTROLLED_SEED/seed.json" ]]; then
    echo "CONTROLLED_ALTERNATING=1 requires $CONTROLLED_SEED/seed.json; capture the seed first" >&2
    exit 1
  fi
  if (( CONTROLLED_REPETITIONS < 10 || CONTROLLED_REPETITIONS % 2 != 0 )); then
    echo "CONTROLLED_REPETITIONS must be an even number of at least 10 (five runs per pipeline mode)" >&2
    exit 1
  fi
  if [[ ! -f "$CONTROLLED_PLUGIN" ]]; then
    echo "controlled retain-all plugin $CONTROLLED_PLUGIN is missing" >&2
    exit 1
  fi
fi

run_once warmup serial 1 false
for repetition in $(seq 1 "$REPETITIONS"); do
  run_once "serial-$repetition" serial 1 false
done

controlled_reports=""
if [[ "$CONTROLLED_ALTERNATING" == "1" ]]; then
  for repetition in $(seq 1 "$CONTROLLED_REPETITIONS"); do
    if (( repetition % 2 == 1 )); then
      run_controlled_once "controlled-disabled-$repetition" disabled
    else
      run_controlled_once "controlled-retain-all-$repetition" retain-all
    fi
  done
  controlled_reports=$(python3 - "$OUTPUT" "$CONTROLLED_REPETITIONS" <<'PY'
import json,os,sys
root,reps=sys.argv[1],int(sys.argv[2])
out=[]
for i in range(1,reps+1):
  name='controlled-disabled-'+str(i) if i%2==1 else 'controlled-retain-all-'+str(i)
  report_path=os.path.join(root,name,'report.json')
  if os.path.isfile(report_path):
    out.append(json.load(open(report_path)))
print(json.dumps(out))
PY
)
fi

python3 - \
  "$OUTPUT" "$COMMIT" "$BINARY_SHA" "$SOURCE_PATCH_SHA" \
  "$FIXTURE_SHA" "$SCHEDULE_SHA" "$FIXTURE_INPUT_BYTES" \
  "$WRITE_INTENSITY" "$REPETITIONS" "$controlled_reports" <<'PY'
import datetime,json,os,sys
root,commit,binary,source_patch,fixture,schedule,input_bytes,intensity,reps,controlled_json=sys.argv[1:]
load=lambda name: json.load(open(os.path.join(root,name,'report.json')))
serial=[load('serial-'+str(i)) for i in range(1,int(reps)+1)]
suite={'generatedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'commit':commit,'binarySHA256':binary,'sourcePatchSHA256':source_patch,
       'fixtureSHA256':fixture,'scheduleSHA256':schedule,
       'fixtureInputBytes':int(input_bytes),'writeIntensity':int(intensity),
       'serialRuns':serial,'throughputRuns':[],
       'oneShardOnly':True,'preRollDiscovered':False,'ledgerVerified':all(r['ledgerVerified'] for r in serial)}
# Phase 1 logical write-amplification gate: median per-run WA across the five
# serial replays. The server populates run.LogicalWriteAmplification as the
# ratio of compressed core + secondary-index merge output bytes to the same
# merge events' selected input bytes, so the median is the correct aggregation against the design
# envelope ([0.5, 2.0] with a 1.0009 reference). Summing output bytes across
# runs (the previous approach) was ~5× the real WA for five serial replays
# of the same fixture.
wa_values=sorted(
  float(run.get('logicalWriteAmplification',0) or 0)
  for run in serial
  if float(run.get('logicalWriteAmplification',0) or 0) > 0
)
if wa_values:
  suite['logicalWriteAmplification']=wa_values[len(wa_values)//2]
else:
  suite['logicalWriteAmplification']=0.0
if controlled_json:
  try:
    suite['disabledEnabledAlternating']=json.loads(controlled_json)
  except json.JSONDecodeError:
    suite['disabledEnabledAlternating']=[]
open(os.path.join(root,'suite.json'),'w').write(json.dumps(suite,indent=2))
PY

"$BIN" render --suite="$OUTPUT/suite.json" --output="$OUTPUT/report.html"
echo "baseline report: $OUTPUT/report.html"
"$BIN" validate --suite="$OUTPUT/suite.json"
