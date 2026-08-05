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
FIXTURE=${FIXTURE:-"$ROOT/.scratch/trace-pipeline-merge-performance/generated-fixture-ticket05"}
OUTPUT=${OUTPUT:-"$ROOT/.scratch/trace-pipeline-merge-performance/baseline-report"}
IMAGE=${IMAGE:-golang:1.25.12}
DATA_CPUS=${DATA_CPUS:-0-3}
CONTROLLER_CPU=${CONTROLLER_CPU:-4}
SWEEP_RATES=${SWEEP_RATES:-"1000 2000 3000 4000"}
REPETITIONS=${REPETITIONS:-5}
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
PY
)
FIXTURE_SHA=${META[0]}
SCHEDULE_SHA=${META[1]}
EXPECTED_ROWS=${META[2]}
WRITE_COUNT=${META[3]}
FIXTURE_INPUT_BYTES=${META[4]}

mkdir -p "$OUTPUT" "$(dirname "$BIN")"
go build -o "$BIN" "$ROOT/banyand/cmd/trace-merge-benchmark"
BINARY_SHA=$(sha256sum "$BIN" | awk '{print $1}')
git -C "$ROOT" diff --binary HEAD -- . ':(exclude).scratch' >"$OUTPUT/source.patch"
SOURCE_PATCH_SHA=$(sha256sum "$OUTPUT/source.patch" | awk '{print $1}')
if git -C "$ROOT" status --porcelain --untracked-files=all | grep '^??' | grep -v '^?? \.scratch/' >/dev/null; then
  echo "untracked source files are not captured by source.patch" >&2
  exit 2
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
  docker run --rm -v "$FIXTURE":/fixture:ro -v "$run_dir":/run "$IMAGE" bash -c \
    'find /run -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true; mkdir -p /run/source /run/data/sidx/latency /run/data/sidx/start_time /run/profiles; cp -a /fixture/shard/. /run/source/; chmod 0777 /run'
  local attribution_flag=()
  if [[ "$attribution" == true ]]; then attribution_flag=(--attribution); fi
  docker run -d --name "$container" --cpuset-cpus="$DATA_CPUS" --cpus=4 --memory=8g --memory-swap=8g --pids-limit=512 \
    -e GOMAXPROCS=4 -v "$ROOT":/workspace:ro -v "$run_dir":/run -w /workspace "$IMAGE" \
    /workspace/.scratch/trace-pipeline-merge-performance/bin/trace-merge-benchmark serve \
    --root=/run/data --socket=/run/control.sock --output=/run/report.json --profiles=/run/profiles \
    --commit="$COMMIT" --fixture-sha256="$FIXTURE_SHA" --schedule-sha256="$SCHEDULE_SHA" --run-id="$run_id" \
    --mode="$mode" --acceleration="$acceleration" --expected-rows="$EXPECTED_ROWS" --max-input-part-id="$WRITE_COUNT" \
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
  active_container=""
}

mkdir -p "$OUTPUT/sweep"
for rate in $SWEEP_RATES; do
  run_once "sweep-$rate" throughput "$rate" false
done

FROZEN=$(python3 - "$OUTPUT" $SWEEP_RATES <<'PY'
import json,math,os,sys
root=sys.argv[1]; rates=[float(x) for x in sys.argv[2:]]; points=[]; best=None
for rate in rates:
    p=os.path.join(root,'sweep-'+('%g'%rate),'report.json'); r=json.load(open(p)); l=sorted(x['lagNanos'] for x in r['status']); p95=l[min(len(l)-1,math.ceil(len(l)*.95)-1)]
    target=86400/rate; sustainable=r['correct'] and p95/1e9 <= max(.5,target*.05) and r['primary']['drainNanos']/1e9 <= max(1,target*.20)
    points.append({'acceleration':rate,'wallNanos':r['primary']['wallNanos'],'drainNanos':r['primary']['drainNanos'],'p95LagNanos':p95,'sustainable':sustainable})
    if sustainable: best=rate if best is None else max(best,rate)
if best is None:
    raise SystemExit('no sustainable acceleration found in sweep')
open(os.path.join(root,'sweep.json'),'w').write(json.dumps(points,indent=2))
print(best*.70)
PY
)

run_once warmup throughput "$FROZEN" false
run_once serial-1 serial "$FROZEN" true
for repetition in $(seq 1 "$REPETITIONS"); do
  run_once "throughput-$repetition" throughput "$FROZEN" false
done

python3 - "$OUTPUT" "$COMMIT" "$BINARY_SHA" "$SOURCE_PATCH_SHA" "$FIXTURE_SHA" "$SCHEDULE_SHA" "$FIXTURE_INPUT_BYTES" "$FROZEN" "$REPETITIONS" <<'PY'
import datetime,json,os,sys
root,commit,binary,source_patch,fixture,schedule,input_bytes,frozen,reps=sys.argv[1:]; sweep=json.load(open(os.path.join(root,'sweep.json')))
load=lambda name: json.load(open(os.path.join(root,name,'report.json')))
suite={'generatedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'commit':commit,'binarySHA256':binary,'sourcePatchSHA256':source_patch,
       'fixtureSHA256':fixture,'scheduleSHA256':schedule,
       'fixtureInputBytes':int(input_bytes),
       'maximumSustainableAcceleration':max((x['acceleration'] for x in sweep if x['sustainable']),default=sweep[0]['acceleration']),
       'frozenAcceleration':float(frozen),'sweep':sweep,'serialRuns':[load('serial-1')],
       'throughputRuns':[load('throughput-'+str(i)) for i in range(1,int(reps)+1)],'oneShardOnly':True,'preRollDiscovered':False,
       'ledgerVerified':False}
open(os.path.join(root,'suite.json'),'w').write(json.dumps(suite,indent=2))
PY

"$BIN" render --suite="$OUTPUT/suite.json" --output="$OUTPUT/report.html"
echo "baseline report: $OUTPUT/report.html"
python3 - "$OUTPUT/suite.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); runs=d['serialRuns']+d['throughputRuns']
ready=(d['preRollDiscovered'] and d['ledgerVerified'] and len(d['throughputRuns']) >= 5 and
       all(r['correct'] and r['samplingCalls'] == 0 for r in runs) and
       all(r['hotMerges'] >= 10 and r['matureMerges'] >= 10 for r in d['throughputRuns']))
if not ready:
    raise SystemExit('baseline report is HOLD; inspect failed gates in report.html')
PY
