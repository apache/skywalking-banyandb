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

# Import case 5, a target of another topology. After the round trip, copies the artifact under
# test (ARTIFACT_IN) with the shard number of sw_records doubled in its manifest (the source
# topology the importer compares with the target) and dry-runs its import into the destination,
# which holds sw_records with the real shard number. CSV rows are routed by the target's own
# shards, so the CSV dry run passes (exit 0); native parts are placed as they are, so the native
# dry run must be refused before anything is written (exit 2) by the TOPOLOGY gate, naming the
# group. A dry run writes nothing, so the case is safe to retry. The output is one yaml document
# for expected/topology.yaml.
#
#   topology.sh
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"
# shellcheck source=import-common.sh
source "$DIR/import-common.sh"

replay_failed
if [ ! -f "$IMPORT_WORK/import.done" ]; then
  echo "the real import has not succeeded; run import-dryrun.sh first" >&2
  exit 1
fi
work="$IMPORT_WORK/topology"
rm -rf "$work"
mkdir -p "$work"
cp -r "$ARTIFACT_IN" "$work/artifact"
python3 - "$work/artifact/manifest.json" <<'EOF'
import json, sys
path = sys.argv[1]
with open(path) as f:
    m = json.load(f)
for g in m["groups"]:
    if g["name"] == "sw_records":
        for st in g["stages"]:
            st["shardNum"] = st["shardNum"] * 2
with open(path, "w") as f:
    json.dump(m, f, indent=2)
EOF
rc=0
bydbctl data import --dry-run --nodes "$DST_GRPC" --input "$work/artifact" --force-ttl --include-schema=false \
  >"$work/dryrun.stdout" 2>"$work/dryrun.stderr" || rc=$?
tail -n 60 "$work/dryrun.stdout" "$work/dryrun.stderr" >&2
echo "dryRunExitCode: $rc"
echo "topologyRefused: $(bool grep -q 'TOPOLOGY' "$work/dryrun.stdout")"
# The gate detail line is "<catalog>/<group>/<stage>/...: TOPOLOGY <reason>"; every decision
# table row names sw_records, so only that line shows the refusal is for the right group.
echo "namesGroup: $(bool grep -qE '/sw_records/[^:]*: TOPOLOGY ' "$work/dryrun.stdout")"
