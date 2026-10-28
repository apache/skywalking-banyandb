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


# Import case 3, the full round trip. After the real import of import-dryrun.sh, exports the whole
# destination into a new artifact B and compares it with artifact A as logical rows over the groups
# of A (export-checker compare: keys, deduplicated replicas, newest measure versions, every cell,
# and the rows per catalog/group/stage, _top_n_result included), then runs the query-level checks
# against both clusters (export-checker queries: by entity, by an indexed tag, index-mode, traces
# ordered and by id, and a _top_n_result query on the destination). The output is one yaml
# document for expected/import-roundtrip.yaml. Every attempt re-exports B, so the case is retried
# until the imported rows are visible.
#
#   import-roundtrip.sh <source liaison host:grpc-port>
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"
# shellcheck source=import-common.sh
source "$DIR/import-common.sh"

src="${1:?usage: import-roundtrip.sh <source liaison host:grpc-port>}"
replay_failed
if [ ! -f "$IMPORT_WORK/import.done" ]; then
  echo "the real import has not succeeded; run import-dryrun.sh first" >&2
  exit 1
fi

work="$IMPORT_WORK/roundtrip"
mkdir -p "$work"
export_rc=0
export_all "$DST_GRPC" "$work/B" || export_rc=$?
echo "exportExitCode: $export_rc"
if [ "$export_rc" -ne 0 ]; then
  exit 1
fi

compare_rc=0
export-checker compare "$ARTIFACT_A" "$work/B" 2>"$work/compare.log" || compare_rc=$?
tail -n 50 "$work/compare.log" >&2
echo "compareExitCode: $compare_rc"

queries_rc=0
export-checker queries "$src" "$DST_GRPC" "$ARTIFACT_A" 2>"$work/queries.log" || queries_rc=$?
cat "$work/queries.log" >&2
echo "queriesExitCode: $queries_rc"
