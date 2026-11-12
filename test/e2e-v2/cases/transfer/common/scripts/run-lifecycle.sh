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

# Runs the lifecycle agent once inside the hot node container (it snapshots the local node
# and reads the snapshot directories, so it must share the node's filesystem) to migrate every
# segment older than the hot-stage TTL to the warm node. Without --schedule the agent exits
# after one run; a partial migration makes it exit non-zero.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"

hot="$(container_of data-hot1)"
if ! docker exec "$hot" /lifecycle \
    --grpc-addr 127.0.0.1:17912 \
    --node-labels type=hot \
    --node-discovery-mode=file --node-discovery-file-path=/etc/banyandb/nodes.yaml \
    --progress-file /tmp/lifecycle-progress.json \
    --report-dir /tmp/lifecycle-report; then
  echo "lifecycle migration failed" >&2
  docker exec "$hot" sh -c 'for f in /tmp/lifecycle-report/*; do echo "== $f"; cat "$f"; done' >&2 || true
  exit 1
fi
docker exec "$hot" sh -c 'for f in /tmp/lifecycle-report/*; do echo "== $f"; head -60 "$f"; done' || true
warm="$(container_of data-warm1)"
echo "warm node segments after migration:"
list_segments "$warm"
