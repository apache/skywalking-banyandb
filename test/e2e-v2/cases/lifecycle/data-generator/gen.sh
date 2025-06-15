#!/usr/bin/env bash
# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# determine script directory and relevant paths
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$DIR/../../../script/env"
COMPOSE_FILE="$DIR/docker-compose.yml"

# load environment
export $(grep -v '^#' "$ENV_FILE" | xargs)

# start services
docker compose -f "$COMPOSE_FILE" up -d
CONTAINER_ID=$(docker compose -f "$COMPOSE_FILE" ps -q banyandb)

echo "⌛ monitoring segment files..."
found=false
for i in {1..60}; do
  if docker exec "$CONTAINER_ID" sh -c '[ -n "$(ls /tmp/measure-data/measure/data/metricsDay/seg* 2>/dev/null)" ]'; then
    echo "✅ found segment files"
    sleep 180
    # copy out data
    docker cp "$CONTAINER_ID":/tmp "$DIR/tmp"
    docker cp "$CONTAINER_ID":/tmp/measure-data/measure/data/metadata "$DIR/tmp"
    found=true
    break
  else
    echo "⏳ didn't find segment files (retry $i/60)"
    sleep 10
  fi
done

if $found; then
  echo "✅ segment files copied to $DIR"
else
  echo "❌ segment files not found"
  exit 1
fi

docker compose -f "$COMPOSE_FILE" down -v