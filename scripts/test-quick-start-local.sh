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

# Local test script for the quick-start workflow.
# Run this to verify the quick-start docker-compose setup without using act/GitHub.

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QUICK_START_DIR="$REPO_ROOT/docs/guides/quick-start"

cd "$REPO_ROOT"

echo "=== Free disk space ==="
df -h
if [ "${PRUNE_DOCKER:-0}" = "1" ]; then
  echo "PRUNE_DOCKER=1 set - running 'docker system prune -af --volumes'"
  docker system prune -af --volumes || true
else
  echo "Skipping 'docker system prune -af --volumes'. Set PRUNE_DOCKER=1 to enable full cleanup."
fi
df -h

echo "=== Pull images ==="
cd "$QUICK_START_DIR"
PULL_RETRIES=5
for i in $(seq 1 "$PULL_RETRIES"); do
  if docker compose pull; then
    break
  fi
  if [ "$i" -eq "$PULL_RETRIES" ]; then
    echo "ERROR: docker compose pull failed after $PULL_RETRIES attempts"
    exit 1
  fi
  echo "docker compose pull failed, retrying in 10 seconds (attempt $i of $PULL_RETRIES)..."
  sleep 10
done

echo "=== Start the stack ==="
docker compose up -d
sleep 10

echo "=== Check container status ==="
docker compose ps

echo "=== Wait for BanyanDB to start ==="
for i in $(seq 1 60); do
  if curl -sf "http://localhost:17913/api/healthz" > /dev/null 2>&1; then
    echo "BanyanDB is ready"
    break
  fi
  sleep 2
done
curl -sf "http://localhost:17913/api/healthz" > /dev/null || { echo "BanyanDB not ready"; exit 1; }

echo "=== Wait for OAP to start ==="
for i in $(seq 1 60); do
  if curl -sf "http://localhost:12800/internal/v3/health" > /dev/null 2>&1 || curl -sf "http://localhost:12800/graphql" -X POST -H "Content-Type: application/json" -d '{"query":"{ __typename }"}' > /dev/null 2>&1; then
    echo "OAP is ready"
    break
  fi
  sleep 2
done
curl -sf "http://localhost:12800/graphql" -X POST -H "Content-Type: application/json" -d '{"query":"{ __typename }"}' > /dev/null || { echo "OAP not ready"; exit 1; }

echo "=== Wait for services to initialize ==="
sleep 60
docker compose ps

echo "=== Verify BanyanDB is running ==="
docker logs banyandb 2>&1 | tail -20
curl -sf "http://localhost:17913/api/healthz" > /dev/null || { echo "BanyanDB not reachable"; exit 1; }

echo "=== Verify OAP is running ==="
docker logs skywalking-oap 2>&1 | grep -i "BanyanDB\|Health status" | tail -10
curl -sf "http://localhost:12800/graphql" -X POST -H "Content-Type: application/json" -d '{"query":"{ __typename }"}' > /dev/null || { echo "OAP not reachable"; exit 1; }

echo "=== Verify UI is running ==="
curl -s http://localhost:8080/ | head -5
curl -sf "http://localhost:8080/" > /dev/null

echo "=== Verify demo services ==="
docker compose ps | grep -E "provider|consumer|traffic_loader"

echo "=== Check OAP logs for BanyanDB connection ==="
docker logs skywalking-oap 2>&1 | grep -i "banyandb" | tail -5

echo "=== Install swctl ==="
# shellcheck source=test/e2e-v2/script/env
set -a
. "$REPO_ROOT/test/e2e-v2/script/env"
set +a
LOCAL_SWCTL_DIR="$REPO_ROOT/.local-swctl"
mkdir -p "$LOCAL_SWCTL_DIR"
bash "$REPO_ROOT/test/e2e-v2/script/prepare/setup-e2e-shell/install-swctl.sh" /tmp "$LOCAL_SWCTL_DIR"
export PATH="$LOCAL_SWCTL_DIR:$PATH"
swctl --version

echo "=== Verify data via swctl ==="
MAX_RETRIES=10
RETRY_INTERVAL=10
for i in $(seq 1 $MAX_RETRIES); do
  echo "Attempt $i of $MAX_RETRIES..."
  SERVICES=$(swctl --base-url=http://localhost:12800/graphql service list 2>&1)
  echo "Services:"
  echo "$SERVICES"
  if echo "$SERVICES" | grep -q "service-provider\|service-consumer"; then
    echo "SUCCESS: Demo services found in SkyWalking!"
    break
  fi
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "ERROR: No demo services found after $MAX_RETRIES attempts"
    docker compose -f "$REPO_ROOT/docs/guides/quick-start/docker-compose.yaml" down -v || true
    exit 1
  fi
  echo "Demo services not found, waiting $RETRY_INTERVAL seconds..."
  sleep $RETRY_INTERVAL
done

echo "=== Verify UI shows data ==="
UI_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/)
echo "UI HTTP status: $UI_RESPONSE"
[ "$UI_RESPONSE" = "200" ] || (echo "ERROR: UI not accessible"; exit 1)
UI_CONTENT=$(curl -s http://localhost:8080/ | grep -i "skywalking" | head -3)
[ -n "$UI_CONTENT" ] || (echo "ERROR: UI content empty"; exit 1)
echo "UI is showing content successfully"

echo "=== Stop the stack ==="
cd "$QUICK_START_DIR"
docker compose down -v || true

echo "=== Clean up ==="
if [ "${PRUNE_DOCKER:-0}" = "1" ]; then
  echo "PRUNE_DOCKER=1 set - running 'docker system prune -af --volumes'"
  docker system prune -af --volumes || true
else
  echo "Skipping 'docker system prune -af --volumes'. Set PRUNE_DOCKER=1 to enable full cleanup."
fi

echo ""
echo "✅ Quick start test passed!"
