#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Tier-1 smoke for the canopy image against a live BanyanDB topology
# (standalone or liaison+data cluster — same assertions). Proves the image
# boots, serves the SPA, authenticates via CANOPY_USERS, and proxies
# registry + query traffic; plus two regression probes for bugs that only
# surface on a distributed liaison (TopN agg validation, trace ORDER BY
# index-rule resolution).
#
# Usage: smoke.sh [base-url]   (default http://127.0.0.1:4000)
set -u
BASE="${1:-http://127.0.0.1:4000}"
JAR="$(mktemp)"
RESP="$(mktemp)"
trap 'rm -f "$JAR" "$RESP"' EXIT

fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }
ok() { echo "ok - $*"; }

# curl helpers: assert HTTP 200, body left in $RESP.
post() { # path body
  local code
  code=$(curl -sS -o "$RESP" -w "%{http_code}" -b "$JAR" -X POST "$BASE$1" \
    -H 'Content-Type: application/json' -d "$2") || fail "POST $1 transport error"
  [ "$code" = 200 ] || fail "POST $1 -> HTTP $code: $(head -c 300 "$RESP")"
}
# seed(): like post but tolerates 409 (resource already exists) so the smoke
# is idempotent across reruns against a reused topology.
seed() { # path body
  local code
  code=$(curl -sS -o "$RESP" -w "%{http_code}" -b "$JAR" -X POST "$BASE$1" \
    -H 'Content-Type: application/json' -d "$2") || fail "POST $1 transport error"
  [ "$code" = 200 ] || [ "$code" = 409 ] || fail "POST $1 -> HTTP $code: $(head -c 300 "$RESP")"
}
get() { # path
  local code
  code=$(curl -sS -o "$RESP" -w "%{http_code}" -b "$JAR" "$BASE$1") || fail "GET $1 transport error"
  [ "$code" = 200 ] || fail "GET $1 -> HTTP $code: $(head -c 300 "$RESP")"
}

# ── 1. health ──────────────────────────────────────────────────────────────
ready=""
for _ in $(seq 1 60); do
  curl -fsS -m 2 "$BASE/healthz" >/dev/null 2>&1 && { ready=1; break; }
  sleep 2
done
[ -n "$ready" ] || fail "canopy /healthz not ready after 120s"
ok "healthz"

# ── 2. production login (CANOPY_USERS, bcrypt) ─────────────────────────────
code=$(curl -sS -o "$RESP" -w "%{http_code}" -c "$JAR" -X POST "$BASE/auth/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"canopy-admin","password":"canopy-it-pass"}')
[ "$code" = 200 ] || fail "login -> HTTP $code: $(head -c 300 "$RESP")"
grep -q '"role":"admin"' "$RESP" || fail "login response missing admin role: $(head -c 200 "$RESP")"
ok "login via CANOPY_USERS"

# ── 3. BFF <-> BanyanDB reachability ───────────────────────────────────────
get /api/meta
grep -q '"reachable":true' "$RESP" || fail "/api/meta reports unreachable: $(head -c 200 "$RESP")"
ok "api/meta reachable"

# ── 4. SPA served ──────────────────────────────────────────────────────────
code=$(curl -sS -o "$RESP" -w "%{http_code}" "$BASE/")
[ "$code" = 200 ] || fail "SPA index -> HTTP $code"
grep -qi '<div id="root"' "$RESP" || grep -qi '<script' "$RESP" || fail "SPA index has no app markup"
ok "SPA index"

# ── 5. schema seeding through the BFF proxy ────────────────────────────────
seed /api/v1/group/schema '{"group":{"metadata":{"name":"it_metrics"},"catalog":"CATALOG_MEASURE","resourceOpts":{"shardNum":1,"segmentInterval":{"unit":"UNIT_DAY","num":1},"ttl":{"unit":"UNIT_DAY","num":3}}}}'
seed /api/v1/group/schema '{"group":{"metadata":{"name":"it_traces"},"catalog":"CATALOG_TRACE","resourceOpts":{"shardNum":1,"segmentInterval":{"unit":"UNIT_DAY","num":1},"ttl":{"unit":"UNIT_DAY","num":3}}}}'
ok "groups created"

seed /api/v1/measure/schema '{"measure":{"metadata":{"group":"it_metrics","name":"cpu"},"tagFamilies":[{"name":"default","tags":[{"name":"entity_id","type":"TAG_TYPE_STRING"}]}],"fields":[{"name":"value","fieldType":"FIELD_TYPE_INT","encodingMethod":"ENCODING_METHOD_GORILLA","compressionMethod":"COMPRESSION_METHOD_ZSTD"}],"entity":{"tagNames":["entity_id"]},"interval":"1m"}}'
ok "measure schema created"

seed /api/v1/trace/schema '{"trace":{"metadata":{"group":"it_traces","name":"segment"},"tags":[{"name":"trace_id","type":"TAG_TYPE_STRING"},{"name":"start_time","type":"TAG_TYPE_TIMESTAMP"}],"traceIdTagName":"trace_id","spanIdTagName":"trace_id","timestampTagName":"start_time"}}'
seed /api/v1/index-rule/schema '{"indexRule":{"metadata":{"group":"it_traces","name":"start_time"},"tags":["start_time"],"type":"TYPE_TREE"}}'
seed /api/v1/index-rule-binding/schema '{"indexRuleBinding":{"metadata":{"group":"it_traces","name":"segment"},"rules":["start_time"],"subject":{"catalog":"CATALOG_TRACE","name":"segment"},"beginAt":"2025-01-01T00:00:00Z","expireAt":"2099-01-01T00:00:00Z"}}'
ok "trace schema + index rule + binding created"

# ── 6. regression probe: TopN query (dquery validateRequest path) ──────────
seed /api/v1/topn-agg/schema '{"topNAggregation":{"metadata":{"group":"it_metrics","name":"cpu-topn"},"sourceMeasure":{"group":"it_metrics","name":"cpu"},"fieldName":"value","fieldValueSort":"SORT_DESC","groupByTagNames":["entity_id"],"countersNumber":1000}}'
ok "topn aggregation created"

NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
BEGIN=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)
post /api/v1/measure/topn "{\"groups\":[\"it_metrics\"],\"name\":\"cpu-topn\",\"topN\":5,\"agg\":\"AGGREGATION_FUNCTION_MEAN\",\"fieldValueSort\":\"SORT_DESC\",\"timeRange\":{\"begin\":\"$BEGIN\",\"end\":\"$NOW\"}}"
grep -q 'unspecified requested aggregation function' "$RESP" && fail "topn agg validation regressed: $(head -c 300 "$RESP")"
ok "topn query (bare-enum agg) passes validation"

# ── 7. regression probe: trace ORDER BY resolves an index-rule name ────────
post /api/v1/bydbql/query '{"query":"SELECT trace_id, start_time FROM TRACE segment IN it_traces TIME > '"'"'-1h'"'"' ORDER BY start_time DESC LIMIT 5"}'
grep -q 'index rule' "$RESP" && fail "trace ORDER BY start_time errored: $(head -c 300 "$RESP")"
ok "trace ORDER BY start_time (bound rule) accepted"

# Negative control: a rule that does NOT exist must fail with the analyzer's
# message — proves the probe above actually exercised the index-rule path.
code=$(curl -sS -o "$RESP" -w "%{http_code}" -b "$JAR" -X POST "$BASE/api/v1/bydbql/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT trace_id FROM TRACE segment IN it_traces TIME > '"'"'-1h'"'"' ORDER BY timestamp DESC LIMIT 5"}')
grep -q 'index rule timestamp not found' "$RESP" || fail "negative control: expected 'index rule timestamp not found', got HTTP $code: $(head -c 300 "$RESP")"
ok "negative control: unbound rule name rejected"

echo "SMOKE PASS ($BASE)"
