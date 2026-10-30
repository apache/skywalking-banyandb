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

# Helpers shared by the native log cases. Each case is a script the e2e runner
# calls with the liaison's HTTP address; it prints one YAML document for the
# runner to compare with its expected file, and reports progress on stderr.

# producers are the data nodes started with native logging on, and disabled the
# one left at the default, off. A node's id is its host and gRPC port.
producers=(data0:17912 data1:17912 data2:17912 data3:17912 data4:17912)
disabled=data5:17912
# refusing collects with native logging on, and its storage refuses every write.
refusing=data6:17912
# liaison collects with native logging on. It has no storage, so it sends its
# logs to the data node that owns shard 0. Its id is its host and internal
# gRPC port.
liaison=liaison:18912

# Above about a thousand rows a query is refused by the memory budget.
limit=900

# container_of prints the id of the compose container for a service. The e2e
# runner hosts one compose project at a time.
container_of() {
	local ids
	ids="$(docker ps -a --filter "label=com.docker.compose.service=$1" --format '{{.ID}}')"
	case "$(echo "$ids" | grep -c .)" in
	1) echo "$ids" ;;
	0) echo "no compose container found for service $1" >&2; return 1 ;;
	*) echo "several compose containers found for service $1: $ids" >&2; return 1 ;;
	esac
}

# wait_liaison waits until the liaison serves. A case starts as soon as the
# containers exist, which is before the liaison is ready.
wait_liaison() {
	until curl -sf -m 5 "http://${http_addr}/api/healthz" 2>/dev/null | grep -q SERVING; do
		if [[ $SECONDS -ge $deadline ]]; then
			echo "the liaison did not become healthy" >&2
			exit 1
		fi
		sleep 2
	done
}

# create_marker_group creates a stream group with a unique name and prints the
# module its storage logs under. Opening the group makes every data node log
# `initialized` under that module, so the pair (module, node_id) marks one line
# of one producer.
create_marker_group() {
	local nonce
	nonce="nl_e2e_$(date +%s)_$RANDOM"
	until curl -sf -m 10 -XPOST "${api}/group/schema" -d "{
	  \"group\": {
	    \"metadata\": {\"name\": \"${nonce}\"},
	    \"catalog\": \"CATALOG_STREAM\",
	    \"resourceOpts\": {
	      \"shardNum\": 1,
	      \"segmentInterval\": {\"unit\": \"UNIT_DAY\", \"num\": 1},
	      \"ttl\": {\"unit\": \"UNIT_DAY\", \"num\": 1}
	    }
	  }
	}" >/dev/null; do
		if [[ $SECONDS -ge $deadline ]]; then
			echo "failed to create the group ${nonce}" >&2
			exit 1
		fi
		sleep 2
	done
	echo "$nonce" | tr '[:lower:]' '[:upper:]'
}

# now_utc prints the current time, shifted by $1 seconds, as the query API
# takes it.
now_utc() {
	local t=$(( $(date +%s) + ${1:-0} ))
	date -u -d "@${t}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "${t}" +%Y-%m-%dT%H:%M:%SZ
}

# query_log reads the log stream through the liaison. $1 is the criteria JSON,
# or an empty string for none; $2 is "true" to ask for the query trace; $3 is
# the start of the time range, 30 minutes ago by default. The server criteria
# name only entity tags: the engine applies any other one after the limit cuts
# the scan.
query_log() {
	local criteria=$1 trace=${2:-false} begin=${3:-} end
	begin=${begin:-$(now_utc -1800)}
	end=$(now_utc 600)
	curl -s -m 30 -XPOST "${api}/stream/data" -d "{
	  \"name\": \"log\",
	  \"groups\": [\"_monitoring_log\"],
	  \"limit\": ${limit},
	  \"trace\": ${trace},
	  ${criteria:+\"criteria\": ${criteria},}
	  \"timeRange\": {\"begin\": \"${begin}\", \"end\": \"${end}\"},
	  \"projection\": {\"tagFamilies\": [
	    {\"name\": \"searchable\", \"tags\": [\"node_id\", \"node_type\", \"module\", \"level\", \"message\"]}
	  ]}
	}"
}

# node_eq is the criteria selecting one node's rows.
node_eq() {
	echo "{\"condition\": {\"name\": \"node_id\", \"op\": \"BINARY_OP_EQ\", \"value\": {\"str\": {\"value\": \"$1\"}}}}"
}

# rows turns a query response into one JSON object per row, or prints an
# error line. A full result is refused: an absence cannot be read from a cut
# one.
rows() {
	jq -c --argjson limit "$limit" '
	  if (.elements | not) then {error: ((. | tostring)[0:300])}
	  elif (.elements | length) >= $limit then {error: "truncated"}
	  else .elements[]
	    | [.tagFamilies[].tags[]] as $tags
	    | reduce $tags[] as $t ({}; .[$t.key] = ($t.value.str.value // null))
	  end'
}

# span_counts prints {node: rows} for every data node span in a traced response.
span_counts() {
	jq -c '[.. | objects
	  | select((.message? // "") | type == "string" and startswith("data-"))
	  | {(.message | ltrimstr("data-")): ([.tags[]? | select(.key == "resp_count") | .value | tonumber] | add // 0)}]
	  | add // {}'
}

# placement prints "local" when the rows a node stored since a time include
# the marker module, and only that node returned any of them; it prints what
# was seen otherwise. The query is traced: the liaison asks every data node,
# and the trace has one span per node, data-<node_id>, with the number of rows
# that node returned. The group has no replicas, so a row on a node is a row
# that node stored.
#
#   placement <node_id> <marker module> <since>
placement() {
	local node=$1 module=$2 since=$3 response marker counts
	response=$(query_log "$(node_eq "$node")" true "$since")
	marker=$(echo "$response" | rows | jq -s --arg m "$module" 'any(.[]; .module == $m)')
	counts=$(echo "$response" | span_counts)
	echo "$counts" | jq -r --arg n "$node" --argjson marker "$marker" '
	  (to_entries | map(select(.key != $n and .value > 0))) as $elsewhere
	  | if (.[$n] // 0) > 0 and ($elsewhere | length) == 0 and $marker then "local"
	    else "own=\(.[$n] // 0) marker=\($marker) elsewhere=\($elsewhere | map("\(.key):\(.value)") | join(";")) spans=\(keys | join(";"))"
	    end'
}
