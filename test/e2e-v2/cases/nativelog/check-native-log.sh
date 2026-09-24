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


# TC2 of the native self-stored log cases: each data node stores its own log
# events under its own identity. It lives here, and not in test/integration,
# because one Go process holds one native sink and therefore one identity.
#
# The case creates a group through the liaison. Opening the group's storage
# makes every data node log `initialized` under the group's name as the module,
# so one action produces one line per node, and the stored rows say which node
# each line came from.

set -euo pipefail

if [[ $# -ne 1 ]]; then
	echo "usage: $0 <liaison-http-host:port>" >&2
	exit 2
fi

http_addr=$1
api="http://${http_addr}/api/v1"
# Above about a thousand rows the query is refused by the memory budget.
limit=900
deadline=$((SECONDS + 240))

nonce="nl_e2e_$(date +%s)"
module=$(echo "$nonce" | tr '[:lower:]' '[:upper:]')

# The case runs as soon as the containers exist, which is before the liaison
# serves anything, so every request here waits for it rather than failing once.
until curl -sf -m 5 "http://${http_addr}/api/healthz" 2>/dev/null | grep -q SERVING; do
	if [[ $SECONDS -ge $deadline ]]; then
		echo "the liaison did not become healthy" >&2
		exit 1
	fi
	sleep 2
done

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

while :; do
	begin=$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-10M +%Y-%m-%dT%H:%M:%SZ)
	end=$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+10M +%Y-%m-%dT%H:%M:%SZ)
	response=$(curl -s -m 30 -XPOST "${api}/stream/data" -d "{
	  \"name\": \"log\",
	  \"groups\": [\"_monitoring_log\"],
	  \"limit\": ${limit},
	  \"timeRange\": {\"begin\": \"${begin}\", \"end\": \"${end}\"},
	  \"projection\": {\"tagFamilies\": [
	    {\"name\": \"searchable\", \"tags\": [\"node_id\", \"node_type\", \"module\", \"message\"]}
	  ]}
	}")

	# The query engine applies a criteria on a tag outside the entity after the
	# limit cuts the scan, so the module is matched here and a full result is
	# refused rather than read as an empty one.
	verdict=$(echo "$response" | jq -r --arg module "$module" --argjson limit "$limit" '
	  if (.elements | not) then "error: " + (. | tostring)[0:200]
	  elif (.elements | length) >= $limit then "truncated"
	  else
	    [.elements[]
	     | [.tagFamilies[].tags[]] as $tags
	     | {module: ($tags[] | select(.key == "module") | .value.str.value),
	        node: ($tags[] | select(.key == "node_id") | .value.str.value),
	        type: ($tags[] | select(.key == "node_type") | .value.str.value)}
	     | select(.module == $module)]
	    | if length == 0 then "none"
	      else (map(.node) | unique | join(",")) + " types=" + (map(.type) | unique | join(","))
	      end
	  end')

	case "$verdict" in
	none)
		;;
	truncated)
		echo "the query returned its limit of ${limit} rows, so the result is truncated" >&2
		exit 1
		;;
	error:*)
		echo "$verdict" >&2
		;;
	*)
		nodes=${verdict%% types=*}
		types=${verdict##* types=}
		count=$(echo "$nodes" | tr ',' '\n' | grep -c .)
		if [[ "$count" -lt 2 ]]; then
			echo "only ${count} node stored the ${module} line: ${nodes}" >&2
		elif [[ "$types" != "data" ]]; then
			echo "the ${module} lines carry node types ${types}, want data" >&2
			exit 1
		else
			echo "each data node stored its own line: ${nodes}"
			echo "status: success"
			exit 0
		fi
		;;
	esac

	if [[ $SECONDS -ge $deadline ]]; then
		echo "the ${module} lines of both data nodes did not arrive in time" >&2
		exit 1
	fi
	sleep 5
done
