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

# Enforces that the row-result call sites left after the row-based query path
# was removed only ever shrink. See the baseline file for the policy.
#
# Counts per file rather than pinning line numbers, so an unrelated edit above
# a call site does not fail the build while a NEW call site still does.

set -euo pipefail

baseline="${1:-scripts/lint/rowcallsites/baseline.txt}"

if [ ! -f "$baseline" ]; then
	echo "row-call-sites: baseline not found: $baseline" >&2
	exit 1
fi

# `|| [ $? -eq 1 ]`: grep exits 1 when nothing matches, which is the end state
# this ratchet exists to reach, not an error. Under `set -o pipefail` that would
# otherwise abort with no output. A real grep failure (exit 2) still propagates.
# node_modules holds third-party Go files that are not ours to police.
current="$(
	{ grep -rn --include='*.go' --exclude-dir=node_modules '\.Pull()' . || [ $? -eq 1 ]; } |
		awk -F: '$1 !~ /_test\.go$/ {
			code = $0
			sub(/^[^:]*:[0-9]+:/, "", code)
			sub(/\/\/.*$/, "", code)
			if (code ~ /\.Pull\(\)/) print $1
		}' |
		sed 's|^\./||' |
		sort | uniq -c | awk '{print $2" "$1}' | sort
)"

# awk rather than grep -v: awk exits 0 on empty output, grep exits 1.
expected="$(awk '!/^[[:space:]]*#/ && NF' "$baseline")"

bad=0
while read -r path count; do
	[ -z "$path" ] && continue
	allowed="$(echo "$expected" | awk -v p="$path" '$1 == p {print $2}')"
	if [ -z "$allowed" ]; then
		echo "FAIL: new row-result call site in $path ($count). The row-based query path was removed in 0.12.0; use the batch API."
		bad=1
	elif [ "$count" -gt "$allowed" ]; then
		echo "FAIL: $path has $count row-result call sites, baseline allows $allowed."
		bad=1
	fi
done <<<"$current"

if [ "$bad" -ne 0 ]; then
	echo "See $baseline for the policy and for what still pins each site." >&2
	exit 1
fi

echo "row call sites OK"
