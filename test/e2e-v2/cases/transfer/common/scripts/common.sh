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

# Shared helpers of the transfer e2e scripts (the CSV and the native case). Source, do not execute.

# container_of prints the id of the compose container (running or not) for a service name
# and fails loudly when none or several exist. Containers are matched by compose service
# label within the e2e runner's compose project (E2E_COMPOSE_PROJECT, otherwise the name the
# runner itself picks: GITHUB_RUN_ID on CI, skywalking_e2e locally), so another stack started
# from the same compose file on a developer machine does not get in the way.
container_of() {
  local ids
  ids="$(docker ps -a --filter "label=com.docker.compose.project=${E2E_COMPOSE_PROJECT:-${GITHUB_RUN_ID:-skywalking_e2e}}" \
    --filter "label=com.docker.compose.service=$1" --format '{{.ID}}')"
  case "$(echo "$ids" | grep -c .)" in
    1) echo "$ids" ;;
    0) echo "no compose container found for service $1" >&2; return 1 ;;
    *) echo "several compose containers found for service $1: $ids" >&2; return 1 ;;
  esac
}

# poll runs the given command up to $1 times, $2 seconds apart, until it succeeds.
poll() {
  local attempts="$1" interval="$2" i
  shift 2
  for i in $(seq 1 "$attempts"); do
    if "$@"; then
      return 0
    fi
    [ "$i" -lt "$attempts" ] && sleep "$interval"
  done
  return 1
}

# list_segments prints the segment directories of every catalog on a data node container,
# for the e2e log. Unmatched globs make ls exit non-zero, hence the trailing || true.
list_segments() {
  docker exec "$1" sh -c 'ls -d /tmp/stream/data/*/seg-* /tmp/trace/data/*/seg-* /tmp/measure/data/*/seg-* 2>/dev/null' | sort | head -40 || true
}

# leftover_snapshots prints the export session snapshot directories the given data node
# containers still hold, comma separated, and fails when listing a container fails. The inner
# `true` swallows ls's no-match exit, so a non-zero status means docker exec itself failed.
leftover_snapshots() {
  local c found line out=
  for c in "$@"; do
    if ! found="$(docker exec "$c" sh -c 'ls -d /tmp/*/export-snapshots/* 2>/dev/null; true')"; then
      echo "listing export snapshots in container $c failed" >&2
      return 1
    fi
    while IFS= read -r line; do
      if [ -n "$line" ]; then
        out="${out:+$out, }$line"
      fi
    done <<<"$found"
  done
  echo "$out"
}

# ALL_CATALOGS selects every catalog for bydbctl data export.
ALL_CATALOGS=(--selector catalog=stream --selector catalog=measure --selector catalog=trace --selector catalog=property)

# require_format exits unless FORMAT, the export format a case runs with, is csv or native.
require_format() {
  case "${FORMAT:-}" in
    csv | native) ;;
    *)
      echo "FORMAT must be csv or native, got '${FORMAT:-}'" >&2
      exit 2
      ;;
  esac
}

# cluster_rows prints the rows the cluster behind liaison $1 holds according to an export dry run,
# leaving out _deletion_task, the internal property group every cluster has. The yaml rendering
# sorts the keys of a row (estRows comes before group), so the awk does not rely on key order: it
# keeps each list item's group and estRows and adds the count when the item ends, at the next
# item, the next top-level key or the end of the output.
cluster_rows() {
  bydbctl data export --dry-run --nodes "$1" "${ALL_CATALOGS[@]}" -o yaml | awk '
    function flush() { if (g != "_deletion_task") n += e; g = ""; e = 0 }
    /^ *- / || /^[^ -]/ { flush() }
    /^ *-? *group:/ { g = $NF }
    /^ *-? *estRows:/ { e = $NF }
    END { flush(); print n + 0 }
  '
}
