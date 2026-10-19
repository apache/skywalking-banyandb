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

# Shared helpers for the export dry-run e2e scripts. Source, do not execute.

# container_of prints the id of the compose container (running or not) for a service name
# and fails loudly when none or several exist. Containers are matched by compose service
# label only, like the rbac scripts do; the e2e runner hosts a single compose project at a time.
container_of() {
  local ids
  ids="$(docker ps -a --filter "label=com.docker.compose.service=$1" --format '{{.ID}}')"
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
