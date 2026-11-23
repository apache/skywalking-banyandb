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

# Last setup step: stops the data writers (OAP and trace-mocker) so that every export of the source
# holds the same rows (the CSV export A and the native export N alike), then waits until the rows the
# source reports have settled: the same count three times in a row, 10s apart, past the flush
# interval of every catalog.
#
#   quiesce.sh <source liaison host:grpc-port>
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$DIR/common.sh"

src="${1:?usage: quiesce.sh <source liaison host:grpc-port>}"
docker stop "$(container_of trace-mocker)" "$(container_of oap)" >&2
last=-1
same=0
for _ in $(seq 1 60); do
  rows="$(cluster_rows "$src")"
  echo "source rows: $rows" >&2
  if [ "$rows" = "$last" ] && [ "$rows" != 0 ]; then
    same=$((same + 1))
    if [ "$same" -ge 2 ]; then
      exit 0
    fi
  else
    same=0
  fi
  last="$rows"
  sleep 10
done
echo "the source rows did not settle" >&2
exit 1
