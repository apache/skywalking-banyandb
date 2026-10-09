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

# Guarantees the export dry run current data in the two catalogs trace-mocker does not reliably
# feed (OAP may also derive older records from the mocker traces into sw_records). OAP stores
# reported events as records in the stream catalog (group sw_records) and its UI templates in
# the property catalog (group sw_property) at startup. The events carry the current time, so
# their segment stays on the hot node. The verify cases retry until a dry run lists them.
set -euo pipefail

oap="${1:?usage: report-events.sh <oap host:grpc-port>}"

for i in 1 2 3; do
  now="$(date +%s)"
  swctl --grpc-addr="$oap" event report --uuid="export-dry-run-$i" --name=Upgrade \
    --service-name=export-e2e --instance-name=export-e2e-instance --endpoint-name=/export \
    --message='Upgrade to {version}' --layer=GENERAL \
    --start-time="${now}000" --end-time="${now}999" version="v$i" >/dev/null
done
echo "reported 3 events"
