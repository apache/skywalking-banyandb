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

# check-dashboard-sync.sh verifies that the union of the three metric lists
# (presence.txt, non_empty.txt, documented_gap.txt) exactly matches the set of
# metrics referenced in "expr" fields of the FODC Grafana dashboard JSON files.
#
# Run this locally or in CI without a cluster -- it only reads files.
# Exit 0 = in sync. Exit 1 = drift detected (printed to stderr).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
METRICS_DIR="${SCRIPT_DIR}/metrics"
DASHBOARDS_DIR="${REPO_ROOT}/docs/operation"

python3 - "${METRICS_DIR}" "${DASHBOARDS_DIR}" <<'EOF'
import json, re, sys

metrics_dir, dashboards_dir = sys.argv[1], sys.argv[2]

# Extract all metric identifiers from "expr" fields in dashboard JSON files.
def expr_metrics(path):
    with open(path) as f:
        text = f.read()
    metrics = set()
    for expr in re.findall(r'"expr"\s*:\s*"([^"]*)"', text):
        for m in re.findall(r'\b(banyandb_[a-z_]+|go_[a-z_]+|process_[a-z_]+)', expr):
            metrics.add(m)
    return metrics

import glob, os
dashboard_files = glob.glob(os.path.join(dashboards_dir, 'grafana-fodc-*.json'))
if not dashboard_files:
    print(f"ERROR: no grafana-fodc-*.json files found in {dashboards_dir}", file=sys.stderr)
    sys.exit(1)

dash_metrics = set()
for f in sorted(dashboard_files):
    dash_metrics |= expr_metrics(f)

# Load metric lists, skipping blank lines and # comments.
def load_list(name):
    metrics = set()
    path = os.path.join(metrics_dir, name)
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                metrics.add(line)
    return metrics

presence     = load_list('presence.txt')
non_empty    = load_list('non_empty.txt')
gap          = load_list('documented_gap.txt')

# Check for duplicates across lists.
overlap_pn = presence & non_empty
overlap_pg = presence & gap
overlap_ng = non_empty & gap
if overlap_pn or overlap_pg or overlap_ng:
    print("ERROR: metrics appear in more than one list:", file=sys.stderr)
    for m in sorted(overlap_pn): print(f"  presence ∩ non_empty : {m}", file=sys.stderr)
    for m in sorted(overlap_pg): print(f"  presence ∩ gap       : {m}", file=sys.stderr)
    for m in sorted(overlap_ng): print(f"  non_empty ∩ gap      : {m}", file=sys.stderr)
    sys.exit(1)

test_metrics = presence | non_empty | gap

in_dash_not_test = sorted(dash_metrics - test_metrics)
in_test_not_dash = sorted(test_metrics - dash_metrics)

ok = True
if in_dash_not_test:
    ok = False
    print(f"FAIL: {len(in_dash_not_test)} dashboard metric(s) missing from test lists"
          " -- add to presence.txt, non_empty.txt, or documented_gap.txt:", file=sys.stderr)
    for m in in_dash_not_test:
        print(f"  {m}", file=sys.stderr)

if in_test_not_dash:
    ok = False
    print(f"FAIL: {len(in_test_not_dash)} test-list metric(s) not referenced by any dashboard"
          " -- remove stale entries:", file=sys.stderr)
    for m in in_test_not_dash:
        print(f"  {m}", file=sys.stderr)

if ok:
    print(f"OK: {len(dash_metrics)} dashboard metrics == union of {len(presence)} presence"
          f" + {len(non_empty)} non_empty + {len(gap)} documented_gap")
    sys.exit(0)
else:
    sys.exit(1)
EOF
