#!/usr/bin/env python3
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

"""Summarize the three predeclared rounds; never infer a pass from timing alone."""
import json
from pathlib import Path
import re
import statistics
import sys

folder = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parent / "logs")
rows = []
for path in sorted(folder.glob("*.log")):
    if path.name.endswith("features.log"):
        source = "candidate-features"
    elif path.name.endswith("baseline-exact.log"):
        source = "baseline-exact"
    elif path.name.endswith("candidate-exact.log"):
        source = "candidate-exact"
    else:
        continue
    content = path.read_text()
    if "\nPASS\n" not in content:
        raise SystemExit(f"incomplete/failed benchmark: {path}")
    for line in content.splitlines():
        match = re.search(r"BenchmarkPatternsRPC/names_(\d+)/(\S+)-\d+\s+(\d+)\s+(.+)$", line)
        if not match:
            continue
        size, arm, iterations, tail = match.groups()
        tokens = tail.split()
        metrics = {tokens[i + 1]: float(tokens[i]) for i in range(0, len(tokens) - 1, 2)}
        rows.append({"source": source, "names": int(size), "arm": arm,
                     "iterations": int(iterations), "round": int(path.name.split("-")[1]),
                     "metrics": metrics, "log": path.name})
keys = sorted({(row["source"], row["names"], row["arm"]) for row in rows})
summary = []
for key in keys:
    cohort = [row for row in rows if (row["source"], row["names"], row["arm"]) == key]
    if sorted(row["round"] for row in cohort) != [0, 1, 2]:
        raise SystemExit(f"missing/duplicate predetermined rounds: {key}")
    metrics = {}
    for name in cohort[0]["metrics"]:
        values = [row["metrics"][name] for row in cohort]
        metrics[name] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
    summary.append({"source": key[0], "names": key[1], "arm": key[2], "metrics": metrics})
if len(rows) != 54:
    raise SystemExit(f"expected 54 arm results, found {len(rows)}")
(folder / "results.json").write_text(json.dumps({"samples": rows, "summary": summary}, indent=2) + "\n")
print("| Source | Names | Arm | Median ms (range) | CPU ms | Bytes/op | Allocs/op | Native rows | Peak tracked MiB |")
print("|---|---:|---|---:|---:|---:|---:|---:|---:|")
for row in summary:
    m = row["metrics"]
    latency = m["ns/op"]
    print(f'| {row["source"]} | {row["names"]} | {row["arm"]} | '
          f'{latency["median"]/1e6:.3f} ({latency["min"]/1e6:.3f}–{latency["max"]/1e6:.3f}) | '
          f'{m["cpu-ns/op"]["median"]/1e6:.3f} | {m["B/op"]["median"]:.0f} | '
          f'{m["allocs/op"]["median"]:.0f} | {m["native-rows/op"]["median"]:.0f} | '
          f'{m["peak-tracked-B"]["median"]/1048576:.2f} |')
