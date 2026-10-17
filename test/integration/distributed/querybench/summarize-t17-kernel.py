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

"""Summarize exactly five frozen K(T17) repetitions, preserving failed gates."""

import json
import re
import statistics
import sys
from pathlib import Path


def main():
    root = Path(sys.argv[1])
    pattern = re.compile(
        r"^BenchmarkSortT17/(.+)/(independent_tuple|fullsort_slice|topk)-[0-9]+"
        r"\s+[0-9]+\s+([0-9.eE+-]+)\s+ns/op(.*)$",
        re.MULTILINE,
    )
    samples = {}
    for repetition in range(1, 6):
        text = (root / f"r{repetition}" / "kernel.txt").read_text()
        observed = set()
        for match in pattern.finditer(text):
            shape, mode, latency, tail = match.groups()
            key = (shape, mode)
            if key in observed:
                raise ValueError(f"duplicate {key} in repetition {repetition}")
            observed.add(key)
            metrics = dict(
                (name, float(value))
                for value, name in re.findall(r"([0-9.eE+-]+)\s+(\S+)", tail)
            )
            samples.setdefault(key, []).append(
                {"repetition": repetition, "ns_per_op": float(latency), "metrics": metrics}
            )
        if len(observed) != 27:
            raise ValueError(f"repetition {repetition} has {len(observed)} samples; expected nine shapes and three modes")
    report = {"source_sha256": (root / "source.sha256").read_text().strip(), "controls": []}
    failures = []
    gates = 0
    for shape in sorted({key[0] for key in samples}):
        control = {"shape": shape, "modes": {}}
        for mode in ("independent_tuple", "fullsort_slice", "topk"):
            runs = samples[(shape, mode)]
            if len(runs) != 5:
                raise ValueError(f"{shape}/{mode} lacks five independent samples")
            control["modes"][mode] = {
                "median_ns_per_op": statistics.median(run["ns_per_op"] for run in runs),
                "samples": runs,
            }
        full = control["modes"]["fullsort_slice"]["median_ns_per_op"]
        topk = control["modes"]["topk"]["median_ns_per_op"]
        control["topk_fullsort_ratio"] = topk / full
        dimensions = dict(part.split("=", 1) for part in shape.split("/") if "=" in part)
        k = int(dimensions["offset"]) + int(dimensions["limit"])
        owns_gate = int(dimensions["rows"]) >= 100000 and k <= 100 and "/random/" in shape
        control["owns_ratio_gate"] = owns_gate
        if owns_gate:
            gates += 1
            if control["topk_fullsort_ratio"] > 0.80:
                failures.append(shape)
        report["controls"].append(control)
    if gates != 2:
        raise ValueError(f"expected both 100k random k10/k100 controls, found {gates}")
    report["ratio_gate"] = "fail" if failures else "pass"
    report["failed_controls"] = failures
    (root / "kernel-summary.json").write_text(json.dumps(report, indent=2) + "\n")
    for control in report["controls"]:
        print(f"{control['shape']}: topk/fullsort={control['topk_fullsort_ratio']:.4f}")
    if failures:
        raise SystemExit("K(T17) 0.80 ratio gate failed; samples and honest result retained")


if __name__ == "__main__":
    main()
