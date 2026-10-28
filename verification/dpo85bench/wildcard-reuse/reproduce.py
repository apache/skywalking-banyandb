#!/usr/bin/env python3
# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run one fixed legacy wildcard comparison; preserve every process result."""

import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys

BASE = "b54a9fc76e74baa1c0ecbf75513a082c32649a72"
ARMS = ("prefix", "leading", "no_match")
ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).resolve().parent
HARNESS = "pkg/index/native/wildcard_benchmark_test.go"


def command(args, cwd=ROOT):
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_manifest(root):
    """Hash the Go sources present, including generated files used by builds."""
    entries = {}
    for directory in ("api", "pkg", "third_party"):
        for path in sorted((root / directory).rglob("*.go")):
            entries[str(path.relative_to(root))] = digest(path)
    for name in ("go.mod", "go.sum"):
        entries[name] = digest(root / name)
    return entries


def verify_baseline(root):
    """Verify tracked library sources; generated integration fixtures are unrelated."""
    verified = 0
    tree = command(["git", "ls-tree", "-r", BASE]).splitlines()
    for line in tree:
        metadata, name = line.split("\t", 1)
        if not ((name.endswith(".go") and name.startswith(("api/", "pkg/", "third_party/"))) or name in ("go.mod", "go.sum")):
            continue
        source = (root / name).read_bytes()
        actual = hashlib.sha1(b"blob " + str(len(source)).encode() + b"\0" + source).hexdigest()
        if actual != metadata.split()[2]:
            raise RuntimeError(f"Baseline archive differs from {BASE}: {name}")
        verified += 1
    return verified


def write_report(samples, summary):
    lines = [
        "# Native wildcard comparison results", "",
        "Three fixed fresh-process samples per arm and revision, 100 queries per sample; medians below.",
        "Every query returned the expected row identities and released its allocation reservations.", "",
        "| Arm | Before µs/op | After µs/op | Latency change | Before B/op | After B/op | Before allocs/op | After allocs/op |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for arm in ARMS:
        before, after = summary[arm]["baseline"], summary[arm]["candidate"]
        delta = summary[arm]["change_percent"]["ns/op"]
        lines.append(f"| {arm} | {before['ns/op']/1000:.3f} | {after['ns/op']/1000:.3f} | {delta:+.1f}% | "
                     f"{before['B/op']:.0f} | {after['B/op']:.0f} | {before['allocs/op']:.0f} | {after['allocs/op']:.0f} |")
    lines += ["", "| Arm | Before latency range µs/op | After latency range µs/op | Before reserved B/op | After reserved B/op |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for arm in ARMS:
        ranges = []
        for revision in ("baseline", "candidate"):
            values = [sample["metrics"]["ns/op"]/1000 for sample in samples if sample["arm"] == arm and sample["revision"] == revision]
            ranges.append(f"{min(values):.3f}–{max(values):.3f}")
        lines.append(f"| {arm} | {ranges[0]} | {ranges[1]} | {summary[arm]['baseline']['reserved-B/op']:.0f} | "
                     f"{summary[arm]['candidate']['reserved-B/op']:.0f} |")
    lines += ["", "Negative latency change means faster. These three samples describe this fixed native workload;",
              "they are not a statistical regression gate or an end-to-end QL latency claim.", "",
              "Reserved bytes are a conservative query accounting allowance, not physical allocation or RSS.",
              "The old compiler did not reserve its wildcard allocations; the shared engine does.", "",
              "[Method and reproduction](README.md) · [All samples](samples.json) · [Summary](summary.json) ·",
              "[Provenance](provenance.json) · [Source hashes](source-hashes.json)", ""]
    (OUTPUT / "results.md").write_text("\n".join(lines))


def main():
    baseline = Path(sys.argv[1]).resolve()
    image = os.environ.get("WILDCARD_BENCH_IMAGE", "banyandb-distributed-querybench:go1.26.10")
    cpus = os.environ.get("WILDCARD_BENCH_CPUS", "0,1")
    logs = OUTPUT / "logs"
    logs.mkdir(exist_ok=True)
    if list(logs.glob("round-*.log")):
        raise RuntimeError("Round logs already exist; keep this fixed campaign immutable.")
    baseline_files = verify_baseline(baseline)
    shutil.copy2(ROOT / HARNESS, baseline / HARNESS)
    manifests = {}
    for revision, root in (("baseline", baseline), ("candidate", ROOT)):
        manifests[revision] = source_manifest(root)
        subprocess.run(["go", "test", "-c", "-o", str(logs / f"{revision}.test"), "./pkg/index/native"], cwd=root, check=True)
    (OUTPUT / "source-hashes.json").write_text(json.dumps(manifests, indent=2) + "\n")
    provenance = {
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "baseline_commit": BASE,
        "baseline_tracked_sources_verified": baseline_files,
        "candidate_head": command(["git", "rev-parse", "HEAD"]),
        "candidate_status": "Uncommitted shared native wildcard implementation; source-hashes.json identifies measured files.",
        "toolchain": command(["go", "version"]),
        "image": json.loads(command(["docker", "image", "inspect", image]))[0]["Id"],
        "cpuset": cpus,
        "cpu_quota": 2,
        "memory_bytes": 4 << 30,
        "memory_plus_swap_bytes": 4 << 30,
        "gomaxprocs": 2,
        "query_allocation_allowance_bytes": 256 << 20,
        "dictionary_terms": 10000,
        "rounds": 3,
        "iterations_per_arm": 100,
        "process_policy": "Fresh process and identical index fixture for every arm/revision/round; alternate revision order each round.",
        "container_limits": command(["docker", "run", "--rm", f"--cpuset-cpus={cpus}", "--cpus=2", "--memory=4g", "--memory-swap=4g",
                                     image, "sh", "-c", "cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpuset.cpus.effective /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max"]).splitlines(),
        "harness_sha256": digest(ROOT / HARNESS),
        "source_hashes_sha256": digest(OUTPUT / "source-hashes.json"),
        "binary_sha256": {revision: digest(logs / f"{revision}.test") for revision in manifests},
    }
    (OUTPUT / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    samples = []
    for round_number in range(3):
        revisions = ("baseline", "candidate") if round_number % 2 == 0 else ("candidate", "baseline")
        for arm in ARMS:
            for revision in revisions:
                log = logs / f"round-{round_number}-{arm}-{revision}.log"
                args = ["docker", "run", "--rm", "--name", f"native-wildcard-{round_number}-{arm}-{revision}",
                        f"--cpuset-cpus={cpus}", "--cpus=2", "--memory=4g", "--memory-swap=4g",
                        "-v", f"{logs}:/bench:ro", "-e", "GOMAXPROCS=2", image, f"/bench/{revision}.test",
                        "-test.run=^$", f"-test.bench=^BenchmarkNativeWildcardReuse$/{arm}$", "-test.benchtime=100x",
                        "-test.count=1", "-test.timeout=5m"]
                with log.open("w") as stream:
                    outcome = subprocess.run(args, stdout=stream, stderr=subprocess.STDOUT, check=False)
                raw = log.read_text()
                line = next((line for line in raw.splitlines() if re.match(r"^BenchmarkNativeWildcardReuse/\w+-2\s+100\s", line)), None)
                sample = {"round": round_number, "arm": arm, "revision": revision, "exit_code": outcome.returncode,
                          "log_sha256": digest(log), "benchmark_line": line}
                samples.append(sample)
                if line:
                    parts = line.split()
                    sample["iterations"] = int(parts[1])
                    sample["metrics"] = {parts[index + 1]: float(parts[index]) for index in range(2, len(parts), 2)}
                (OUTPUT / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
                print(f"round {round_number} {arm} {revision}: {line or raw[-500:]}", flush=True)
                expected_hits = 0 if arm == "no_match" else 16
                if outcome.returncode or not line or sample["metrics"]["hits/op"] != expected_hits or "PASS" not in raw:
                    raise RuntimeError(f"Benchmark/result validation failed; inspect {log}")
    for revision, root in (("baseline", baseline), ("candidate", ROOT)):
        if source_manifest(root) != manifests[revision]:
            raise RuntimeError(f"{revision} source drift during measurement; retain samples without final-source claim")
    summary = {}
    for arm in ARMS:
        summary[arm] = {}
        for revision in ("baseline", "candidate"):
            chosen = [sample["metrics"] for sample in samples if sample["arm"] == arm and sample["revision"] == revision]
            summary[arm][revision] = {unit: statistics.median([row[unit] for row in chosen]) for unit in chosen[0]}
        before, after = summary[arm]["baseline"], summary[arm]["candidate"]
        summary[arm]["change_percent"] = {unit: (after[unit] / before[unit] - 1) * 100 for unit in ("ns/op", "B/op", "allocs/op")}
    (OUTPUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_report(samples, summary)
    provenance["completed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    provenance["samples_sha256"] = digest(OUTPUT / "samples.json")
    provenance["summary_sha256"] = digest(OUTPUT / "summary.json")
    provenance["source_drift"] = False
    (OUTPUT / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")


if __name__ == "__main__":
    main()
