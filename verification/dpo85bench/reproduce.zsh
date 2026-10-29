#!/usr/bin/env zsh
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

set -euo pipefail
# A local zsh startup file can reset PATH despite the caller sourcing its toolchain.
if [[ -n ${GOROOT:-} ]]; then
  export PATH="$GOROOT/bin:$PATH"
fi

# Run from the candidate repository. Compile the same harness on merged main for
# the unchanged exact-query control; all setup/readiness queries use QL.
pattern_root=${0:A:h:h:h}
pattern_baseline=${1:?Pass an isolated source directory at merged commit b54a9fc76}
pattern_output=${2:-${pattern_root}/verification/dpo85bench/logs}
pattern_image=${PATTERN_BENCH_IMAGE:-banyandb-distributed-querybench:go1.26.10}
pattern_cpus=${PATTERN_BENCH_CPUS:-0,1}
mkdir -p "$pattern_output" "$pattern_baseline/verification/dpo85bench"
cp "$pattern_root/verification/dpo85bench/pattern_benchmark_test.go" "$pattern_baseline/verification/dpo85bench/"
(
  cd "$pattern_root"
  go test -c -tags=querymetrics -o "$pattern_output/candidate.test" ./verification/dpo85bench
)
(
  cd "$pattern_baseline"
  go test -c -tags=querymetrics -o "$pattern_output/baseline.test" ./verification/dpo85bench
)
{
  date -u +%FT%TZ
  git -C "$pattern_root" rev-parse HEAD
  go version
  docker image inspect "$pattern_image" --format '{{.Id}} {{json .RepoDigests}}'
  sha256sum "$pattern_root/verification/dpo85bench/pattern_benchmark_test.go" "$pattern_output/candidate.test" "$pattern_output/baseline.test"
  print -- "baseline=b54a9fc76e74baa1c0ecbf75513a082c32649a72 cpus=$pattern_cpus memory=4GiB swap=0 allowed-bytes=3GiB iterations=100 rounds=3"
} > "$pattern_output/provenance.txt"
git -C "$pattern_root" diff --binary > "$pattern_output/candidate.patch"
python3 - "$pattern_root" "$pattern_output" <<'PY_SOURCE'
from pathlib import Path
import hashlib
import subprocess
import sys
import tarfile
root, output = map(Path, sys.argv[1:])
changed = subprocess.check_output(["git", "-C", str(root), "diff", "--name-only", "HEAD", "-z"])
untracked = subprocess.check_output(["git", "-C", str(root), "ls-files", "--others", "--exclude-standard", "-z"])
files = sorted(set((changed + untracked).decode().split("\0")) - {""})
with tarfile.open(output / "candidate-files.tar.gz", "w:gz") as archive, (output / "candidate-files.sha256").open("w") as manifest:
    for name in files:
        path = root / name
        if not path.is_file() or output == path or output in path.parents:
            continue
        archive.add(path, arcname=name)
        manifest.write(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {name}\n")
PY_SOURCE

for pattern_names in 1000 10000; do
  for pattern_round in 0 1 2; do
    # Each control arm gets a fresh identical process. Alternate their order.
    if (( pattern_round % 2 )); then
      pattern_revisions=(candidate baseline)
    else
      pattern_revisions=(baseline candidate)
    fi
    for pattern_revision in "${pattern_revisions[@]}"; do
      docker run --rm --name "ql-pattern-control-${pattern_names}-${pattern_round}-${pattern_revision}" \
        --cpuset-cpus="$pattern_cpus" --memory=4g --memory-swap=4g \
        -v /mnt/d:/mnt/d -v "$pattern_output:/bench" -w "$pattern_root" \
        -e GOMAXPROCS=2 -e PATTERN_BENCH_NAMES="$pattern_names" -e PATTERN_BENCH_ROUND="$pattern_round" \
        -e PATTERN_BENCH_ONLY_EXACT=1 "$pattern_image" "/bench/${pattern_revision}.test" \
        -test.run='^$' -test.bench='^BenchmarkPatternsRPC$' -test.benchtime=100x -test.count=1 -test.timeout=10m \
        > "$pattern_output/${pattern_names}-${pattern_round}-${pattern_revision}-exact.log" 2>&1
    done
    docker run --rm --name "ql-pattern-feature-${pattern_names}-${pattern_round}" \
      --cpuset-cpus="$pattern_cpus" --memory=4g --memory-swap=4g \
      -v /mnt/d:/mnt/d -v "$pattern_output:/bench" -w "$pattern_root" \
      -e GOMAXPROCS=2 -e PATTERN_BENCH_NAMES="$pattern_names" -e PATTERN_BENCH_ROUND="$pattern_round" \
      "$pattern_image" /bench/candidate.test \
      -test.run='^$' -test.bench='^BenchmarkPatternsRPC$' -test.benchtime=100x -test.count=1 -test.timeout=10m \
      > "$pattern_output/${pattern_names}-${pattern_round}-features.log" 2>&1
  done
done
