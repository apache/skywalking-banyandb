#!/usr/bin/env zsh
set -e
candidate=$PWD
baseline=${QG2_BASELINE_WORKTREE:-/tmp/qg1-baseline-check}
[[ $(git -C "$baseline" rev-parse HEAD) == 111193cab562b2d120a3e003b609f22923c9ff55 ]]
mkdir -p "$baseline/verification/qg2bench"
cp "$candidate/verification/qg2bench/paired_test.go" "$candidate/verification/qg2bench/qg2_benchmark_test.go" "$baseline/verification/qg2bench/"
for rows in 64 8192; do
  for round in 0 1 2 3 4; do
    if (( round % 2 == 0 )); then order='baseline current'; else order='current baseline'; fi
    for version in $=order; do
      if [[ "$version" == baseline ]]; then workspace=$baseline; is_baseline=1; else workspace=$candidate; is_baseline=0; fi
      docker run --rm --name "qg2-paired-${rows}-${round}-${version}" --cpuset-cpus=0,1 --memory=4g --memory-swap=4g \
        -e GOMAXPROCS=2 -e QG2_PAIRED=1 -e QG2_BASELINE="$is_baseline" -e QG2_ROWS="$rows" -e QG2_ROUND="$round" \
        -e GOCACHE=/gocache -e GOPATH=/go -v "$workspace:/workspace" -v /home/gao/go/pkg/mod:/go/pkg/mod \
        -v /home/gao/.cache/go-build:/gocache -w /workspace golang:1.26.10 /bin/sh -c \
        'go test -tags=querymetrics ./verification/qg2bench -run "^$" -bench "^BenchmarkQG2RPC$" -benchtime=1x -count=1 -timeout=10m -v' \
        > "/tmp/qg2-paired-${rows}-${round}-${version}.log" 2>&1
done
done
done
