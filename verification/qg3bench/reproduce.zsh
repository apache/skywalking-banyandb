#!/usr/bin/env zsh
set -e
mkdir -p verification/qg3bench/logs
for rows in 64 8192; do
  for round in 0 1 2; do
    docker run --rm --name "qg3-bench-${rows}-${round}" --cpuset-cpus=0,1 --memory=4g --memory-swap=4g \
      -e GOMAXPROCS=2 -e QG3_ROWS="$rows" -e QG3_ROUND="$round" \
      -e GOCACHE=/gocache -e GOPATH=/go -v "$PWD:/workspace" -v /home/gao/go/pkg/mod:/go/pkg/mod \
      -v /home/gao/.cache/go-build:/gocache -w /workspace golang:1.26.10 /bin/sh -c \
      'go test -tags=querymetrics ./verification/qg3bench -run "^$" -bench "^BenchmarkQG3RPC$" -benchtime=1x -count=1 -timeout=10m -v' \
      > "verification/qg3bench/logs/${rows}-${round}.log" 2>&1
  done
done
