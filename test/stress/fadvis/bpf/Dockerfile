FROM golang:1.24-bullseye AS builder
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    clang llvm libelf-dev build-essential \
    linux-tools-common linux-tools-generic \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /bpf

# 1) Dump BTF to vmlinux.h
RUN mkdir -p headers && \
    bpftool btf dump file /sys/kernel/btf/vmlinux format c > headers/vmlinux.h

# 2) Install bpf2go
RUN go install github.com/cilium/ebpf/cmd/bpf2go@latest

# 3) Copy your C & Go generator files
COPY fadvise.c fadvise.go ./ 

# 4) Run bpf2go, pointing it at our freshly‐dumped headers
RUN go run github.com/cilium/ebpf/cmd/bpf2go@latest \
      -go-package bpf \
      -target native \
      -tags linux \
      fadvise \
      fadvise.c \
    -- -I headers