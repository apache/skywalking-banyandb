# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This Dockerfile is only for local development and testing purposes.

# Dockerfile: Multi-stage builder for eBPF object compilation and benchmark execution

# Dockerfile for eBPF benchmark (multi-arch, makefile-driven)

FROM --platform=$BUILDPLATFORM golang:1.24-bullseye as build-env

ARG TARGETARCH
ARG TARGETOS
ARG GOARCH=${TARGETARCH}
ARG GOOS=${TARGETOS}
ENV DEBIAN_FRONTEND=noninteractive

# === Install dependencies (for bpf2go compilation) ===
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        clang llvm libelf-dev bpftool make git \
        linux-headers-$(uname -r) || true && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copy project source code
COPY . .

# Preload Go modules (avoid rebuild)
RUN go mod download

# === Execute BPF build using Makefile ===
WORKDIR /workspace/test/stress/fadvis
RUN make bpf

# === Optional: Benchmark stage ===
FROM build-env as benchmark

CMD ["make", "bpf-benchmark"]