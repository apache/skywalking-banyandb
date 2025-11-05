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

# This Dockerfile is for stress testing with optional eBPF sidecar support
FROM alpine:edge AS certs
RUN apk add --no-cache ca-certificates && update-ca-certificates

FROM ubuntu:22.04

ARG TARGETARCH

# Install runtime dependencies for eBPF
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    iproute2 \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy BanyanDB binaries
COPY banyand/build/bin/linux/${TARGETARCH}/banyand-server-static /banyand
COPY bydbctl/build/bin/linux/${TARGETARCH}/bydbctl-cli-static /bydbctl
COPY banyand/build/bin/linux/${TARGETARCH}/banyand-lifecycle-static /lifecycle
COPY --from=certs /etc/ssl/certs /etc/ssl/certs

# Copy the startup script
COPY test/stress/classic/start-with-ebpf.sh /start-with-ebpf.sh
RUN chmod +x /start-with-ebpf.sh

ENV GRPC_GO_LOG_SEVERITY_LEVEL=WARN
ENV GRPC_GO_LOG_FORMATTER=json

# Expose BanyanDB ports
EXPOSE 17912 17913 6060 2121
# Expose eBPF sidecar ports
EXPOSE 18080 19090

ENTRYPOINT ["/start-with-ebpf.sh"]
CMD ["standalone"]
