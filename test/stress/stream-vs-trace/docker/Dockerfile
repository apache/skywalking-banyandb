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

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates wget

# Create non-root user
RUN adduser -D -g '' banyandb

# Create data directory
RUN mkdir -p /data && chown banyandb:banyandb /data

# Copy banyand binary from builder
# The release build creates binaries in build/bin/<OS>/<ARCH>/ format
ARG TARGETARCH=amd64
COPY banyand/build/bin/linux/${TARGETARCH}/banyand-server-static /usr/local/bin/banyand

# Copy schema files for the test
COPY --chown=banyandb:banyandb test/stress/stream-vs-trace/testdata/schema /schema

# Switch to non-root user
USER banyandb

# Set data volume
VOLUME ["/data"]

# Expose BanyanDB ports
EXPOSE 17912 17913 6060 2121

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/banyand"]

# Default command for standalone mode
CMD ["standalone", "--root-path=/data"]