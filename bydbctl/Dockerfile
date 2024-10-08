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


FROM alpine:edge AS certs
RUN apk add --no-cache ca-certificates && update-ca-certificates

FROM busybox:stable-glibc AS build-linux

ARG TARGETARCH

COPY build/bin/linux/${TARGETARCH}/bydbctl-cli-static /bydbctl
COPY --from=certs /etc/ssl/certs /etc/ssl/certs

FROM mcr.microsoft.com/windows/servercore:ltsc2022 AS build-windows

ARG TARGETARCH

COPY build/bin/windows/${TARGETARCH}/bydbctl-cli-static "/bydbctl"

FROM build-${TARGETOS} AS final

ENTRYPOINT ["/bydbctl"]
