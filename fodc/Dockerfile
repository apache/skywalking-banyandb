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
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the License.

FROM alpine:edge AS certs
RUN apk add --no-cache ca-certificates && update-ca-certificates

FROM busybox:stable-glibc AS build-linux

ARG TARGETARCH
ARG BINARYTYPE

COPY build/bin/linux/${TARGETARCH}/fodc-cli-${BINARYTYPE} /fodc-cli
COPY build/bin/linux/${TARGETARCH}/fodc-view-${BINARYTYPE} /fodc-view
COPY --from=certs /etc/ssl/certs /etc/ssl/certs

FROM mcr.microsoft.com/windows/servercore:ltsc2022 AS build-windows

ARG TARGETARCH

COPY build/bin/windows/${TARGETARCH}/fodc-cli-static "/fodc-cli"
COPY build/bin/windows/${TARGETARCH}/fodc-view-static "/fodc-view"

FROM build-${TARGETOS} AS final

# Install wget for health checks (optional, can be removed if not needed)
RUN if [ "${TARGETOS}" = "linux" ]; then \
      apk add --no-cache wget; \
    fi

EXPOSE 17914

# Default command runs in sidecar mode
ENTRYPOINT ["/fodc-cli"]
CMD ["--sidecar"]

