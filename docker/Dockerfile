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
ARG NODE_VERSION=18.16.0
FROM node:${NODE_VERSION}-alpine AS node

FROM golang:alpine3.18 AS builder
RUN apk add bash git openssh github-cli make

COPY --from=node /usr/lib /usr/lib
COPY --from=node /usr/local/lib /usr/local/lib
COPY --from=node /usr/local/include /usr/local/include
COPY --from=node /usr/local/bin /usr/local/bin

ENV GOPATH "/go"
ENV GO111MODULE "on"
WORKDIR /src
COPY ./ ./

RUN make generate

RUN make -C banyand release

FROM alpine:3

COPY --from=builder /src/banyand/build/bin/banyand-server-static /banyand
COPY --from=builder /src/bydbctl/build/bin/bydbctl /bydbctl


EXPOSE 17912
EXPOSE 17913
EXPOSE 6060

ENTRYPOINT ["/banyand"]
