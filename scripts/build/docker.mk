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
#

# The hub of the docker image. The default value is skywalking-banyandd.
# For github registry, it would be ghcr.io/apache/skywalking-banyandb
HUB ?= skywalking-banyandb

# The tag of the docker image. The default value if latest.
# For github actions, ${{ github.sha }} can be used for every commit.
TAG ?= latest

# Disable cache in CI environment
ifeq (true,$(CI))
	BUILD_ARGS := --no-cache --load
endif

BUILD_ARGS := $(BUILD_ARGS) --build-arg CERT_IMAGE=alpine:edge --build-arg BASE_IMAGE=golang:1.18

.PHONY: docker
docker:
	@echo "Build Skywalking/BanyanDB Docker Image"
	@time docker buildx build $(BUILD_ARGS) -t $(HUB):$(TAG) -f Dockerfile ..

.PHONY: docker.push
docker.push:
	@echo "Push Skywalking/BanyanDB Docker Image"
	@time docker push $(HUB):$(TAG)