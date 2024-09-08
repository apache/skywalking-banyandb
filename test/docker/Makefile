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

# The hub of the docker image. The default value is skywalking-.
# For github registry, it would be ghcr.io/apache/skywalking-
HUB ?= apache

# The tag of the docker image. The default value if latest.
TAG ?= latest

IMG := $(HUB)/skywalking-banyandb:$(TAG)-testing

build:
	@echo "Building $(IMG)"
	@pwd
	time docker buildx build --load --no-cache -t $(IMG) -f Dockerfile --provenance=false ../../

push:
	@echo "Pushing $(IMG) with platform linux/amd64,linux/arm64"
	@pwd
	time docker buildx build --platform linux/amd64,linux/arm64 --push -t $(IMG) -f Dockerfile --provenance=false ../../
