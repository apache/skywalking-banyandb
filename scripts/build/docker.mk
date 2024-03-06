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

ifndef IMG_NAME
$(error The IMG_NAME variable should be set)
endif

# The tag of the docker image. The default value if latest.
TAG ?= latest

IMG := $(HUB)/$(IMG_NAME):$(TAG)

# Disable cache in CI environment
ifeq (true,$(CI))
	ifeq ($(OS),Windows_NT)
		# windows not support buildx, so no "--load" option
		DOCKER_BUILD_ARGS := $(DOCKER_BUILD_ARGS) --no-cache
	else
		DOCKER_BUILD_ARGS := $(DOCKER_BUILD_ARGS) --no-cache --load
	endif
endif

DOCKER_FILE := Dockerfile
DOCKER_BUILD = docker buildx build
TIME = time
ifeq ($(OS),Windows_NT)
	# build docker using other Dockerfile and docker build command
	DOCKER_FILE = Dockerfile.windows
	DOCKER_BUILD = docker build
	# time is not support in the windows(CI environment)
	TIME =
endif

.PHONY: docker
docker:
	@echo "Build $(IMG)"
	$(TIME) $(DOCKER_BUILD) $(DOCKER_BUILD_ARGS) -t $(IMG) -f $(DOCKER_FILE) ..

.PHONY: docker.push
docker.push:
	@echo "Push $(IMG)"
	$(TIME) docker push $(IMG)

