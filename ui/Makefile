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

NAME := ui

include ../scripts/build/base.mk
include ../scripts/build/docker.mk
include ../scripts/build/help.mk

.PHONY: generate
generate:
	@echo "No to generate"

.PHONY: all
all: build

.PHONY: install
install:
	yarn install

.PHONY: build
build: install
	@echo "Building $(NAME)"
	yarn build
	@echo "Done building $(NAME)"

.PHONY: test
test:
	@echo "No to test"

.PHONY: release
release: build

.PHONY: generate
generate: build

.PHONY: lint
lint:
	@echo "No to lint"

.PHONY: docker
docker:
	@echo "BanyanD will embed the UI"
