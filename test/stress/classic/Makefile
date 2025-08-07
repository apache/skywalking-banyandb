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

NAME := stress

mk_path  := $(abspath $(lastword $(MAKEFILE_LIST)))
mk_dir   := $(dir $(mk_path))

root_dir := $(mk_dir)../../..

CLI_ARGS :=

ifdef PROFILE
	CLI_ARGS := $(CLI_ARGS) --profile $(PROFILE)
endif

cli_env := DOCKER_BUILDKIT=1 USER_ID=$(shell id -u) GROUP_ID=$(shell id -g)

.PHONY: clean
clean:
	rm -rf /tmp/banyandb-stress-agent

.PHONY: build-server
build-server:
	(cd $(root_dir) && TARGET_OS=linux PLATFORMS="linux/amd64 linux/arm64" make release)

.PHONY: dev-up
dev-up: clean
	$(cli_env) docker compose $(CLI_ARGS) --env-file ./env.dev up --build

.PHONY: up
up: clean
	$(cli_env) docker compose $(CLI_ARGS) --env-file ./env up --build

.PHONY: down
down:
	docker compose down
