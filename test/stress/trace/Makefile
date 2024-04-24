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
mk_path  := $(abspath $(lastword $(MAKEFILE_LIST)))
mk_dir   := $(dir $(mk_path))
root_dir := $(abspath $(mk_dir)/../../..)
tool_bin := $(root_dir)/bin

include $(root_dir)/scripts/build/version.mk
include $(root_dir)/scripts/build/ginkgo.mk

NAME := stress

CLI_ARGS :=

ifdef PROFILE
	CLI_ARGS := $(CLI_ARGS) --profile $(PROFILE)
endif

cli_env := DOCKER_BUILDKIT=1 USER_ID=$(shell id -u) GROUP_ID=$(shell id -g)

SIZE ?= 2

QPS ?= 10

GROUP ?= "default"

GINKGO_FLAGS ?=

.PHONY: clean
clean:
	rm -rf /tmp/banyandb-stress-trace

up-%: clean
	$(cli_env) docker compose -f docker-compose-$*.yaml $(CLI_ARGS) --env-file ./env up --build

down-%:
	docker compose -f docker-compose-$*.yaml down

.PHONY: test_traffic
test_traffic:
	curl -XPOST 'http://localhost:12800/mock-data/segments/tasks?size=$(SIZE)&group=$(GROUP)' -H'Content-Type: application/json' -d "@segment.tpl.json"

.PHONY: up_traffic
up_traffic:
	curl -XPOST 'http://localhost:12800/mock-data/segments/tasks?qps=$(QPS)&group=$(GROUP)' -H'Content-Type: application/json' -d "@segment.tpl.json"

.PHONY: ls_traffic
ls_traffic:
	curl -XGET 'http://localhost:12800/mock-data/segments/tasks'

.PHONY: rm_traffic
rm_traffic:
	curl -XDELETE 'http://localhost:12800/mock-data/segments/tasks'

.PHONY: test_query
test_query: $(GINKGO)
	$(GINKGO) $(GINKGO_FLAGS) -p -v -timeout 2h TestQuery ./...
	