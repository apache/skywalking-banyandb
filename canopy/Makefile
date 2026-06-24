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

NAME := canopy

mk_path  := $(abspath $(lastword $(MAKEFILE_LIST)))
mk_dir   := $(dir $(mk_path))
tool_bin := $(mk_dir)../bin

include ../scripts/build/license.mk

.PHONY: install
install:
	npm install

.PHONY: build
build: install
	npm run build

.PHONY: clean-build
clean-build:
	rm -rf web/dist server/dist

.PHONY: lint
lint: install
	npm run lint

.PHONY: test
test: install
	npm run test

.PHONY: license-check
license-check: $(LICENSE_EYE) ## Check license header
	$(LICENSE_EYE) header check

.PHONY: license-fix
license-fix: $(LICENSE_EYE) ## Fix license header issues
	$(LICENSE_EYE) header fix
