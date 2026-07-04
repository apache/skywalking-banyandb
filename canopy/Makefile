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
repo_root := $(abspath $(mk_dir)../)

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

# license-check / license-fix delegate to the ROOT .licenserc.yaml instead of
# canopy/.licenserc.yaml, so the OMC runtime state + handoff / playwright
# artefacts (which live outside this directory but inside the repo) are
# excluded by patterns defined ONCE in the root config. This avoids editing
# ui/ or any other subproject's .licenserc.yaml.
.PHONY: license-check
license-check: $(LICENSE_EYE) ## Check license header
	$(LICENSE_EYE) -c $(repo_root).licenserc.yaml header check

.PHONY: license-fix
license-fix: $(LICENSE_EYE) ## Fix license header issues
	$(LICENSE_EYE) -c $(repo_root).licenserc.yaml header fix
