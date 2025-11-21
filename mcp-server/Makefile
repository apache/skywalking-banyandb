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

NAME := mcp-server
DIST_INDEX := dist/index.js

IMG_NAME := skywalking-banyandb-mcp-server

include ../scripts/build/version.mk
include ../scripts/build/base.mk
include ../scripts/build/docker.mk
include ../scripts/build/license.mk
include ../scripts/build/help.mk

.PHONY: check-version
check-version: install
	@node tools/checkversion

.PHONY: install
install:
	npm install

.PHONY: all
all: build

.PHONY: generate
generate: install

$(DIST_INDEX): install
	@echo "Building $(NAME)"
	npm run format:check
	npm run lint
	npm run build
	@echo "Done building $(NAME)"

.PHONY: build
build: $(DIST_INDEX)

.PHONY: clean-build
clean-build:
	@echo "Cleaning build artifacts"
	rm -rf dist

.PHONY: test
test:
	@echo "No test"

.PHONY: test-race
test-race:
	@echo "No test"

.PHONY: test-coverage
test-coverage:
	@echo "No test"

.PHONY: release
release: build

.PHONY: docker
docker: build

.PHONY: license-check
license-check: $(LICENSE_EYE) ## Check license header
	$(LICENSE_EYE) header check
 
.PHONY: license-fix
license-fix: $(LICENSE_EYE) ## Fix license header issues
	$(LICENSE_EYE) header fix

.PHONY: license-dep
license-dep:$(LICENSE_EYE)
	@rm -rf licenses
	$(LICENSE_EYE) dep resolve -o mcp-server-licenses -s LICENSE.tpl
	@mkdir -p $(root_dir)/dist/licenses
	@rm -rf $(root_dir)/dist/licenses/mcp-server-licenses
	@cp -r mcp-server-licenses $(root_dir)/dist/licenses/
	@rm -rf mcp-server-licenses

.PHONY: vendor-update
vendor-update:
	npm update

.PHONY: check
check:
	$(MAKE) check-version
	$(MAKE) license-check
	$(MAKE) test
	$(MAKE) test-coverage
	$(MAKE) license-dep
	$(MAKE) vendor-update

