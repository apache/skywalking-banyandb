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

TEST_OPTS ?= -v
TEST_EXTRA_OPTS ?=
TEST_TAGS ?= $(BUILD_TAGS)
TEST_PKG_LIST ?= ./...

TEST_COVERAGE_DIR ?= build/coverage
TEST_COVERAGE_PROFILE_NAME := coverprofile.out
TEST_COVERAGE_PROFILE := $(TEST_COVERAGE_DIR)/$(TEST_COVERAGE_PROFILE_NAME)
TEST_COVERAGE_REPORT := $(TEST_COVERAGE_DIR)/coverage.html
TEST_COVERAGE_PKG_LIST ?= $(TEST_PKG_LIST)
TEST_COVERAGE_OPTS ?= --covermode atomic --coverpkg ./...
TEST_COVERAGE_EXTRA_OPTS ?=

##@ Test targets

include $(root_dir)/scripts/build/ginkgo.mk

.PHONY: test
test: $(GINKGO) generate ## Run all the unit tests
	$(GINKGO) $(TEST_OPTS) $(TEST_EXTRA_OPTS) -tags "$(TEST_TAGS)" $(TEST_PKG_LIST)

.PHONY: test-race
test-race: TEST_OPTS=--race
test-race: test  ## Run all the unit tests with race detector on

.PHONY: test-coverage
test-coverage: $(GINKGO) generate ## Run all the unit tests with coverage analysis on
	mkdir -p "$(TEST_COVERAGE_DIR)"
	$(GINKGO) --cover $(TEST_COVERAGE_OPTS) $(TEST_COVERAGE_EXTRA_OPTS)  --tags "$(TEST_TAGS)" $(TEST_COVERAGE_PKG_LIST) \
	   && mv $(TEST_COVERAGE_PROFILE_NAME) $(TEST_COVERAGE_DIR)
	go tool cover -html="$(TEST_COVERAGE_PROFILE)" -o "$(TEST_COVERAGE_REPORT)"
	@echo "Test coverage report has been saved to $(TEST_COVERAGE_REPORT)"

.PHONY: test-clean
test-clean::  ## Clean all test artifacts
	rm -rf $(TEST_COVERAGE_DIR)
	