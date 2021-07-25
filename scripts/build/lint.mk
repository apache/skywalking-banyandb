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

lint_mk_path := $(abspath $(lastword $(MAKEFILE_LIST)))
lint_mk_dir  := $(dir $(lint_mk_path))
root_dir     := $(lint_mk_dir)../..

GOLANGCI_VERSION := v1.39.0

LINT_OPTS ?= --timeout 1m0s

##@ Code quality targets

LINTER := $(root_dir)/bin/golangci-lint
$(LINTER):
	wget -O - -q https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(root_dir)/bin $(GOLANGCI_VERSION)

.PHONY: lint
lint: $(LINTER) ## Run all the linters
	go install github.com/golang/mock/mockgen@v1.6.0
	go generate $(TEST_PKG_LIST) 
	$(LINTER) --verbose run $(LINT_OPTS) --config $(root_dir)/golangci.yml
