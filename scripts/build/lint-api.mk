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

LINT_OPTS ?= --timeout 1m0s

##@ Code quality targets

.PHONY: lint
lint: $(LINTER)
	@PATH=$(tool_bin):$(proto_dir) $(BUF) lint
	$(LINTER) --verbose run $(LINT_OPTS) --config $(root_dir)/deprecated-golangci.yml

.PHONY: format
format: $(LINTER)
	$(LINTER) --verbose run --fix -c $(root_dir)/.golangci-format.yml ./...
