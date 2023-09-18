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

include $(mk_dir)lint-bin.mk

##@ Code quality targets



.PHONY: lint
lint: $(LINTER) $(REVIVE) ## Run all linters
	$(LINTER) run -v --config $(root_dir)/.golangci.yml --timeout 10m ./... && \
	  $(REVIVE) -config $(root_dir)/revive.toml -formatter friendly ./...

.PHONY: format
format: $(LINTER)
	$(LINTER) run --fix -c $(root_dir)/.golangci-format.yml ./...
