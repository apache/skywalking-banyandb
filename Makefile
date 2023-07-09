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
tool_bin := $(mk_dir)bin

ifneq (,$(wildcard $(mk_dir).env))
include $(mk_dir).env
export
endif

include scripts/build/version.mk

PROJECTS := ui banyand bydbctl

TEST_CI_OPTS ?=

##@ Build targets

clean: TARGET=clean
clean: PROJECTS:=api $(PROJECTS) pkg
clean: default  ## Clean artifacts in all projects
	rm -rf build
	rm -f .env
	rm -f *.out

generate: TARGET=generate
generate: PROJECTS:=api $(PROJECTS) pkg
generate: default  ## Generate API codes

build: TARGET=all
build: default  ## Build all projects

##@ Release targets

release: TARGET=release
release: default  ## Build the release artifacts for all projects, usually the statically linked binaries

##@ Test targets

test: TARGET=test
test: PROJECTS:=$(PROJECTS) pkg test
test: default          ## Run the unit tests in all projects

test-race: TARGET=test-race
test-race: PROJECTS:=$(PROJECTS) pkg test
test-race: default     ## Run the unit tests in all projects with race detector on

test-coverage: TARGET=test-coverage
test-coverage: PROJECTS:=$(PROJECTS) pkg test
test-coverage: default ## Run the unit tests in all projects with coverage analysis on

include scripts/build/ginkgo.mk

test-ci: $(GINKGO) ## Run the unit tests in CI
	$(GINKGO) --race \
	  -ldflags \
	  "-X github.com/apache/skywalking-banyandb/pkg/test/flags.eventuallyTimeout=30s -X github.com/apache/skywalking-banyandb/pkg/test/flags.LogLevel=error" \
	  $(TEST_CI_OPTS) \
	  ./... 

##@ Code quality targets

lint: TARGET=lint
lint: PROJECTS:=api $(PROJECTS) pkg scripts/ci/check test
lint: default ## Run the linters on all projects

##@ Code style targets
tidy:
	go mod tidy

format: TARGET=format
format: PROJECTS:=api $(PROJECTS) pkg scripts/ci/check test
format: tidy
format: default ## Run the linters on all projects

check-req: ## Check the requirements
	@$(MAKE) -C scripts/ci/check test
	@$(MAKE) -C ui check-version

check: ## Check that the status is consistent with CI
	$(MAKE) license-check
	$(MAKE) format
	$(MAKE) tidy
	git add --renormalize .
	mkdir -p /tmp/artifacts
	git diff >/tmp/artifacts/check.diff 2>&1
	@if [ ! -z "`git status -s`" ]; then \
		echo "Following files are not consistent with CI:"; \
		git status -s; \
		cat /tmp/artifacts/check.diff; \
		exit 1; \
	fi

pre-push: ## Check source files before pushing to the remote repo
	$(MAKE) check-req
	$(MAKE) generate
	$(MAKE) lint
	$(MAKE) license-dep
	$(MAKE) check

##@ License targets

include scripts/build/license.mk

license-check: $(LICENSE_EYE)
license-check: TARGET=license-check
license-check: PROJECTS:=ui
license-check: default ## Check license header
	$(LICENSE_EYE) header check
 
license-fix: $(LICENSE_EYE)
license-fix: TARGET=license-fix
license-fix: PROJECTS:=ui
license-fix: default ## Fix license header issues
	$(LICENSE_EYE) header fix

license-dep: $(LICENSE_EYE)
license-dep: TARGET=license-dep
license-dep: PROJECTS:=ui
license-dep: default ## Fix license header issues
	@rm -rf $(mk_dir)/dist/licenses
	$(LICENSE_EYE) dep resolve -o $(mk_dir)/dist/licenses -s $(mk_dir)/dist/LICENSE.tpl
	mv $(mk_dir)/ui/ui-licenses $(mk_dir)/dist/licenses
	cat $(mk_dir)/ui/LICENSE >> $(mk_dir)/dist/LICENSE

##@ Docker targets

docker: TARGET=docker
docker: PROJECTS:=$(PROJECTS)
docker: default  ## Run docker for all projects

docker.push: TARGET=docker.push
docker.push: PROJECTS:=$(PROJECTS)
docker.push: default  ## Run docker.push for all projects

default:
	@for PRJ in $(PROJECTS); do \
		echo "--- $$PRJ: $(TARGET) ---"; \
		$(MAKE) $(TARGET) -C $$PRJ; \
		if [ $$? -ne 0 ]; then \
			exit 1; \
		fi; \
	done

nuke:
	git clean -Xdf

include scripts/build/help.mk

##@ release

RELEASE_SCRIPTS := $(mk_dir)/scripts/release.sh

release-binary: release-source ## Package binary archive
	${RELEASE_SCRIPTS} -b

release-source: clean ## Package source archive
	${RELEASE_SCRIPTS} -s

release-sign: ## Sign artifacts
	${RELEASE_SCRIPTS} -k bin
	${RELEASE_SCRIPTS} -k src

release-assembly: release-binary release-sign ## Generate release package


.PHONY: all $(PROJECTS) clean build  default nuke
.PHONY: lint check tidy format pre-push
.PHONY: test test-race test-coverage test-ci
.PHONY: license-check license-fix license-dep
.PHONY: release release-binary release-source release-sign release-assembly
