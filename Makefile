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

PROJECTS := banyand

##@ Build targets

clean: TARGET=clean
clean: PROJECTS:=$(PROJECTS) pkg
clean: default  ## Clean artifacts in all projects
	find . -type s -name 'localhost:*' -delete

generate: TARGET=generate
generate: PROJECTS:=api $(PROJECTS) pkg
generate: default  ## Generate API codes
	@$(MAKE) format

build: TARGET=all
build: default  ## Build all projects

##@ Release targets

release: TARGET=release
release: default  ## Build the release artifacts for all projects, usually the statically linked binaries

##@ Test targets

test: TARGET=test
test: PROJECTS:=$(PROJECTS) pkg
test: default          ## Run the unit tests in all projects

test-race: TARGET=test-race
test-race: default     ## Run the unit tests in all projects with race detector on

test-coverage: TARGET=test-coverage
test-coverage: default ## Run the unit tests in all projects with coverage analysis on

##@ Code quality targets

lint: TARGET=lint
lint: PROJECTS:=api $(PROJECTS) pkg
lint: default ## Run the linters on all projects

##@ Code style targets

# The goimports tool does not arrange imports in 3 blocks if there are already more than three blocks.
# To avoid that, before running it, we collapse all imports in one block, then run the formatter.
format: ## Format all Go code
	@for f in `find . -name '*.go'`; do \
	    awk '/^import \($$/,/^\)$$/{if($$0=="")next}{print}' $$f > /tmp/fmt; \
	    mv /tmp/fmt $$f; \
	done
	@goimports -w -local github.com/apache/skywalking-banyandb .

# Enforce go version matches what's in go.mod when running `make check` assuming the following:
# * 'go version' returns output like "go version go1.17 darwin/amd64"
# * go.mod contains a line like "go 1.17"
CONFIGURED_GO_VERSION := $(shell sed -ne '/^go /s/.* //gp' go.mod)
EXPECTED_GO_VERSION_PREFIX := "go version go$(CONFIGURED_GO_VERSION)"
GO_VERSION := $(shell go version)

## Check that the status is consistent with CI.
check: clean
# case statement because /bin/sh cannot do prefix comparison, awk is awkward and assuming /bin/bash is brittle
	@case "$(GO_VERSION)" in $(EXPECTED_GO_VERSION_PREFIX)* ) ;; * ) \
		echo "Expected 'go version' to start with $(EXPECTED_GO_VERSION_PREFIX), but it didn't: $(GO_VERSION)"; \
		echo "Upgrade go to $(CONFIGURED_GO_VERSION)+"; \
		exit 1; \
	esac
	$(MAKE) format
	mkdir -p /tmp/artifacts
	git diff >/tmp/artifacts/check.diff 2>&1
	go mod tidy
	@if [ ! -z "`git status -s`" ]; then \
		echo "Following files are not consistent with CI:"; \
		git status -s; \
		cat /tmp/artifacts/check.diff; \
		exit 1; \
	fi
	
pre-push: generate lint license-check check ## Check source files before pushing to the remote repo

##@ License targets

LICENSE_EYE := $(tool_bin)/license-eye
$(LICENSE_EYE):
	@echo "Install license-eye..."
	@mkdir -p $(tool_bin)
	@GOBIN=$(tool_bin) go install github.com/apache/skywalking-eyes/cmd/license-eye@db412b1a98b090485f6fa5e45e61e00b04c03ba5

license-check: $(LICENSE_EYE) ## Check license header
	$(LICENSE_EYE) header check
 
license-fix: $(LICENSE_EYE) ## Fix license header issues
	$(LICENSE_EYE) header fix

license-dep: $(LICENSE_EYE) ## Fix license header issues
	$(LICENSE_EYE) dep resolve -o $(mk_dir)/dist/licenses -s $(mk_dir)/dist/LICENSE.tpl

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
	find . -type s -name 'localhost:*' -delete

include scripts/build/help.mk

##@ release

RELEASE_SCRIPTS := $(mk_dir)/scripts/release.sh

release-binary: release ## Package binary archive
	${RELEASE_SCRIPTS} -b

release-source: clean ## Package source archive
	${RELEASE_SCRIPTS} -s

release-sign: ## Sign artifacts
	${RELEASE_SCRIPTS} -k bin
	${RELEASE_SCRIPTS} -k src

release-assembly: release-binary release-source release-sign ## Generate release package


.PHONY: all $(PROJECTS) clean build release test test-race test-coverage lint default check format license-check license-fix pre-commit nuke
.PHONY: release-binary release-source release-sign release-assembly
