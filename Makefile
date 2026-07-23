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

PROJECTS := ui banyand bydbctl mcp fodc/agent fodc/proxy

TEST_CI_OPTS ?=

##@ Build targets

clean: TARGET=clean
clean: PROJECTS:=api $(PROJECTS) pkg
clean: default  ## Clean artifacts in all projects
	rm -rf build
	rm -f .env
	rm -f *.out

clean-build: TARGET=clean-build
clean-build: default  ## Clean build artifacts in all projects

generate: TARGET=generate
generate: PROJECTS:=api $(PROJECTS) pkg
generate: default  ## Generate API codes

generate-test-cases:  ## Regenerate measure query test cases (input/*.ql, input/*.yaml)
	go run ./test/cases/measure/cmd/generate generate test/cases/measure/data

capture-test-cases:  ## Capture measure query want fixtures (data/want/*.yaml) by running queries against a standalone server
	CAPTURE_WANT_FIXTURES=1 go test -count=1 -timeout 5m -run TestCapture ./test/cases/measure/cmd/capture/

generate-trace-test-cases:  ## Regenerate trace query test cases (input/*.ql, input/*.yml)
	go run ./test/cases/trace/cmd/generate generate test/cases/trace/data

capture-trace-test-cases:  ## Capture trace query want fixtures (data/want/*.yml) against a standalone server
	CAPTURE_TRACE_WANT_FIXTURES=1 go test -count=1 -timeout 5m -run TestCaptureTrace ./test/cases/trace/cmd/capture/

generate-stream-test-cases:  ## Regenerate stream query test cases (input/*.ql, input/*.yaml)
	go run ./test/cases/stream/cmd/generate generate test/cases/stream/data

capture-stream-test-cases:  ## Capture stream query want fixtures (data/want/*.yaml) against a standalone server
	CAPTURE_STREAM_WANT_FIXTURES=1 go test ./test/cases/stream/cmd/capture/ -args test/cases/stream/data

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

test-ci: $(GINKGO) ## Run the unit tests in CI. Usage: make test-ci PKG=./banyand/trace
	$(GINKGO) --race \
	  -ldflags \
	  "-X github.com/apache/skywalking-banyandb/pkg/test/flags.eventuallyTimeout=30s -X github.com/apache/skywalking-banyandb/pkg/test/flags.consistentlyTimeout=10s -X github.com/apache/skywalking-banyandb/pkg/test/flags.LogLevel=error" \
	  $(TEST_CI_OPTS) \
	  $(PKG) 

PKG ?= ./...
GO_VERSION := $(shell grep -E '^go [0-9]+\.[0-9]+' go.mod | awk '{print $$2}')
test-docker: ## Run tests in Docker with constrained resources (2 CPU cores, 4GB RAM). Usage: make test-docker PKG=./banyand/trace
	@echo "Running tests in Docker container (2 CPUs, 4GB RAM) for package: $(PKG)"
	@echo "Using Go version: $(GO_VERSION) (from go.mod)"
	docker run --rm \
	  --cpus=2 \
	  --memory=4g \
	  -v $(mk_dir):/workspace \
	  -w /workspace \
	  golang:$(GO_VERSION) \
	  go run github.com/onsi/ginkgo/v2/ginkgo --race \
	  -ldflags "-X github.com/apache/skywalking-banyandb/pkg/test/flags.eventuallyTimeout=30s -X github.com/apache/skywalking-banyandb/pkg/test/flags.consistentlyTimeout=10s -X github.com/apache/skywalking-banyandb/pkg/test/flags.LogLevel=error" \
	  $(PKG)

load-test-barrier: ## Run the schema-barrier CP-6 SLO load harness (3 data nodes + 1 liaison, 100 callers, 5m measurement). Override with LOAD_FLAGS="-loadtest.measure=30s ..." for smoke runs.
	@echo "Running schema-barrier load harness (CP-6 SLO profile)"
	@echo "Override profile via LOAD_FLAGS, e.g. LOAD_FLAGS='-loadtest.measure=30s -loadtest.callers=20'"
	go test -tags=loadtest -timeout 30m -count=1 -v ./test/load/schema_barrier/... $(LOAD_FLAGS)

##@ Trace pipeline plugin targets
# Linux and macOS only — Go plugins require CGO and are not supported on Windows.
# These targets MUST NOT be included in test-race: a plugin built without -race
# cannot be loaded by a -race host (the Go runtime rejects the mismatch with a
# "plugin was built with a different version of package runtime" error at
# plugin.Open time).  The in-package guards in banyand/trace (diskSurvivorSpans,
# TestInMergeFilter_DisabledFlag_LegacyIdentity, TestInMergeFilter_FailOpenNegativeControl)
# remain the sole race-checked coverage of the merge filter path.

PLUGIN_OUTPUT_DIR ?= build/bin/plugins
BANYAND_SERVER_CGO_BIN ?= banyand/build/bin/dev/banyand-server

.PHONY: build-trace-pipeline-plugin
build-trace-pipeline-plugin: ## Build the latencystatussampler.so plugin (Linux/macOS only; requires a C toolchain)
	@if ! command -v gcc > /dev/null 2>&1 && ! command -v clang > /dev/null 2>&1; then \
		echo "ERROR: build-trace-pipeline-plugin requires a C toolchain (gcc or clang) but neither was found in PATH."; \
		exit 1; \
	fi
	@mkdir -p $(PLUGIN_OUTPUT_DIR)
	CGO_ENABLED=1 go build -buildmode=plugin -trimpath \
		-o $(PLUGIN_OUTPUT_DIR)/latencystatussampler.so \
		./test/plugins/_latencystatussampler
	@echo "Built $(PLUGIN_OUTPUT_DIR)/latencystatussampler.so"

.PHONY: build-trace-pipeline-telemetry-plugins
build-trace-pipeline-telemetry-plugins: ## Build telemetrysampler.so and faultysampler.so reference plugins (Linux/macOS only; requires a C toolchain)
	@if ! command -v gcc > /dev/null 2>&1 && ! command -v clang > /dev/null 2>&1; then \
		echo "ERROR: build-trace-pipeline-telemetry-plugins requires a C toolchain (gcc or clang) but neither was found in PATH."; \
		exit 1; \
	fi
	@mkdir -p $(PLUGIN_OUTPUT_DIR)
	CGO_ENABLED=1 go build -buildmode=plugin -trimpath \
		-o $(PLUGIN_OUTPUT_DIR)/telemetrysampler.so \
		./test/plugins/_telemetrysampler
	@echo "Built $(PLUGIN_OUTPUT_DIR)/telemetrysampler.so"
	CGO_ENABLED=1 go build -buildmode=plugin -trimpath \
		-o $(PLUGIN_OUTPUT_DIR)/faultysampler.so \
		./test/plugins/_faultysampler
	@echo "Built $(PLUGIN_OUTPUT_DIR)/faultysampler.so"

.PHONY: build-plugins
build-plugins: ## Build every plugins/<vendor>/<name> sampler into $(PLUGIN_OUTPUT_DIR) (Linux/macOS only; requires a C toolchain)
	@if ! command -v gcc > /dev/null 2>&1 && ! command -v clang > /dev/null 2>&1; then \
		echo "ERROR: build-plugins requires a C toolchain (gcc or clang) but neither was found in PATH."; \
		exit 1; \
	fi
	@mkdir -p $(PLUGIN_OUTPUT_DIR)
	@set -e; for dir in plugins/*/*/; do \
		[ -f "$$dir/main.go" ] || continue; \
		name=$$(basename "$$dir"); \
		echo "Building $(PLUGIN_OUTPUT_DIR)/$$name.so from $$dir"; \
		CGO_ENABLED=1 go build -buildmode=plugin -trimpath \
			-o $(PLUGIN_OUTPUT_DIR)/$$name.so \
			./$$dir; \
	done
	@echo "Built plugins into $(PLUGIN_OUTPUT_DIR)"

.PHONY: build-trace-pipeline-server
build-trace-pipeline-server: ## Build banyand-server with explicit CGO_ENABLED=1 for plugin hosting (Linux/macOS only)
	@if ! command -v gcc > /dev/null 2>&1 && ! command -v clang > /dev/null 2>&1; then \
		echo "ERROR: build-trace-pipeline-server requires a C toolchain (gcc or clang) but neither was found in PATH."; \
		exit 1; \
	fi
	@# ui/dist must contain at least one embeddable (non-hidden) file for
	@# ui/embed.go's //go:embed dist to succeed.  Create a placeholder when the
	@# UI has not been built (dev / CI without the UI step).
	@mkdir -p ui/dist
	@if [ ! -f ui/dist/index.html ]; then touch ui/dist/index.html; fi
	@mkdir -p $(dir $(BANYAND_SERVER_CGO_BIN))
	CGO_ENABLED=1 go build -trimpath \
		-o $(BANYAND_SERVER_CGO_BIN) \
		github.com/apache/skywalking-banyandb/banyand/cmd/server
	@echo "Built $(BANYAND_SERVER_CGO_BIN)"

.PHONY: test-trace-pipeline
# NOTE: no -race flag — see comment above.  Plugins and -race require both the
# .so and the host to be race-built; the simple CGO_ENABLED=1 targets here do
# not pass -race, so the suite is intentionally excluded from the race lane.
test-trace-pipeline: build-trace-pipeline-plugin build-trace-pipeline-server $(GINKGO) ## Build the .so + CGO server, then run the pipeline integration suites (Linux/macOS only; excluded from test-race)
	BANYAND_BIN=$(mk_dir)$(BANYAND_SERVER_CGO_BIN) \
	BANYAND_TRACE_PLUGIN=$(mk_dir)$(PLUGIN_OUTPUT_DIR)/latencystatussampler.so \
	$(GINKGO) \
	  --tags trace_pipeline \
	  -ldflags "-X github.com/apache/skywalking-banyandb/pkg/test/flags.eventuallyTimeout=30s -X github.com/apache/skywalking-banyandb/pkg/test/flags.consistentlyTimeout=10s -X github.com/apache/skywalking-banyandb/pkg/test/flags.LogLevel=error" \
	  -timeout 10m \
	  ./test/integration/standalone/pipeline/... \
	  ./test/integration/distributed/pipeline/...

##@ Code quality targets

lint: TARGET=lint
lint: PROJECTS:=api $(PROJECTS) pkg scripts/ci/check test
lint: check-import-boundaries
lint: default ## Run the linters on all projects

# lint-rawgo enforces the project's "no raw goroutines" rule. New `go`
# statements in code outside the recovery wrappers must either go
# through run.Go / run.GoOrDie / run.GoWithSignal or carry an explicit
# `//panicdiag:allow-rawgo <reason>` directive. Pre-existing sites are
# tracked in pkg/panicdiag/lintrawgo/baseline.txt; that list only ever
# shrinks. Runs once at the module root rather than per-subproject.
.PHONY: lint-rawgo
lint-rawgo: ## Enforce panic-recovery wrappers for goroutine launches
	go run ./scripts/lint/rawgo \
	  -baseline=pkg/panicdiag/lintrawgo/baseline.txt ./...

.PHONY: update-rawgo-baseline
update-rawgo-baseline: ## Regenerate the raw-go baseline from the current tree
	go run ./scripts/lint/rawgo-baseline \
	  -baseline=pkg/panicdiag/lintrawgo/baseline.txt ./...

# check-import-boundaries enforces the layering invariants documented in
# pkg/initerror/initerror.go: the leaf permanent-error contract must not gain
# project-internal dependencies, and the property schema-registry classifier
# must not import the storage internals it classifies via the leaf interface.
.PHONY: check-import-boundaries
check-import-boundaries: ## Enforce import-boundary invariants for the version-incompat fix
	@bad=0; \
	if grep -rln 'banyand/internal/storage' banyand/metadata/schema/property/ 2>/dev/null; then \
		echo "FAIL: banyand/metadata/schema/property/ must not import banyand/internal/storage"; \
		bad=1; \
	fi; \
	if grep -rln 'banyand/internal/storage' pkg/schema/ 2>/dev/null; then \
		echo "FAIL: pkg/schema/ must not import banyand/internal/storage"; \
		bad=1; \
	fi; \
	if grep -rln 'github.com/apache/skywalking-banyandb/' pkg/initerror/*.go 2>/dev/null | grep -v _test.go; then \
		echo "FAIL: pkg/initerror/ must remain a leaf with zero project-internal imports (test files excepted)"; \
		bad=1; \
	fi; \
	if [ $$bad -ne 0 ]; then exit 1; fi; \
	echo "import boundaries OK"

##@ Vendor update

vendor-update: TARGET=vendor-update
vendor-update: PROJECTS:=$(PROJECTS) pkg test
vendor-update: default ## Run the linters on all projects

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
	@$(MAKE) -C mcp check-version

include scripts/build/vuln.mk

vuln-check: $(GOVULNCHECK)
	$(GOVULNCHECK) -show color,verbose ./...	

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
	$(MAKE) generate-test-cases
	$(MAKE) lint
	$(MAKE) check
	$(MAKE) vuln-check

##@ License targets

include scripts/build/license.mk

# License-check / license-fix run a SINGLE license-eye invocation from the
# repo root with the root .licenserc.yaml. This avoids:
#   - editing ui/.licenserc.yaml (forbidden by plan §Principle 3),
#   - the per-subdir loop over PROJECTS (each subdir would otherwise load
#     its own .licenserc.yaml and miss the root config's OMC-runtime-state
#     / handoff-import / playwright-mcp exclusions).
# The root config already includes 'ui' in paths-ignore so the Vue app is
# not double-scanned; canopy files are scanned from the root, which is the
# desired surface for the license header check.
license-check: $(LICENSE_EYE) ## Check license header
	$(LICENSE_EYE) header check

license-fix: $(LICENSE_EYE) ## Fix license header issues
	$(LICENSE_EYE) header fix
	$(LICENSE_EYE) header fix

license-dep: $(LICENSE_EYE)
license-dep: TARGET=license-dep
license-dep: PROJECTS:=ui mcp
license-dep: default ## Fix license header issues
	@rm -rf $(mk_dir)/dist/licenses
	$(LICENSE_EYE) dep resolve -o $(mk_dir)/dist/licenses -s $(mk_dir)/dist/LICENSE.tpl
	mv $(mk_dir)/ui/ui-licenses $(mk_dir)/dist/licenses
	cat $(mk_dir)/ui/LICENSE >> $(mk_dir)/dist/LICENSE
	mv $(mk_dir)/mcp/mcp-licenses $(mk_dir)/dist/licenses
	cat $(mk_dir)/mcp/LICENSE >> $(mk_dir)/dist/LICENSE

##@ Docker targets

docker.build: TARGET=docker
docker.build: PROJECTS:= banyand bydbctl mcp fodc/agent fodc/proxy
docker.build: default ## Build docker images

docker.push: TARGET=docker.push
docker.push: PROJECTS:= banyand bydbctl mcp fodc/agent fodc/proxy
docker.push: default ## Push docker images

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

release-source: ## Package source archive
	${RELEASE_SCRIPTS} -s

release-sign: ## Sign artifacts
	${RELEASE_SCRIPTS} -k banyand
	${RELEASE_SCRIPTS} -k bydbctl
	${RELEASE_SCRIPTS} -k fodc-agent
	${RELEASE_SCRIPTS} -k fodc-proxy
	${RELEASE_SCRIPTS} -k src

release-assembly: release-binary release-sign ## Generate release package

PUSH_RELEASE_SCRIPTS := $(mk_dir)/scripts/push-release.sh

release-push-candidate: ## Push release candidate
	${PUSH_RELEASE_SCRIPTS}
	
.PHONY: all $(PROJECTS) clean build  default nuke
.PHONY: lint check tidy format pre-push generate-test-cases capture-test-cases generate-trace-test-cases capture-trace-test-cases generate-stream-test-cases capture-stream-test-cases check-import-boundaries
.PHONY: test test-race test-coverage test-ci test-docker
.PHONY: build-trace-pipeline-plugin build-trace-pipeline-telemetry-plugins build-trace-pipeline-server test-trace-pipeline
.PHONY: license-check license-fix license-dep
.PHONY: release release-binary release-source release-sign release-assembly
.PHONY: vendor-update
