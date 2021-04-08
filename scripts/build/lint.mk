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
	$(LINTER) --verbose run $(LINT_OPTS) --config $(root_dir)/golangci.yml
