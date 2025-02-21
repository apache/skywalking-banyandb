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

ifndef NAME
$(error The NAME variable should be set)
endif

ifndef BINARIES
$(error The BINARIES variable should be set to the name binaries to produce)
endif

STATIC_BINARIES ?= $(addsuffix -static,$(BINARIES))
SLIM_BINARIES ?= $(addsuffix -slim,$(BINARIES))
BUILD_DIR ?= build/bin
TARGET_OS ?= linux
OS := ${TARGET_OS}
# Define SUB_DIR var if the project is not at root level project
SOURCE_DIR ?= $(if $(SUB_DIR),$(SUB_DIR)/$(NAME),$(NAME))

# Sentinel to guard against stale compiled binaries
BUILD_LOCK := $(BUILD_DIR)/$(shell git rev-parse HEAD).lock

# Build Go binaries for multiple architectures in the CI pipeline.
GOBUILD_ARCHS := $(shell echo "${PLATFORMS}" | sed "s/linux\///g; s/darwin\///g; s/windows\///g; s/\,/ /g" | tr ' ' '\n' | sort | uniq | tr '\n' ' ')

##@ Build targets

.PHONY: all
all: $(BINARIES)  ## Build all the binaries
BINARIES_GOBUILD_TARGET_PATTERN := $(BUILD_DIR)/dev/$(NAME)-%
BINARIES_GOBUILD_TARGET := $(addprefix $(BUILD_DIR)/dev/,$(BINARIES))
$(BINARIES): $(NAME)-%: $(BINARIES_GOBUILD_TARGET_PATTERN)
$(BINARIES_GOBUILD_TARGET): $(BUILD_LOCK)
	$(call set_build_package,$@,$@)
	@echo "Building binary $@"
	$(MAKE) prepare-build
	$(call go_build_executable)
	@echo "Done building $@"

STATIC_BINARIES_GOBUILD_TARGET_PATTERN := $(foreach goarch,$(GOBUILD_ARCHS),$(BUILD_DIR)/$(OS)/$(goarch)/$(NAME)-%-static)
STATIC_BINARIES_GOBUILD_TARGET := $(foreach goarch,$(GOBUILD_ARCHS),$(addprefix $(BUILD_DIR)/$(OS)/$(goarch)/,$(STATIC_BINARIES)))
$(STATIC_BINARIES): $(NAME)-%-static: $(STATIC_BINARIES_GOBUILD_TARGET_PATTERN)
$(STATIC_BINARIES_GOBUILD_TARGET): $(BUILD_DIR)/$(OS)/%-static: $(BUILD_LOCK)
	$(call set_build_package,$*,$@)
	@echo "Building static $*"
	$(MAKE) prepare-build
	$(call go_build_static_executable,,-s -w)
	@echo "Done building static $*"

SLIM_BINARIES_GOBUILD_TARGET_PATTERN := $(foreach goarch,$(GOBUILD_ARCHS),$(BUILD_DIR)/$(OS)/$(goarch)/$(NAME)-%-slim)
SLIM_BINARIES_GOBUILD_TARGET := $(foreach goarch,$(GOBUILD_ARCHS),$(addprefix $(BUILD_DIR)/$(OS)/$(goarch)/,$(SLIM_BINARIES)))
$(SLIM_BINARIES): $(NAME)-%-slim: $(SLIM_BINARIES_GOBUILD_TARGET_PATTERN)
$(SLIM_BINARIES_GOBUILD_TARGET): $(BUILD_DIR)/$(OS)/%-slim: $(BUILD_LOCK)
	$(call set_build_package,$*,$@)
	@echo "Building slim $*"
	$(MAKE) prepare-build
	$(if $(filter slim,$(BUILD_TAGS)),, $(eval BUILD_TAGS := $(BUILD_TAGS) slim))
	$(call go_build_static_executable,,-s -w)
	@echo "Done building static $*"

.PHONY: release
release: $(STATIC_BINARIES) ## Build the release binaries

.PHONY: clean-build
clean-build:  ## Clean all artifacts
	rm -rf $(BUILD_DIR)

# Helper target to prevent stale binaries from being packaged in Docker images.
# This file is used as a sentinel for the last Git SHA of the built binaries. If it does
# not match the current Git SHA, it means the existing compiled binaries are stale and should
# be rebuilt again.
$(BUILD_LOCK):
	@echo "cleaning up stale build artifacts..."
	$(MAKE) clean-build
	mkdir -p $(BUILD_DIR)
	touch $@

# Helper functions for go build.
# Takes in arguments 1. output binary name suffix 2. additional options to the go build function.
define go_build_executable
	go build \
		-v -ldflags '${GO_LINK_VERSION}' -tags "$(BUILD_TAGS)" $(1) \
		-o $@ $(MODULE_PATH)/$(SOURCE_DIR)/cmd/$(build_package);
	chmod +x $@;
endef

define go_build_static_executable
	CGO_ENABLED=0 GOOS=${TARGET_OS} GOARCH=$(target_arch) go build \
		$(fastbuild) -ldflags '${2} ${GO_LINK_VERSION} -extldflags "-static"' \
		-tags "netgo $(BUILD_TAGS)" -installsuffix netgo $(1)\
		-o $@ $(MODULE_PATH)/$(SOURCE_DIR)/cmd/$(build_package);
	chmod +x $@;
endef


# Sets the package name to be built as part of building $(NAME) project and
# target architecture to be build for.
define set_build_package
	$(eval build_package := $(word 2,$(subst $(NAME)-, ,$1)))
	$(eval target_arch := $(word 4,$(subst /, ,$2)))
endef
