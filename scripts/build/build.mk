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
DEBUG_BINARIES ?= $(addsuffix -debug,$(BINARIES))
DEBUG_STATIC_BINARIES ?= $(addsuffix -static,$(DEBUG_BINARIES))
BUILD_DIR ?= build/bin
# Define SUB_DIR var if the project is not at root level project
SOURCE_DIR := $(if $(SUB_DIR),$(SUB_DIR)/$(NAME),$(NAME))
GOOS ?= linux
GOARCH ?= amd64

##@ Build targets

.PHONY: all
all: $(BINARIES)  ## Build all the binaries

$(BINARIES): $(NAME)-%: $(BUILD_DIR)/$(NAME)-%
$(addprefix $(BUILD_DIR)/,$(BINARIES)): $(BUILD_DIR)/$(NAME)-%:
	@echo "Building binary"
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -v -buildvcs=false --ldflags '${GO_LINK_VERSION}' -tags "$(BUILD_TAGS)" -o $@ github.com/apache/skywalking-banyandb/$(SOURCE_DIR)/cmd/$*
	chmod +x $@
	@echo "Done building $(NAME) $*"

.PHONY: debug
debug: $(DEBUG_BINARIES)  ## Build the debug binaries
$(DEBUG_BINARIES): $(NAME)-%-debug: $(BUILD_DIR)/$(NAME)-%-debug
$(addprefix $(BUILD_DIR)/,$(DEBUG_BINARIES)): $(BUILD_DIR)/$(NAME)-%-debug:
	@echo "Building debug binary"
	mkdir -p $(BUILD_DIR)
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -v -buildvcs=false --ldflags '${GO_LINK_VERSION}' -tags "$(BUILD_TAGS)" -gcflags='all=-N -l' -o $@ github.com/apache/skywalking-banyandb/$(SOURCE_DIR)/cmd/$*
	chmod +x $@
	@echo "Done building debug $(NAME) $*"

$(STATIC_BINARIES): $(NAME)-%-static: $(BUILD_DIR)/$(NAME)-%-static
$(addprefix $(BUILD_DIR)/,$(STATIC_BINARIES)): $(BUILD_DIR)/$(NAME)-%-static:
	@echo "Building static binary"
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
	        -buildvcs=false \
		-a --ldflags '${GO_LINK_VERSION} -extldflags "-static"' -tags "netgo $(BUILD_TAGS)" -installsuffix netgo \
		-o $(BUILD_DIR)/$(NAME)-$*-static github.com/apache/skywalking-banyandb/$(SOURCE_DIR)/cmd/$*
	chmod +x $(BUILD_DIR)/$(NAME)-$*-static
	@echo "Done building static $(NAME) $*"

.PHONY: debug-static
debug-static: $(DEBUG_STATIC_BINARIES)  ## Build the debug static binaries
$(DEBUG_STATIC_BINARIES): $(NAME)-%-debug-static: $(BUILD_DIR)/$(NAME)-%-debug-static
$(addprefix $(BUILD_DIR)/,$(DEBUG_STATIC_BINARIES)): $(BUILD_DIR)/$(NAME)-%-debug-static:
	@echo "Building debug static binary"
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
	        -buildvcs=false \
		-a --ldflags '${GO_LINK_VERSION} -extldflags "-static"' -tags "netgo $(BUILD_TAGS)" -gcflags='all=-N -l' -installsuffix netgo \
		-o $(BUILD_DIR)/$(NAME)-$*-debug-static github.com/apache/skywalking-banyandb/$(SOURCE_DIR)/cmd/$*
	chmod +x $(BUILD_DIR)/$(NAME)-$*-debug-static
	@echo "Done building debug static $(NAME) $*"

.PHONY: release
release: $(STATIC_BINARIES)   ## Build the release binaries

.PHONY: clean-build
clean-build:  ## Clean all artifacts
	rm -rf $(BUILD_DIR)
