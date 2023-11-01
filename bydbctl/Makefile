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

NAME := bydbctl
IMG_NAME := skywalking-bydbctl

BUILD_DIR ?= build/bin

include ../scripts/build/version.mk
include ../scripts/build/base.mk
include ../scripts/build/generate_go.mk

.PHONY: all
all: build

.PHONY: build
build: 
	@echo "Building binary"
	go build -v --ldflags '${GO_LINK_VERSION}' -o $(BUILD_DIR)/$(NAME) github.com/apache/skywalking-banyandb/bydbctl/cmd/bydbctl/$*
	chmod +x $(BUILD_DIR)/$(NAME)
	@echo "Done building $(NAME)"

BUILDS := darwin-amd64 darwin-arm64 linux-386 linux-amd64 linux-arm64 windows-386 windows-amd64
BUILD_RULE = GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -v --ldflags '$(GO_LINK_VERSION)' \
		-o $(BUILD_DIR)/$(NAME)-$(VERSION_STRING)-$(GOOS)-$(GOARCH) \
		github.com/apache/skywalking-banyandb/bydbctl/cmd/bydbctl/$*

.PHONY: $(BUILDS)
$(BUILDS): GOOS = $(word 1,$(subst -, ,$@))
$(BUILDS): GOARCH = $(word 2,$(subst -, ,$@))
$(BUILDS):
	$(BUILD_RULE)

.PHONY: release 
release: $(BUILDS)

include ../scripts/build/docker.mk
include ../scripts/build/test.mk
include ../scripts/build/lint.mk
include ../scripts/build/vendor.mk
include ../scripts/build/help.mk
