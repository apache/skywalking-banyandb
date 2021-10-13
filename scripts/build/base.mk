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
root_dir := $(mk_dir)../..
tool_bin := $(root_dir)/bin
tool_include := "$(root_dir)/include"
uname_os := $(shell uname -s)
uname_arch := $(shell uname -m)

protoc_version ?= 3.18.1

# There are no protobuf releases for Darwin ARM so for
# now we always use the x86_64 release through Rosetta.
ifeq ($(uname_os),Darwin)
protoc_os := osx
protoc_arch := x86_64
endif
ifeq ($(uname_os),Linux)
protoc_os = linux
protoc_arch := $(uname_arch)
endif


## Tools

PROTOC := $(tool_bin)/protoc
$(PROTOC):
	@echo "Install protoc..."
	@if ! command -v curl >/dev/null 2>/dev/null; then echo "error: curl must be installed"  >&2; exit 1; fi
	@if ! command -v unzip >/dev/null 2>/dev/null; then echo "error: unzip must be installed"  >&2; exit 1; fi
	@rm -f $(tool_bin)/protoc
	@rm -rf $(tool_include)/google
	@mkdir -p $(tool_bin) $(tool_include)
	$(eval protoc_tmp := $(shell mktemp -d))
	cd $(protoc_tmp); curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v$(protoc_version)/protoc-$(protoc_version)-$(protoc_os)-$(protoc_arch).zip -o protoc.zip
	cd $(protoc_tmp); unzip protoc.zip && mv bin/protoc $(tool_bin)/protoc && mv include/google $(tool_include)/google
	@rm -rf $(protoc_tmp)
	@echo "Install proto plugins..."
	@rm -f $(tool_bin)/protoc-gen-go
	@rm -f $(tool_bin)/protoc-gen-go-grpc
	@rm -f $(tool_bin)/buf
	@rm -f $(tool_bin)/protoc-gen-buf-breaking
	@rm -f $(tool_bin)/protoc-gen-buf-lint
	@GOBIN=$(tool_bin) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1
	@GOBIN=$(tool_bin) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1
	@GOBIN=$(tool_bin) go install github.com/bufbuild/buf/cmd/buf@v0.44.0
	@GOBIN=$(tool_bin) go install github.com/bufbuild/buf/cmd/protoc-gen-buf-breaking@v0.44.0
	@GOBIN=$(tool_bin) go install github.com/bufbuild/buf/cmd/protoc-gen-buf-lint@v0.44.0

MOCKGEN := $(tool_bin)/mockgen
$(MOCKGEN):
	@echo "Install mock generate tool..."
	@mkdir -p $(tool_bin)
	@GOBIN=$(tool_bin) go install github.com/golang/mock/mockgen@v1.6.0

PROTOC_GEN_GO := $(tool_bin)/protoc-gen-go
$(PROTOC_GEN_GO):
	@echo "Install protoc gen go..."
	@mkdir -p $(tool_bin)
	@GOBIN=$(tool_bin) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1

PROTOC_GEN_GO_GRPC := $(tool_bin)/protoc-gen-go-grpc
$(PROTOC_GEN_GO_GRPC):
	@echo "Install protoc gen go grpc..."
	@mkdir -p $(tool_bin)
	@GOBIN=$(tool_bin) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1

.PHONY: clean
clean:
	git clean -Xdf
