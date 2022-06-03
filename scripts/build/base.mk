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

buf_version ?= v1.5.0

## Tools
BUF := $(tool_bin)/buf
$(BUF):
	@echo "Install proto plugins..."
	@rm -f $(tool_bin)/protoc-gen-go
	@rm -f $(tool_bin)/protoc-gen-go-grpc
	@rm -f $(tool_bin)/protoc-gen-doc
	@rm -f $(tool_bin)/buf
	@rm -f $(tool_bin)/protoc-gen-buf-breaking
	@rm -f $(tool_bin)/protoc-gen-buf-lint
	@GOBIN=$(tool_bin) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
	@GOBIN=$(tool_bin) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
	@GOBIN=$(tool_bin) go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@v1.5.1
	@GOBIN=$(tool_bin) go install github.com/bufbuild/buf/cmd/buf@$(buf_version)
	@GOBIN=$(tool_bin) go install github.com/bufbuild/buf/cmd/protoc-gen-buf-breaking@$(buf_version)
	@GOBIN=$(tool_bin) go install github.com/bufbuild/buf/cmd/protoc-gen-buf-lint@$(buf_version)

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
	@GOBIN=$(tool_bin) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

.PHONY: clean
clean:
	git clean -Xdf
