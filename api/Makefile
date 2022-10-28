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

NAME := api

include ../scripts/build/base.mk
include ../scripts/build/lint-api.mk
include ../scripts/build/help.mk

proto_dir := $(root_dir)/api/proto

## Tools
BUF := $(tool_bin)/buf
$(BUF):
	@echo "Install proto plugins..."
	@rm -f $(tool_bin)/protoc-gen-go
	@rm -f $(tool_bin)/protoc-gen-go-grpc
	@rm -f $(tool_bin)/protoc-gen-doc
	@rm -f $(tool_bin)/protoc-gen-grpc-gateway
	@rm -f $(tool_bin)/protoc-gen-openapiv2
	@rm -f $(tool_bin)/protoc-gen-validate
	@rm -f $(tool_bin)/buf
	@GOBIN=$(tool_bin) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
	@GOBIN=$(tool_bin) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
	@GOBIN=$(tool_bin) go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@v1.5.1
	@GOBIN=$(tool_bin) go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@v2.10.3
	@GOBIN=$(tool_bin) go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.10.3
	@GOBIN=$(tool_bin) go install github.com/bufbuild/buf/cmd/buf@$(buf_version)
	@GOBIN=$(tool_bin) go install github.com/envoyproxy/protoc-gen-validate@v0.6.7

generate: $(BUF)
	@PATH=$(tool_bin):$(proto_dir) $(BUF) generate && \
	  cp proto/api-reference.md $(root_dir)/docs
