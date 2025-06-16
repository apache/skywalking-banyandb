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

BPF2GO := $(tool_bin)/bpf2go
IS_LINUX := $(shell uname -s | grep -i linux)

$(BPF2GO):
	@echo "Installing bpf2go..."
	@mkdir -p $(tool_bin)
	@GOBIN=$(tool_bin) go install github.com/cilium/ebpf/cmd/bpf2go@$(BPF2GO_VERSION)
	@if [ ! -f "$(BPF2GO)" ]; then \
		echo "WARNING: Failed to install bpf2go at $(BPF2GO)"; \
		if [ "$(IS_LINUX)" = "Linux" ]; then \
			echo "ERROR: bpf2go is required on Linux"; \
			exit 1; \
		fi; \
		echo "Creating a stub bpf2go executable for non-Linux platform"; \
		echo '#!/bin/sh' > $(BPF2GO); \
		echo 'echo "bpf2go stub: eBPF code generation only works on Linux"' >> $(BPF2GO); \
		echo 'exit 0' >> $(BPF2GO); \
		chmod +x $(BPF2GO); \
	fi