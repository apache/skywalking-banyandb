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

# Define the bpftool location - use 'which bpftool' if available on system
BPFTL := $(shell which bpftool 2>/dev/null)
IS_LINUX := $(shell uname -s | grep -i linux)

$(BPFTL):
ifeq ($(IS_LINUX),Linux)
	@echo "Installing bpftool (Linux detected)..."
	@sudo apt-get update && sudo apt-get install -y bpftool clang llvm libelf-dev
	@echo "Verifying bpftool installation..."
	@which bpftool || (echo "Failed to install bpftool" && exit 1)
	$(eval BPFTL := $(shell which bpftool))
else
	@echo "Non-Linux OS detected, switching to Docker for eBPF generation..."
	@echo "Please ensure Docker is installed and running"
endif
