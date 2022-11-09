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

# Retrieve git versioning details so we can add to our binary assets
VERSION_PATH    := github.com/apache/skywalking-banyandb/pkg/version
ifdef RELEASE_VERSION
VERSION_STRING := $(RELEASE_VERSION)
GIT_BRANCH_NAME := release
else
VERSION_STRING  := $(shell git describe --tags --long)
GIT_BRANCH_NAME := $(shell git rev-parse --abbrev-ref HEAD)
endif

GO_LINK_VERSION := -X ${VERSION_PATH}.build=${VERSION_STRING}-${GIT_BRANCH_NAME}

.PHONY: clean
clean:
	git clean -Xdf
