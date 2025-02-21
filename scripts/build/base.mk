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

# The main module path of a repository. For this repo, it is github.com/tetrateio/tetrate.
# MODULE_PATH is a variable used by some of the scripts in this subdirectory to refer to the main
# module name of this repository. This allows another project to import this subdirectory (for
# example, using a tool similar to: https://github.com/buildinspace/peru) and override this
# variable.
MODULE_PATH ?= $(shell go mod edit -json | jq -r .Module.Path)

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

API_VERSION_PATH := github.com/apache/skywalking-banyandb/api/proto/banyandb
PROTO_REVISION := $(shell git log -1 --pretty=format:%h -- ${root_dir}/api/proto/banyandb)

GO_LINK_VERSION := ${GO_LINK_VERSION} -X ${API_VERSION_PATH}.revision=${PROTO_REVISION}


# Operating system name, such as 'Darwin'
OS := $(shell uname)

# TARGET_OS is the operating system to be used for building components for local dev setups.
ifeq ($(OS),Darwin)
	TARGET_OS ?= darwin
else ifeq ($(OS),Linux)
	TARGET_OS ?= linux
endif

# Architecture, such as 'arm64'
ARCH := $(shell uname -m)

# Architecture to be used for building components for local dev setups.
ifeq ($(ARCH),x86_64)
	TARGET_ARCH ?= amd64
else ifeq ($(ARCH),amd64)
	TARGET_ARCH ?= amd64
else ifeq ($(ARCH),arm64)
	TARGET_ARCH ?= arm64
endif

# A comma-separated list of platforms for which the
# docker images should be built. Builds for linux/<local-arch> for dev purposes
# and multiple archs when run as part of the CI pipeline.
PLATFORMS ?= linux/$(TARGET_ARCH)

# Force using docker buildkit for all docker builds,
# where not enabled by default.
export DOCKER_BUILDKIT := 1

# Avoid warning in Apple's new XCode 15 linker when using test -race.
ifeq (Darwin,$(shell uname -s))
LDFLAGS_COMMON ?= -extldflags=-Wl,-ld_classic
endif

.PHONY: clean
clean:
	git clean -Xdf
