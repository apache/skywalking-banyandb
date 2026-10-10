# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE_2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

BUF_VERSION := v1.60.0
PROTOC_GEN_GO_VERSION := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
PROTOC_GEN_DOC_VERSION := v1.5.1
GRPC_GATEWAY_VERSION := v2.30.0
PROTOC_GEN_VALIDATE_VERSION := v1.3.3

GOLANGCI_LINT_VERSION := v1.64.8
REVIVE_VERSION := v1.13.0
LICENSE_EYE_VERSION := 55373684d1b70e5f8fd9fc8ec114a89ad11a56a3

MOCKGEN_VERSION := v0.6.0

GINKGO_VERSION := v2.32.1

GOVULNCHECK_VERSION := v1.1.4

# Version for bpf2go tool used for eBPF code generation. Keep in sync with
# pkg/fs/fadvismonitor/Dockerfile ARG BPF2GO_VERSION
BPF2GO_VERSION := v0.21.0

## Build environment for committed artifacts
## (scripts/build/dockerfiles/build.Dockerfile, see
##  docs/design/0.12.0/docker-canonical-build/README.md)
##
## This is the only place versions and pins for the pinned build environment are
## written down. The Dockerfile contains no version literals, the make wrapper
## contains none, and nothing is duplicated: a bump is one line here.
##
## Neither the Go nor the Node version is here, and that is deliberate. `go 1.26.9`
## in go.mod, and `"node": "24.6.0"` in BOTH mcp/package.json and canopy/package.json,
## are the declarations the toolchains themselves read and enforce. A second copy here would create two
## sources of truth that can silently disagree, and the failure mode is subtle: a
## contributor resolving a different license set from CI. Both are derived and
## passed to the Dockerfile as build args; the entrypoint fails the build if the
## image and either declaration drift apart, and `make check-node-version`
## cross-checks that mcp and canopy pin the same version, and that every other
## project's engines field is satisfied by it.
##
# The single canonical platform for the build environment. Not overridable: the
# toolchain tarballs are amd64-specific, and BuildKit's cache key does not include
# the target platform, so a --platform override reuses the amd64 layers and
# produces a mislabelled image instead of an obvious failure. The Dockerfile
# asserts its own architecture, so a mismatch fails loudly.
BUILD_PLATFORM := linux/amd64

# Debian base image, digest-pinned. A digest freezes the OS image but not the
# contents of the apt repository, so DEBIAN_SNAPSHOT below is what freezes the
# packages installed into it. These two cannot be derived from a URL the way a
# tarball checksum can, which is why they are recorded here.
DEBIAN_DISTRO := bookworm
DEBIAN_IMAGE := debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
DEBIAN_SNAPSHOT := 20250929T000000Z

# Fixed timestamp for anything the environment stamps. Archive member
# normalization is NOT implemented yet; see the design §10.1.
SOURCE_DATE_EPOCH := 1700000000
