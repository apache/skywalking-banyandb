# Frontend pinned by digest, not by tag: a tag here is an unpinned input like any
# other, and this Dockerfile exists to have none.
# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
#
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
# Build environment for the artifacts that are committed to this repository.
#
#   docker buildx build --target tools ... (see scripts/build/dockerize.mk)
#
# Every base image, toolchain tarball and package index is digest- or
# checksum-pinned, and the versions are passed in as build args derived from
# go.mod, the mcp and canopy package.json files, and scripts/build/version.mk.
# There is deliberately
# no default for any of them: a Dockerfile that carries a version is a second
# place to forget to update it, and the failure mode is a contributor silently
# generating different bytes than CI.
#
# Design: docs/design/0.12.0/docker-canonical-build/README.md

# --- Pinned inputs ----------------------------------------------------------
# All required; no defaults on purpose. scripts/build/dockerize.mk supplies
# every one of them.
ARG DEBIAN_IMAGE
ARG GO_VERSION
ARG NODE_VERSION
ARG DEBIAN_SNAPSHOT
ARG LICENSE_EYE_VERSION
ARG SOURCE_DATE_EPOCH

FROM ${DEBIAN_IMAGE} AS tools

# A global ARG is out of scope inside a build stage until it is re-declared here.
# Every ARG referenced below — by the ENV block or by a RUN — must appear in this
# list, or it silently expands to the empty string. SOURCE_DATE_EPOCH was
# expanded to empty this way once, which is why it is called out.
ARG DEBIAN_SNAPSHOT
ARG DEBIAN_DISTRO
ARG SOURCE_DATE_EPOCH

# GOTOOLCHAIN=local is a deliberate divergence from CI, which resets
# GOTOOLCHAIN to `auto` after setup-go. Here a go.mod newer than the image
# fails loudly instead of silently downloading a different toolchain — the
# entrypoint's version cross-check turns that into a legible error.
# No WORKDIR on purpose. The entrypoint runs as the host's uid and creates its
# own scratch directory under /tmp; a WORKDIR baked here would be created as
# root at build time and would be unwritable by that uid.

ENV DEBIAN_FRONTEND=noninteractive \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH} \
    TZ=UTC \
    LC_ALL=C \
    LANG=C \
    PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin \
    GOPATH=/go \
    GOMODCACHE=/go/pkg/mod \
    GOCACHE=/root/.cache/go-build \
    GOTOOLCHAIN=local \
    GOFLAGS=-buildvcs=false

# apt is pinned to a snapshot. A base-image digest freezes the OS image but not
# the repository contents, so "whatever the mirror serves today" would otherwise
# decide which make/git/ca-certificates get installed. `check-valid-until=no`
# and Acquire::Check-Valid-Until=false are required because a snapshot is by
# definition past its validity window.
#
# The snapshot is fetched over http, not https, and that is not a downgrade:
# debian:bookworm-slim ships no CA store, so it cannot complete a TLS handshake
# with snapshot.debian.org to install the very package that would give it one.
# The transport is plaintext but the content is not trusted on that basis —
# apt verifies the Release and Packages signatures against the archive keyring
# in the digest-pinned base image, and the package payloads are checksummed
# against those signed indexes. `apt-get update` fails outright on a signature
# mismatch. Once ca-certificates is installed, the Go and Node tarballs below
# are fetched over https and verified against pinned SHA-256 sums.
RUN printf 'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/%s %s main\n' \
        "${DEBIAN_SNAPSHOT}" "${DEBIAN_DISTRO}" >/etc/apt/sources.list \
 && rm -f /etc/apt/sources.list.d/debian.sources \
 && apt-get -o Acquire::Check-Valid-Until=false update \
 && apt-get -o Acquire::Check-Valid-Until=false install -y --no-install-recommends \
      ca-certificates \
      curl \
      git \
      jq \
      make \
      tar \
      xz-utils \
 && rm -rf /var/lib/apt/lists/*

# Go and Node come from their official tarballs with checksum verification.
# Copying /usr/local/go out of a golang image, or node's binary out of a node
# image, silently drops PATH, GOPATH and the shared libraries the binary needs;
# installing the tarball is the boring, correct way.
# The tarball is verified against the checksum the vendor publishes for this exact
# file, fetched from the vendor's own index over TLS. A locally recorded hash
# would catch a compromised origin as well as corruption; this only catches
# corruption and truncation. That is a deliberate trade for one fewer thing to
# maintain on a version bump, and it is the trust model the rest of this build
# already accepts: license-eye is installed by `go install`, which resolves and
# verifies through the module proxy, and license-eye then runs `npm ci`, which
# verifies every package from the npm registry.
ARG GO_VERSION
RUN set -e; \
    go_file="go${GO_VERSION}.linux-amd64.tar.gz"; \
    curl -fsSLo /tmp/go.tgz "https://go.dev/dl/${go_file}"; \
    expected="$(curl -fsSL 'https://go.dev/dl/?mode=json&include=all' \
      | jq -r --arg f "$go_file" '.[] | select(.version == "go'"${GO_VERSION}"'") | .files[] | select(.filename == $f) | .sha256')"; \
    test -n "$expected" && test "$expected" != "null" \
      || { echo "no published checksum for $go_file at go${GO_VERSION}"; exit 1; }; \
    echo "${expected}  /tmp/go.tgz" | sha256sum -c -; \
    tar -C /usr/local -xzf /tmp/go.tgz; \
    rm /tmp/go.tgz

ARG NODE_VERSION
RUN set -e; \
    node_file="node-v${NODE_VERSION}-linux-x64.tar.xz"; \
    curl -fsSLo /tmp/node.tar.xz "https://nodejs.org/dist/v${NODE_VERSION}/${node_file}"; \
    expected="$(curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/SHASUMS256.txt" \
      | awk -v f="$node_file" '$2 == f { print $1 }')"; \
    test -n "$expected" \
      || { echo "no published checksum for $node_file at v${NODE_VERSION}"; exit 1; }; \
    echo "${expected}  /tmp/node.tar.xz" | sha256sum -c -; \
    mkdir -p /usr/local/lib/nodejs; \
    tar -C /usr/local/lib/nodejs -xJf /tmp/node.tar.xz --strip-components=1; \
    ln -s /usr/local/lib/nodejs/bin/node /usr/local/bin/node; \
    ln -s /usr/local/lib/nodejs/bin/npm  /usr/local/bin/npm; \
    ln -s /usr/local/lib/nodejs/bin/npx  /usr/local/bin/npx; \
    rm /tmp/node.tar.xz

# license-eye is baked in rather than installed per run: it removes the host Go
# version from the tool's provenance and makes every run use the same binary.
ARG LICENSE_EYE_VERSION
RUN --mount=type=cache,target=/go/pkg/mod \
    GOBIN=/usr/local/bin go install github.com/apache/skywalking-eyes/cmd/license-eye@${LICENSE_EYE_VERSION}

# Normalize the entrypoint's bytes and mode during the build. A CRLF shebang
# from a Windows checkout, or a lost executable bit, must not be able to make
# the image unusable.
COPY scripts/build/dockerfiles/entrypoint.sh /usr/local/bin/entrypoint
RUN chmod 0755 /usr/local/bin/entrypoint \
 && sed -i 's/\r$//' /usr/local/bin/entrypoint

# Fail at build time rather than at first use. A broken toolchain that only
# surfaces when a contributor runs the target is the expensive kind.
#
# The architecture assertion is load-bearing, not decoration. The toolchain
# tarballs above are pinned to the amd64 filenames and checksums, and BuildKit's
# cache key does not include the target platform — so asking for a different
# platform reuses the amd64 layers and produces an image that is mislabelled
# rather than obviously broken. Asserting here turns that into a loud failure at
# build time. The supported platform is a constant in dockerize.mk, not an
# override, for the same reason.
# Go and Node name the same architecture differently (GOARCH=amd64 vs
# process.arch=x64), so they are asserted separately.
ARG EXPECTED_ARCH=amd64
ARG EXPECTED_NODE_ARCH=x64
RUN [ "$(go env GOARCH)" = "${EXPECTED_ARCH}" ] || { \
      echo "image is $(go env GOOS)/$(go env GOARCH) but ${EXPECTED_ARCH} was required"; \
      echo "the pinned toolchain tarballs are amd64-only; see docs/design/0.12.0/docker-canonical-build/README.md"; \
      exit 1; } \
 && node -e "if (process.arch !== '${EXPECTED_NODE_ARCH}') { console.error('node arch is ' + process.arch + ', expected ${EXPECTED_NODE_ARCH}'); process.exit(1) }" \
 && go version && node --version && npm --version && make --version | head -1 && license-eye --version

ENTRYPOINT ["/usr/local/bin/entrypoint"]
