# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM alpine:edge AS certs
RUN apk add --no-cache ca-certificates && update-ca-certificates

FROM busybox:stable-glibc AS build-linux

ARG TARGETARCH
ARG BINARYTYPE

COPY build/bin/linux/${TARGETARCH}/banyand-server-${BINARYTYPE} /banyand
COPY build/bin/linux/${TARGETARCH}/banyand-backup-${BINARYTYPE} /backup
COPY build/bin/linux/${TARGETARCH}/banyand-restore-${BINARYTYPE} /restore
COPY build/bin/linux/${TARGETARCH}/banyand-lifecycle-${BINARYTYPE} /lifecycle
COPY build/bin/linux/${TARGETARCH}/banyand-migration-${BINARYTYPE} /migration
COPY --from=certs /etc/ssl/certs /etc/ssl/certs

FROM build-${TARGETOS} AS final

ENV GRPC_GO_LOG_SEVERITY_LEVEL=ERROR
ENV GRPC_GO_LOG_FORMATTER=json

EXPOSE 17912
EXPOSE 17913
EXPOSE 6060
EXPOSE 2121

ENTRYPOINT ["/banyand"]

# ---------------------------------------------------------------------------
# Plugin packaging: TWO opt-in images (see docs/operation/plugins.md, plan DD2).
#
#   1. final-plugins  -> apache/skywalking-banyandb:<TAG>-plugins
#        The plugin-capable CGO/dynamic HOST image. Ships an EMPTY /plugins
#        trusted-dir mount point and NO .so files. Built via
#        `BINARYTYPE=plugins make -C banyand docker`.
#   2. final-carrier  -> apache/skywalking-banyandb:<TAG>-plugins-carrier
#        The CARRIER image (same repo, "-plugins-carrier" tag suffix): just the
#        built .so on busybox:stable-glibc.
#        MOUNTED into the host at /plugins at deploy time (image-volume or
#        initContainer). Built via `make -C banyand docker.plugins-carrier`.
#
# Everything below this point is UNREACHABLE from the default `final` target
# above (an unreferenced stage in a multi-stage Dockerfile is never built), so
# it cannot regress the default CGO_ENABLED=0 static/slim image or its build.
# The host image is built with `--target final-plugins`, the carrier with
# `--target final-carrier` (both wired in scripts/build/docker.mk).
#
# Go plugins require plugin.Open's host to be CGO-enabled and dynamically
# linked, sharing the exact toolchain/module graph/libc as every .so it loads
# (see plugins/README.md). `make release` therefore never produces either
# artifact: the host binary AND every plugins/*/*/main.go are compiled here,
# from ONE shared builder base (build-plugins-base) at ONE commit, so the
# host<->.so parity plugin.Open demands holds by lockstep — the two images
# MUST be built and tagged together (same CI run, same <TAG>), never versioned
# independently.
#
# The full repo module (go.mod lives one directory above this Dockerfile's
# own build context, banyand/) is supplied via the named build context
# "reporoot" (wired by docker.mk for the plugin targets) rather than by
# changing this Dockerfile's primary context — so the default path's context
# and COPY sources above are untouched.
#
# NOTE: the `-plugins` stages use buildx named build contexts
# (`--build-context reporoot=..` + `COPY --from=reporoot`), which require
# BuildKit (Docker 20.10+/buildx 0.8+). No `# syntax=` frontend directive is
# needed — named contexts are handled by the built-in Dockerfile frontend —
# and one is intentionally omitted so the license header can remain the first
# lines of the file (a `# syntax` line must be line 1 to be honored, which
# would displace the header). The default (busybox static/slim) stages above
# use only classic Dockerfile features and build under any engine.
# ---------------------------------------------------------------------------

FROM golang:1.25-bookworm AS build-plugins-base

ENV GOTOOLCHAIN=auto
ENV CGO_ENABLED=1
WORKDIR /src

COPY --from=reporoot go.mod go.sum ./
RUN go mod download
COPY --from=reporoot . .

# build-plugins-server compiles the CGO-dynamic banyand-server. Flags mirror
# `make build-trace-pipeline-server` (root Makefile) exactly: -trimpath, no
# -s/-w stripping, so the host's build-id/module-graph metadata that
# plugin.Open's package-hash check relies on matches the plugin builder below.
#
# `mkdir -p ui/dist && touch ui/dist/index.html` is the same placeholder
# `build-trace-pipeline-server` uses for local dev builds: ui/embed.go's
# `//go:embed dist` needs at least one non-hidden file to embed. It is NOT
# destructive — `touch` only updates mtime and never truncates an existing
# file — so when the CI job that builds this stage has already run
# `make generate` (which builds the real ui/dist via the ui/ subproject)
# before invoking this Dockerfile, the real UI is what gets embedded; this
# line only kicks in as a fallback when ui/dist is missing entirely.
#
# GO_LINK_VERSION carries the same `-X <pkg>.build=... -X <pkg>.revision=...`
# version-stamp ldflags `make release` uses (scripts/build/base.mk), passed
# through docker.mk so `banyand version` reports the correct build in the
# published -plugins artifact. `-X` only injects string constants; it does
# NOT change the compiled module graph, so host/plugin plugin.Open parity is
# unaffected.
#
# `-tags slim` matches the `-slim` release binary: it selects
# banyand/liaison/http/rpath_empty.go over rpath_ui.go (`//go:build !slim`),
# dropping the `//go:embed ui/dist` web console. A plugin-hosting data node has
# no need for the UI, so omitting it cuts image size and CVE surface. The
# `mkdir -p ui/dist && touch ui/dist/index.html` placeholder is still needed:
# ui/embed.go's `//go:embed dist` requires at least one non-hidden file to
# embed even under slim (the slim build path compiles ui/embed.go but the
# empty embed is never served). CGO_ENABLED=1 (from build-plugins-base) and all
# other flags are unchanged, so host<->.so plugin.Open parity holds.
FROM build-plugins-base AS build-plugins-server
ARG GO_LINK_VERSION
RUN mkdir -p ui/dist && touch ui/dist/index.html \
    && go build -trimpath -ldflags "${GO_LINK_VERSION}" -tags slim -o /out/banyand-server-plugins ./banyand/cmd/server

# build-plugins-so compiles every first-party plugin under plugins/<vendor>/<name>
# with the IDENTICAL toolchain/flags as build-plugins-server above (same base
# image, same builder stage lineage) — parity by construction, per
# plugins/README.md. Adding a new plugins/*/*/main.go needs no Dockerfile
# change: this loop discovers it.
#
# `set -e` is load-bearing: a shell for-loop's exit status is only its LAST
# iteration's, so without it a non-last plugin that fails to compile would be
# silently skipped and ship an image missing that .so. With `set -e` any
# failed `go build` aborts the whole RUN (and the image build).
FROM build-plugins-base AS build-plugins-so
RUN set -e; \
    mkdir -p /out/plugins; \
    for dir in plugins/*/*/; do \
      [ -f "$dir/main.go" ] || continue; \
      name=$(basename "$dir"); \
      echo "building $name.so from $dir"; \
      go build -buildmode=plugin -trimpath -o /out/plugins/"$name".so ./"$dir"; \
    done

# final-plugins is the plugin-capable HOST image ("-plugins" variant):
# gcr.io/distroless/base-debian12 (NOT busybox) because the CGO-dynamic
# banyand-server links against glibc; distroless base-debian12 is a Debian
# "bookworm" image (glibc 2.36), matching the glibc the golang:1.25-bookworm
# builder links against — so host<->.so parity holds (see plugins/README.md's
# ABI/toolchain-lock section). Distroless is chosen over debian:bookworm-slim
# to shrink the CVE surface: it carries no shell, no apt, and no package
# manager (just glibc + ca-certificates + the few runtime libs), so the
# standing glibc-CVE-tracked footprint is minimal. (If banyand fails to boot
# for a missing shared lib, switch this FROM to gcr.io/distroless/cc-debian12,
# which adds libstdc++/libgcc — also bookworm.)
#
# It ships an EMPTY /plugins directory (the trusted-dir mount POINT) and NO .so
# files: plugins are delivered at deploy time by MOUNTING the carrier image
# (final-carrier below) at /plugins. Because /plugins is empty, a mount there
# shadows nothing. banyand-backup/restore/lifecycle/migration do not host
# plugins, so they are reused unmodified from the ordinary `make release`
# static build output (this Dockerfile's own context, like the default
# `build-linux` stage above) — the plugin CI job runs `make release` first for
# exactly this.
FROM gcr.io/distroless/base-debian12 AS final-plugins

ARG TARGETARCH

COPY --from=build-plugins-server /out/banyand-server-plugins /banyand
# Empty trusted-dir mount point. NO .so is baked in — the carrier image is
# mounted here at deploy time (docs/operation/plugins.md). Created via WORKDIR
# (no shell in distroless to run mkdir): WORKDIR /plugins makes the dir, then
# WORKDIR / restores the working dir.
WORKDIR /plugins
WORKDIR /
COPY build/bin/linux/${TARGETARCH}/banyand-backup-static /backup
COPY build/bin/linux/${TARGETARCH}/banyand-restore-static /restore
COPY build/bin/linux/${TARGETARCH}/banyand-lifecycle-static /lifecycle
COPY build/bin/linux/${TARGETARCH}/banyand-migration-static /migration

ENV GRPC_GO_LOG_SEVERITY_LEVEL=ERROR
ENV GRPC_GO_LOG_FORMATTER=json

EXPOSE 17912
EXPOSE 17913
EXPOSE 6060
EXPOSE 2121

ENTRYPOINT ["/banyand"]

# final-carrier is the CARRIER image (same repo, "-plugins-carrier" tag suffix:
# apache/skywalking-banyandb:<TAG>-plugins-carrier): ONLY the built .so files,
# on busybox:stable-glibc. busybox is chosen because (a) it is glibc-consistent
# with the bookworm-built .so, and (b) it has a shell + `cp`, so the same image
# doubles as the initContainer that copies plugins into a shared emptyDir (the
# portable, pre-1.31 delivery path — see docs/operation/plugins.md). It is
# built from the SAME build-plugins-so stage / same commit / same CI run as the
# host binary above, so the .so it ships is lockstep-parity with the host that
# will load it. The plugins land at /plugins so an image-volume mount of this
# carrier at the host's /plugins lines up 1:1.
FROM busybox:stable-glibc AS final-carrier
COPY --from=build-plugins-so /out/plugins /plugins
