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
# Canonical, pinned build environment for the artifacts committed to this
# repository. See docs/design/0.12.0/docker-canonical-build/README.md.
#
# The native targets (make license-dep, make check, ...) keep working unchanged.
# This is the path that produces identical bytes on Linux, macOS and Windows,
# and it is the one CI treats as canonical.
#
#   make docker-license-dep                 # the canonical license generation
#   make docker-run TARGET=<target>         # another target, in the same environment
#   make bump-build-image                    # refresh the Debian digest and snapshot
#
# `make docker-run TARGET=check` is deliberately NOT an example: the tree is
# streamed in without .git, so `check` and `check-format` cannot run there. See
# the contract on the docker-run target below.
#
# Nothing here writes to the host's bin/ or node_modules: the tree is streamed
# into the container over stdin and only the license artifacts are copied back.

mk_path  := $(abspath $(lastword $(MAKEFILE_LIST)))
# This file lives in scripts/build/, so the repository root is two levels up.
# Derived from mk_path rather than from the including Makefile's mk_dir so that
# the values are correct whether this is included from the root Makefile or
# invoked directly.
root_dir := $(abspath $(dir $(mk_path))/../..)
# Trailing-slash form; $(abspath) strips it and every path below is a concatenation.
root_p   := $(root_dir)/

VERSION_MK := $(root_p)scripts/build/version.mk
include $(VERSION_MK)

# --- Versions and pins -------------------------------------------------------
# Every one of these comes from scripts/build/version.mk, which is the only place
# they are written down. There are no version literals below this line and none in
# the Dockerfile, so a bump is one edit in one file.
#
# Neither Go nor Node is read from version.mk, on purpose. `go` in go.mod and
# engines.node in canopy/package.json are the declarations the toolchains
# themselves enforce; declaring either twice would create two sources that can
# silently disagree. The entrypoint turns a drift between the image and either
# declaration into a loud failure.
GO_VERSION    := $(shell sed -n 's/^go //p' $(root_p)go.mod)

# Recursive, so the resolver runs only when something expands it -- that is, only
# for the Docker targets. As an immediate assignment it ran on every parse,
# including `make build` and `make help`, and it needed bash there. A POSIX shell
# is still a requirement for the recipes themselves, which is documented rather
# than assumed: on Windows that means Git Bash or WSL2.
NODE_VERSION   = $(shell bash $(root_p)scripts/ci/check/node_version.sh resolve $(root_p))
LICENSE_EYE_VERSION := $(strip $(LICENSE_EYE_VERSION))

# --- Image coordinates -------------------------------------------------------
# The canonical platform, a constant rather than an override, and not overridable
# in practice: the pinned toolchain tarballs are amd64-specific and BuildKit's
# cache key omits the target platform, so a --platform override reuses the amd64
# layers and yields a mislabelled image. The Dockerfile asserts its own
# architecture, so a mismatch fails the build. See version.mk.
# `override`, not `:=`: a plain assignment LOSES to `make PLATFORM=linux/arm64` on
# the command line, which is how the "not overridable" claim was falsified. The
# override directive is the documented way to win against a command-line
# assignment. The Dockerfile asserts its own architecture as well, so a mistake
# here still fails the build rather than producing a mislabelled image.
override PLATFORM := $(BUILD_PLATFORM)
override DISTRO   := $(DEBIAN_DISTRO)
# Go and Node name the same architecture differently (GOARCH=amd64 vs
# process.arch=x64); both are derived from BUILD_PLATFORM, neither is written down.
go_arch   = $(patsubst linux/%,%,$(1))
node_arch = $(if $(filter amd64,$(call go_arch,$(1))),x64,$(call go_arch,$(1)))
BUILD_IMAGE      ?= skywalking-banyandb-build
BUILD_IMAGE_TAG  ?= go$(GO_VERSION)-node$(NODE_VERSION)-$(LICENSE_EYE_VERSION)
BUILDKIT_CACHE   ?= $(root_p)bin/.buildkit

# The local cache is a nice-to-have, not a requirement, and the `docker` driver
# cannot export one:
#
#   ERROR: failed to build: Cache export is not supported for the docker driver.
#   Switch to a different driver, or turn on the containerd image store.
#
# That is the DEFAULT driver on Docker Desktop, so passing --cache-to
# unconditionally breaks `make docker-license-dep` for macOS and Windows
# contributors out of the box. Ask the builder instead of assuming.
# Recursive (`=`), not immediate (`:=`), and no --bootstrap. As an immediate
# assignment this ran on EVERY parse, so `make build` and `make help` contacted
# Docker, and --bootstrap can start a builder as a side effect. Deferred, it is
# expanded only by the Docker recipes, which is where it is actually needed.
#
# Without --bootstrap, `inspect` reports the current builder's driver even when
# that builder is not running yet, which is all this is used for. If buildx is
# absent the driver reads empty and we enable the cache flags optimistically; a
# driver that cannot export one is handled by buildImageCacheFlags at build time.
BUILDX_DRIVER = $(shell docker buildx inspect 2>/dev/null \
                 | sed -n 's/^Driver: *//p' | head -1)
BUILDKIT_CACHE_FLAGS = $(if $(filter docker,$(BUILDX_DRIVER)),,\
                        --cache-from type=local,src=$(BUILDKIT_CACHE) \
                        --cache-to type=local,dest=$(BUILDKIT_CACHE),mode=max)
STAGE_DIR        ?= $(root_p)bin/.license-stage

# Run the container as the invoking user where that is meaningful. On a POSIX
# host two reasons, both load-bearing: the artifacts written into the mounted
# STAGE_DIR must be owned by that user or the copy-back needs sudo (and fails
# outright on a WSL DrvFs mount), and `npm ci` runs inside the container and must
# not leave root-owned files in a tree the user will go on to work in.
#
# On Windows the container is not the problem -- Docker Desktop runs Linux
# containers natively -- but `id` does not exist in PowerShell or cmd, and it is
# not needed there: NTFS/DrvFs has no uid ownership, so the copied-back files
# belong to the Windows user regardless of which uid wrote them. So --user is
# applied only when a POSIX `id` is actually found, and the default is to omit it
# rather than to refuse to run.
HOST_UID ?= $(shell id -u 2>/dev/null || echo unavailable)
HOST_GID ?= $(shell id -g 2>/dev/null || echo unavailable)
HAVE_POSIX_ID := $(if $(filter unavailable,$(HOST_UID)),,yes)
ifeq ($(HAVE_POSIX_ID),yes)
DOCKER_USER_FLAG = --user $(HOST_UID):$(HOST_GID)
else
DOCKER_USER_FLAG =
endif

# --- Resource defaults -------------------------------------------------------
# 2 CPUs / 4 GB, matching the existing `make test-docker` target rather than
# inventing a second convention. The point is not comfort: a container that sees
# the whole machine's core count sizes the Go build and V8's heap from it, and
# the resulting peak becomes both unpleasant and host-dependent.
RUN_CPUS        ?= 2
RUN_MEMORY      ?= 4g
# Equal values disable swap, so the limit is a hard ceiling rather than a
# threshold the kernel is free to exceed by swapping.
RUN_MEMORY_SWAP ?= $(RUN_MEMORY)
# npm's default 64 MB /dev/shm is a known source of silent failures.
RUN_SHM_SIZE    ?= 256m

# The GOMAXPROCS and heap cap are the two that matter for determinism rather
# than just politeness: without them Go reads the host CPU count and fights the
# cgroup quota, and V8 grows until the OOM killer wins.
#
# --cpuset-cpus is not redundant with --cpus. --cpus caps CPU *time* via the CFS
# quota (cpu.max), which does not change the affinity mask, so runtime.NumCPU()
# and `nproc` still report every core on the host — Go would then size its build
# for 32 procs inside a 2-CPU quota. The cpuset is what makes the visible CPU
# count match the limit. Verified in the image: NumCPU reports 2, cpu.max
# reports 200000/100000.
CPUSET_CPUS ?= 0-$(shell expr $(RUN_CPUS) - 1)

DOCKER_RUN_ENV = $(DOCKER_USER_FLAG) \
                 --cpuset-cpus $(CPUSET_CPUS) \
                 --env GOMAXPROCS=$(RUN_CPUS) \
                 --env NODE_OPTIONS=--max-old-space-size=2048 \
                 --env SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH) \
                 --env TZ=UTC \
                 --env LC_ALL=C \
                 --env LICENSE_EYE=/usr/local/bin/license-eye

# Excluded from the copy in: none of these are inputs to license generation.
# `dist` is NOT excluded wholesale — canopy and mcp read dist/LICENSE.tpl as an
# input template — only the two generated paths inside it are. `--exclude`
# globs the whole path, so `dist/LICENSE` does not match `dist/LICENSE.tpl`.
# The generated output sets. These are what `make license-dep` writes and what
# the copy-back REPLACES. They must stay in sync with LICENSE_PATHS in
# scripts/ci/check/license_manifest.sh and ARTIFACTS in the entrypoint;
# `check-coverage` fails if a project gains a `license-dep` target and none of
# these covers it.
#
#   dist/LICENSE dist/licenses mcp/LICENSE mcp/licenses
#   canopy/LICENSE canopy/licenses
#
# The embedded UI was removed upstream in 0e3ee0ec, so there is no ui entry here
# or in LICENSE_PATHS.
GENERATED_OUTPUTS = \
	dist/LICENSE dist/licenses \
	mcp/LICENSE mcp/licenses \
	canopy/LICENSE canopy/licenses

# Not excluded wholesale: canopy and mcp read dist/LICENSE.tpl as an INPUT
# template. --exclude globs the whole path, so dist/LICENSE does not match
# dist/LICENSE.tpl.
COPY_EXCLUDES = --exclude=.git --exclude=bin --exclude=node_modules $(patsubst %,--exclude=%,$(GENERATED_OUTPUTS))

.PHONY: docker-image docker-run docker-license-dep docker-license-check bump-build-image print-build-args

# Every --build-arg is explicit. A bare `--build-arg X` reads the shell
# environment, and a plain Make variable is not exported to it by default.
print-build-args:
	@echo "DEBIAN_IMAGE=$(DEBIAN_IMAGE)"
	@echo "DEBIAN_SNAPSHOT=$(DEBIAN_SNAPSHOT)"
	@echo "GO_VERSION=$(GO_VERSION)"
	@echo "NODE_VERSION=$(NODE_VERSION)"
	@echo "LICENSE_EYE_VERSION=$(LICENSE_EYE_VERSION)"
	@echo "DEBIAN_DISTRO=$(DEBIAN_DISTRO)"
	@echo "SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)"

docker-image: ## Build the pinned build environment (digest- and checksum-verified)
	@echo "buildx driver: $(if $(BUILDX_DRIVER),$(BUILDX_DRIVER),unknown)$(if $(BUILDKIT_CACHE_FLAGS), (local cache enabled), (no local cache: this driver cannot export one))"
	@test -n "$(DEBIAN_IMAGE)" || { echo "scripts/build/version.mk is missing DEBIAN_IMAGE; run 'make bump-build-image'" >&2; exit 1; }
	@test -n "$(DEBIAN_SNAPSHOT)" || { echo "scripts/build/version.mk is missing DEBIAN_SNAPSHOT; run 'make bump-build-image'" >&2; exit 1; }
	@test -n "$(DEBIAN_DISTRO)" || { echo "DEBIAN_DISTRO is empty; check scripts/build/version.mk and that it is passed as a build arg" >&2; exit 1; }
	@echo "Building $(BUILD_IMAGE):$(BUILD_IMAGE_TAG) for $(PLATFORM)"
	docker buildx build \
	  --platform $(PLATFORM) \
	  --target tools \
	  --load \
	  --file $(root_p)scripts/build/dockerfiles/build.Dockerfile \
	  --build-arg DEBIAN_IMAGE=$(DEBIAN_IMAGE) \
	  --build-arg DEBIAN_SNAPSHOT=$(DEBIAN_SNAPSHOT) \
	  --build-arg DEBIAN_DISTRO=$(DEBIAN_DISTRO) \
	  --build-arg GO_VERSION=$(GO_VERSION) \
	  --build-arg NODE_VERSION=$(NODE_VERSION) \
	  --build-arg LICENSE_EYE_VERSION=$(LICENSE_EYE_VERSION) \
	  --build-arg SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH) \
	  --build-arg EXPECTED_ARCH=$(call go_arch,$(PLATFORM)) \
	  --build-arg EXPECTED_NODE_ARCH=$(call node_arch,$(PLATFORM)) \
	  $(BUILDKIT_CACHE_FLAGS) \
	  --tag $(BUILD_IMAGE):$(BUILD_IMAGE_TAG) \
	  $(root_p)

# TARGET must be a target-specific variable. Written as
# `docker-license-dep: docker-run ; TARGET=license-dep` it would be a shell
# recipe executed *after* docker-run, and the container would never see it.
docker-license-dep:   TARGET=license-dep
docker-license-check: TARGET=license-check

# The host uid/gid are required, not merely convenient: without them the
# container writes root-owned files into the mounted staging directory, and the
# What the host genuinely has to provide: docker, and a tar that understands
# --exclude (GNU tar, or the bsdtar that ships with Windows 10+). GNU make is
# already a documented requirement for every platform.
#
# A native Windows host is supported. Docker Desktop runs this Linux image
# natively there, so the container is not the obstacle; the only thing missing is
# a POSIX `id`, and DOCKER_USER_FLAG already handles that by omission rather than
# by refusing to run. WSL2 is the smoother option because it also provides GNU
# make and GNU tar, but it is not required for this target.
define require_docker_host
	@command -v docker >/dev/null 2>&1 || { \
	  echo "make docker-* needs docker on PATH." >&2; exit 1; }
	@# Probe by USE, not by reading --help. BSD tar (macOS) implements --exclude
	@# but its --help is a short summary that does not mention it, so grepping the
	@# help text would reject a perfectly good tar on macOS. Doing it is the only
	@# portable test.
	@probe=$$(mktemp -d) && touch $$probe/keep.txt && \
	  tar -cf /dev/null --exclude=never-matches $$probe 2>/dev/null && rm -rf $$probe || { \
	    rm -rf $$probe; \
	    echo "make docker-* needs a tar with --exclude support (GNU tar or bsdtar)." >&2; \
	    exit 1; }
endef

# The post-generation verification is a bash script. Where bash is available it
# runs here and now, so a bad artifact is reported at the moment it is produced.
# Where it is not (a minimal Windows host with neither WSL2 nor Git Bash), the
# generation still succeeds and the notice points at CI, which runs the same
# check unconditionally. Failing the generation here would be worse than
# reporting it there: the artifacts are the deliverable, the check is a guard.
define verify_after_generation
	@if command -v bash >/dev/null 2>&1; then \
	  $(MAKE) -C $(root_p) check-license-outputs; \
	else \
	  echo "notice: bash not found, so check-license-outputs was skipped locally." >&2; \
	  echo "        CI runs it unconditionally; WSL2 or Git Bash can run it now." >&2; \
	fi
endef

docker-license-dep docker-license-check: docker-image
	$(require_docker_host)
	@echo "Running 'make $(TARGET)' in $(BUILD_IMAGE):$(BUILD_IMAGE_TAG) \
          (cpus=$(RUN_CPUS) memory=$(RUN_MEMORY) shm=$(RUN_SHM_SIZE) platform=$(PLATFORM))"
	@rm -rf $(STAGE_DIR) && mkdir -p $(STAGE_DIR)
	@tar -C $(root_p) $(COPY_EXCLUDES) -cf - . \
	  | docker run -i \
	      --rm \
	      --platform $(PLATFORM) \
	      --cpus=$(RUN_CPUS) \
	      --memory=$(RUN_MEMORY) \
	      --memory-swap=$(RUN_MEMORY_SWAP) \
	      --shm-size=$(RUN_SHM_SIZE) \
	      $(DOCKER_RUN_ENV) \
	      --volume $(STAGE_DIR):/out \
	      $(BUILD_IMAGE):$(BUILD_IMAGE_TAG) \
	      $(TARGET)
	@# REPLACE the generated output sets rather than extracting over them. An
	@# overlay silently keeps any file the container did not produce, which is
	@# exactly what happens when license-eye's npm install fails without failing
	@# the run -- the stale license survives, matches its own committed blob, and
	@# check-committed passes. Removing first is safe because the container run
	@# has already completed successfully by this point.
	@for path in $(GENERATED_OUTPUTS); do rm -rf "$(root_p)$$path"; done
	@tar -C $(STAGE_DIR) -cf - . | tar -C $(root_p) -xf -
	$(verify_after_generation)

# Escape hatch for anything else that should run in the pinned environment.
#
# CONTRACT: the target must not need Git metadata. The tree is streamed in
# without .git, so `check`, `check-format` and anything else shelling out to git
# cannot run here -- they would fail on a missing repository, not on a real
# problem. Use them natively; the environment this provides is the toolchain, not
# a working copy.
docker-run: docker-image
	$(require_docker_host)
	@rm -rf $(STAGE_DIR) && mkdir -p $(STAGE_DIR)
	@tar -C $(root_p) $(COPY_EXCLUDES) -cf - . \
	  | docker run -i \
	      --rm \
	      --platform $(PLATFORM) \
	      --cpus=$(RUN_CPUS) \
	      --memory=$(RUN_MEMORY) \
	      --memory-swap=$(RUN_MEMORY_SWAP) \
	      --shm-size=$(RUN_SHM_SIZE) \
	      $(DOCKER_RUN_ENV) \
	      --volume $(STAGE_DIR):/out \
	      $(BUILD_IMAGE):$(BUILD_IMAGE_TAG) \
	      $(TARGET)

# Re-resolve everything that cannot be derived. The file is written through a
# temp file and renamed, so a failed lookup (a moved tag, a network blip) leaves
# the previous pins intact rather than a half-updated file.
bump-build-image: ## Refresh the Debian digest and snapshot in scripts/build/version.mk
	@# Both values are resolved and validated BEFORE version.mk is rewritten. The
	@# previous version embedded `date -d` inside the sed replacement: GNU date
	@# works, BSD date does not, and sed would still have succeeded -- replacing a
	@# good pin with an empty value while the target reported success. A partially
	@# written pin file is worse than an outdated one.
	@set -e; \
	base=$$(docker buildx imagetools inspect debian:$(DISTRO)-slim --format '{{.Manifest.Digest}}' 2>/dev/null || true); \
	# A FORMAT check, not a non-empty check. imagetools can return an empty
	# string on a transient failure, and "non-empty" would then have written
	# `debian:bookworm-slim@` -- a pin that looks valid and pins nothing.
	printf '%s' "$$base" | grep -Eq '^sha256:[0-9a-f]{64}$$' || { \
	  echo "could not resolve a sha256 digest for debian:$(DISTRO)-slim (got '$$base')" >&2; exit 1; }; \
	# Portable "one week ago": `date -d` is GNU-only, so fall back to python or to
	# leaving the snapshot untouched rather than blanking it.
	snapshot="$$(python3 -c 'import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=7)).strftime("%Y%m%dT%H%M%SZ"))' 2>/dev/null || true)"; \
	if [ -z "$$snapshot" ]; then \
	  echo "could not compute a snapshot date; leaving DEBIAN_SNAPSHOT unchanged"; \
	else \
	  echo "$$snapshot" | grep -Eq '^[0-9]{8}T[0-9]{6}Z$$' || { echo "implausible snapshot date '$$snapshot'" >&2; exit 1; }; \
	fi; \
	tmp=$$(mktemp); \
	sed -e "s|^DEBIAN_IMAGE .*|DEBIAN_IMAGE := debian:$(DISTRO)-slim@$$base|" \
	    $${snapshot:+-e "s|^DEBIAN_SNAPSHOT .*|DEBIAN_SNAPSHOT := $$snapshot|"} \
	    $(VERSION_MK) > $$tmp; \
	grep -qF "DEBIAN_IMAGE := debian:$(DISTRO)-slim@$$base" $$tmp || { echo "the digest rewrite did not take" >&2; rm -f $$tmp; exit 1; }; \
	if [ -n "$$snapshot" ]; then grep -qF "DEBIAN_SNAPSHOT := $$snapshot" $$tmp || { echo "the snapshot rewrite did not take" >&2; rm -f $$tmp; exit 1; }; fi; \
	mv $$tmp $(VERSION_MK); \
	echo "Updated $(VERSION_MK):"; \
	echo "  DEBIAN_IMAGE    = debian:$(DISTRO)-slim@$$base"; \
	echo "  DEBIAN_SNAPSHOT = $${snapshot:-unchanged}"; \
	echo "Go, Node and license-eye need no refresh here: Go is read from go.mod,"

