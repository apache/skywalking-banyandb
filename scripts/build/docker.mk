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

# The hub of the docker image. The default value is skywalking-.
# For github registry, it would be ghcr.io/apache/skywalking-
HUB ?= apache

ifndef IMG_NAME
$(error The IMG_NAME variable should be set)
endif

# The tag of the docker image. The default value if latest.
TAG ?= latest

BINARYTYPE ?= static

ifeq ($(BINARYTYPE),slim)
	TAG := $(TAG)-slim
endif

# banyand/Dockerfile has a "-plugins" builder stage (BINARYTYPE=plugins) whose
# COPY --from=reporoot reaches the full repo module (go.mod lives one
# directory above banyand/, this project's own build context), plus a later
# final-plugins stage. Both properties require touching every banyand build,
# not just BINARYTYPE=plugins:
#   - buildx/BuildKit resolves EVERY named build context referenced anywhere
#     in a Dockerfile while parsing it, BEFORE pruning stages unreached by
#     --target — so "reporoot" must be supplied even for a static/slim build,
#     or buildx tries to resolve it as a registry image and fails. Supplying
#     it is harmless: the default target's build graph never references it.
#   - without an explicit --target, docker/buildx builds the LAST stage in
#     the file, which is final-plugins (defined after final for the
#     BINARYTYPE=plugins case) — so the default/slim build must pin
#     --target final explicitly to stay on the static/busybox path.
# Guarded by NAME (set per-project Makefile) so other projects' plain
# single-stage Dockerfiles are unaffected.
ifeq ($(NAME),banyand)
	DOCKER_BUILD_ARGS := $(DOCKER_BUILD_ARGS) --build-context reporoot=..
	ifeq ($(BINARYTYPE),plugins)
		TAG := $(TAG)-plugins
		# GO_LINK_VERSION (from base.mk, included before this file by the
		# banyand Makefile) carries the version-stamp `-X` ldflags. The
		# static/slim binaries get it via `make release`; the -plugins host
		# binary is compiled inside the Dockerfile, so pass it through as a
		# build-arg. Double-quoted because the value contains spaces.
		DOCKER_BUILD_ARGS := $(DOCKER_BUILD_ARGS) --target final-plugins --build-arg "GO_LINK_VERSION=$(GO_LINK_VERSION)"
	else
		DOCKER_BUILD_ARGS := $(DOCKER_BUILD_ARGS) --target final
	endif
endif

IMG := $(HUB)/$(IMG_NAME):$(TAG)

# Disable cache in CI environment
ifeq (true,$(CI))
	DOCKER_BUILD_ARGS := $(DOCKER_BUILD_ARGS) --no-cache
endif

docker: LOAD_OR_PUSH = --load
docker: DOCKER_TYPE = "Build"
docker.push: LOAD_OR_PUSH = --push
docker.push: DOCKER_TYPE = "Push"

docker docker.push:
	@echo "$(DOCKER_TYPE) $(IMG) with platform $(PLATFORMS)"
	@pwd
	time docker buildx build $(DOCKER_BUILD_ARGS) --platform $(PLATFORMS) $(LOAD_OR_PUSH) -t $(IMG) -f Dockerfile --provenance=false --build-arg BINARYTYPE=$(BINARYTYPE) .

# --- Plugin carrier image (banyand only; see docs/operation/plugins.md, DD2) ---
# The carrier holds ONLY the built .so files, compiled in the SAME Dockerfile
# builder base (build-plugins-base) / same commit as the -plugins HOST binary,
# so the host<->.so toolchain parity plugin.Open demands holds by lockstep.
# It lives on the SAME repo as every other banyand image, distinguished only by
# the "-plugins-carrier" TAG suffix: $(HUB)/$(IMG_NAME):$(TAG)-plugins-carrier
# (the host image is $(TAG)-plugins). MUST be invoked WITHOUT BINARYTYPE=plugins
# so the base TAG is not itself suffixed, e.g.:
#   TAG=<sha> PLATFORMS=linux/amd64,linux/arm64 make -C banyand docker.plugins-carrier.push
# It builds --target final-carrier and needs the same "reporoot" named context
# as the host build (to reach the repo module); those flags are set explicitly
# here rather than reusing DOCKER_BUILD_ARGS, which pins --target final.
ifeq ($(NAME),banyand)
PLUGINS_CARRIER_IMG := $(HUB)/$(IMG_NAME):$(TAG)-plugins-carrier
CARRIER_BUILD_ARGS := --build-context reporoot=.. --target final-carrier
ifeq (true,$(CI))
	CARRIER_BUILD_ARGS := $(CARRIER_BUILD_ARGS) --no-cache
endif

docker.plugins-carrier: LOAD_OR_PUSH = --load
docker.plugins-carrier: DOCKER_TYPE = "Build"
docker.plugins-carrier.push: LOAD_OR_PUSH = --push
docker.plugins-carrier.push: DOCKER_TYPE = "Push"

.PHONY: docker.plugins-carrier docker.plugins-carrier.push
docker.plugins-carrier docker.plugins-carrier.push:
	@echo "$(DOCKER_TYPE) $(PLUGINS_CARRIER_IMG) with platform $(PLATFORMS)"
	@pwd
	time docker buildx build $(CARRIER_BUILD_ARGS) --platform $(PLATFORMS) $(LOAD_OR_PUSH) -t $(PLUGINS_CARRIER_IMG) -f Dockerfile --provenance=false .
endif

