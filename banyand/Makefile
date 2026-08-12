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

NAME := banyand
BINARIES := $(NAME)-server $(NAME)-backup $(NAME)-restore $(NAME)-lifecycle $(NAME)-migration

IMG_NAME := skywalking-banyandb

include ../scripts/build/version.mk
include ../scripts/build/base.mk
include ../scripts/build/generate_go.mk
include ../scripts/build/build.mk
include ../scripts/build/docker.mk
include ../scripts/build/test.mk
include ../scripts/build/lint.mk
include ../scripts/build/vendor.mk
include ../scripts/build/help.mk

prepare-build: generate

release: $(STATIC_BINARIES) $(SLIM_BINARIES)

# license-plugins-report prints the runtime package manifest of the "-plugins"
# HOST image (DD7 / docs/operation/plugins.md's Licensing section): unlike the
# default busybox:stable-glibc image, this variant's distroless runtime layer
# carries a glibc/ca-certificates package set not covered by `make
# license-dep`'s Go/JS module-license resolution. This target enumerates
# exactly what is present so NOTICE/LICENSE attribution can be assembled or
# verified against ground truth rather than an assumed package list; it does
# not itself write NOTICE/LICENSE.
#
# The host image is distroless (no shell, no dpkg-query), so we cannot run a
# command inside it. Instead we extract the per-package dpkg metadata distroless
# ships under /var/lib/dpkg/status.d/ via `docker create` + `docker cp` (no
# container is ever RUN) and print Package/Version pairs.
#
# MUST be invoked with BINARYTYPE=plugins, e.g.
#   BINARYTYPE=plugins make -C banyand license-plugins-report
# so docker.mk resolves $(IMG) to the "-plugins"-suffixed HOST tag. It reads
# $(IMG) directly (rather than re-appending "-plugins") to avoid a double suffix.
.PHONY: license-plugins-report
license-plugins-report: ## Print the "-plugins" host image's distroless package manifest; run with BINARYTYPE=plugins
	@case "$(IMG)" in \
	  *-plugins) ;; \
	  *) echo "ERROR: run with BINARYTYPE=plugins (got image $(IMG))" >&2; exit 1 ;; \
	esac
	@echo "# Package manifest for $(IMG) (distroless base-debian12 runtime layer)"
	@cid=$$(docker create "$(IMG)"); \
	docker cp "$$cid":/var/lib/dpkg/status.d - 2>/dev/null | tar -xO 2>/dev/null \
	  | awk '/^Package:/{p=$$2} /^Version:/{print p "\t" $$2}' | sort; \
	docker rm "$$cid" >/dev/null
