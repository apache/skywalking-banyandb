#!/usr/bin/env bash
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
# Entrypoint of the pinned build environment.
#
#   docker run -i <image> <make-target>...
#
# The worktree is streamed in over stdin rather than bind-mounted: the container
# runs `npm ci` and `npm prune --production` as part of license resolution, and
# doing that against a mounted host tree replaces the host's node_modules with
# Linux packages and leaves root-owned files behind. A bind mount also breaks on
# drive letters, spaces and linked Git worktrees, none of which the host's own
# path can paper over. See design §6.1.
#
# The image and this script assume the caller has already created /work and
# /out and passed a suitable license_manifest.sh into the workspace.

set -euo pipefail

LC_ALL=C
export LC_ALL

WORK_DIR="${WORK_DIR:-/tmp/work}"
OUT_DIR="${OUT_DIR:-/out}"

# Run as the host's uid (see dockerize.mk) so the artifacts written into the
# mounted /out belong to the invoking user rather than to root. That is what
# lets the wrapper untar them back over the worktree without sudo, and it also
# keeps `npm ci` from leaving root-owned files behind. The image's caches live
# under /go and /root, which a non-root uid cannot write, so they move to /tmp.
if [ "$(id -u)" -ne 0 ]; then
	export HOME=/tmp/home
	export GOPATH=/tmp/go
	export GOMODCACHE=/tmp/go/pkg/mod
	export GOCACHE=/tmp/go-build
	export npm_config_cache=/tmp/npm-cache
	mkdir -p "$HOME" "$GOMODCACHE" "$GOCACHE" "$npm_config_cache"
fi

# The complete artifact surface written by `make license-dep`. Keep in sync with
# LICENSE_PATHS in scripts/ci/check/license_manifest.sh and GENERATED_OUTPUTS in
# scripts/build/dockerize.mk; the three are the same list, and the copy-back
# removes each of them before extracting, so a file the generator did not produce
# cannot survive.
ARTIFACTS=(
	"dist/LICENSE"
	"dist/licenses"
	"mcp/LICENSE"
	"mcp/licenses"
	"canopy/LICENSE"
	"canopy/licenses"
)

die() {
	echo "entrypoint: $*" >&2
	exit 1
}

# --- Unpack the tree that the caller streamed in over stdin ------------------
# The wrapper does `tar -C <root> --exclude=... -cf - . | docker run -i ...`,
# so the sources arrive on stdin rather than through a volume.
mkdir -p "$WORK_DIR"
if [ ! -f "$WORK_DIR/go.mod" ]; then
	echo "entrypoint: unpacking the streamed tree into $WORK_DIR"
	# The tar status is kept. A truncated stream can deliver go.mod early and then
	# stop, so testing only for go.mod afterwards would happily generate from a
	# partial tree. The reason is reported rather than whatever tar said, and the
	# run still fails.
	tar_status=0
	tar -xf - -C "$WORK_DIR" 2>/dev/null || tar_status=$?
	[ "$tar_status" -eq 0 ] || die "the streamed source tree is incomplete (tar exited $tar_status); refusing to generate from a partial tree"
fi
[ -f "$WORK_DIR/go.mod" ] || die "no go.mod under $WORK_DIR; the streamed tree was empty"

# --- Cross-check the toolchain against the repo's own declarations ------------
# A stale image is the most likely failure here, and GOTOOLCHAIN=local turns the
# Go half of that into a build error. This turns it into a sentence.
want_go="$(sed -n 's/^go //p' "$WORK_DIR/go.mod" | head -1)"
have_go="$(go version | sed -n 's/^go version go\([0-9.]*\).*/\1/p')"
[ -n "$want_go" ] || die "cannot read the Go version from go.mod"
[ "$want_go" = "$have_go" ] ||
	die "image has Go $have_go but go.mod requires $want_go; run 'make bump-build-image'"

# The Node version is declared by the mcp and canopy package.json files, which
# are streamed in with the tree, so the image can be checked against the very
# declarations this build derives it from.
for manifest in mcp canopy; do
	want_node="$(sed -n 's/.*"node"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$WORK_DIR/$manifest/package.json" 2>/dev/null | head -1)"
	[ -n "$want_node" ] || die "cannot read engines.node from $manifest/package.json"
	have_node="$(node --version | sed 's/^v//')"
	[ "$want_node" = "$have_node" ] ||
		die "image has Node $have_node but $manifest pins $want_node; fix scripts/build/version.mk's consumer (the resolver) and rebuild"
done

command -v license-eye >/dev/null 2>&1 || die "license-eye is not on PATH in the image"
# scripts/build/license.mk assigns LICENSE_EYE with `:=`, and a makefile
# assignment beats an environment variable -- so exporting this is NOT enough and
# the container would quietly `go install` the tool again. It is passed as a
# command-line variable in the `make` invocation below, which does win, and which
# every recursive $(MAKE) also inherits.
Baked_license_eye=/usr/local/bin/license-eye
command -v "$Baked_license_eye" >/dev/null 2>&1 || die "license-eye is not baked into the image"

[ "$#" -gt 0 ] || die "usage: <image> <make-target>..."
echo "entrypoint: running make $* in $WORK_DIR"

# The sources are read-only. The recipes write only into the artifact paths
# below, and anything unexpected is left behind for the caller to notice rather
# than silently copied out.
# Command-line assignment: overrides the makefile's own `:=` and propagates to
# every recursive $(MAKE).
make -C "$WORK_DIR" LICENSE_EYE="$Baked_license_eye" "$@" || die "make $* failed in the container"

# --- Collect the artifacts ---------------------------------------------------
mkdir -p "$OUT_DIR"
for rel in "${ARTIFACTS[@]}"; do
	if [ -d "$WORK_DIR/$rel" ]; then
		mkdir -p "$OUT_DIR/$(dirname "$rel")"
		cp -R "$WORK_DIR/$rel" "$OUT_DIR/$rel"
	elif [ -f "$WORK_DIR/$rel" ]; then
		mkdir -p "$OUT_DIR/$(dirname "$rel")"
		cp "$WORK_DIR/$rel" "$OUT_DIR/$rel"
	fi
done

# NOTE: CRLF normalization is deliberately NOT done here. It lives in the
# `license-dep` target itself (scripts/ci/check/license_manifest.sh normalize)
# so that the native and container paths produce the same bytes; doing it only
# in the container would make the two disagree, which is the one thing the
# cross-check in CI exists to catch.

# --- Report the manifest ------------------------------------------------------
# Printed on stdout so a host can capture it with `tee` instead of
# reimplementing the walk, and so the cross-host matrix compares exactly these
# bytes rather than something Git has had an opinion about.
echo "entrypoint: manifest of $OUT_DIR"
while IFS= read -r -d '' file; do
	printf '%s  %s\n' \
		"$(sha256sum <"$file" | cut -d' ' -f1)" \
		"${file#"$OUT_DIR"/}"
done < <(find "$OUT_DIR" -type f -print0 | sort -z)

echo "entrypoint: artifacts written to $OUT_DIR"
