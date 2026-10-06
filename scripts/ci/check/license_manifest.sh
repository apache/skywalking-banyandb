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
# Byte-level verifier for the license artifacts produced by `make license-dep`.
#
# `git diff` is deliberately NOT used to judge these outputs: it compares blobs
# after Git's own normalization, so it can pass while the bytes on disk differ,
# and it says nothing about a file that was generated and then deleted again.
# This script hashes the raw bytes and compares complete path sets, so additions,
# deletions and content changes are all reported.
#
# Modes:
#   manifest <root>              emit "<sha256>  <relpath>" for every artifact, sorted
#   manifest-committed <root>    the same, computed from the committed blobs
#   check-committed <repo-root> compare the artifacts on disk against committed blobs
#   check-eol <root>             fail if any artifact contains a CR byte
#   check-coverage <repo-root>   fail if a project with a `license-dep` target is uncovered
#   normalize <root>             rewrite CRLF to LF in the artifacts (part of generation)
#
# Only `check-committed` needs Git, so `manifest` and `check-eol` are portable to
# every host, which is what lets the cross-OS matrix compare manifests directly.
#
# shellcheck shell=bash

set -euo pipefail

LC_ALL=C
export LC_ALL

# ---------------------------------------------------------------------------
# The complete artifact surface written by `make license-dep`.
#
# Keep in sync with docs/design/0.12.0/docker-canonical-build/README.md §1.1.
# Entries are exact paths (files or directories); a path that no longer exists
# is skipped, so a removed project drops out on its own. `check-coverage`
# fails when a project gains a `license-dep` target without an entry here, so
# the list cannot silently fall behind the tree.
#
# The embedded UI was removed upstream in 0e3ee0ec, so there is no ui entry; a
# project that gains a `license-dep` target without one is caught by
# `check-coverage` rather than being silently unverified.
#
# NOTE: the repository-root LICENSE and NOTICE are NOT in this list. They are
# not produced by these recipes, so a check that included them would pass
# vacuously and give false confidence.
# ---------------------------------------------------------------------------
LICENSE_PATHS=(
	"dist/LICENSE"
	"dist/licenses"
	"mcp/LICENSE"
	"mcp/licenses"
	"canopy/LICENSE"
	"canopy/licenses"
)

# Projects whose Makefile may own a `license-dep` target, used by check-coverage.
LICENSE_PROJECTS=(dist mcp canopy)

usage() {
	echo "usage: $0 {manifest|manifest-committed|check-committed|check-eol|check-coverage|normalize} <root>" >&2
	exit 2
}

sha256_stdin() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum | cut -d' ' -f1
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 | cut -d' ' -f1
	else
		openssl dgst -sha256 | sed 's/.*= *//'
	fi
}

sha256_of() {
	sha256_stdin <"$1"
}

# emit_artifact <root> <relpath>
emit_artifact() {
	local root="$1" rel="$2" path="$root/$rel" file
	if [ -f "$path" ]; then
		printf '%s  %s\n' "$(sha256_of "$path")" "$rel"
	elif [ -d "$path" ]; then
		while IFS= read -r -d '' file; do
			printf '%s  %s\n' "$(sha256_of "$file")" "${file#"$root"/}"
		done < <(find "$path" -type f -print0 | sort -z)
	fi
}

# LC_ALL=C sort: the manifest must be byte-comparable across hosts, so the order
# is fixed by byte value rather than by the host's collation.
mode_manifest() {
	local root="${1:-}" rel
	[ -n "$root" ] || usage
	for rel in "${LICENSE_PATHS[@]}"; do
		emit_artifact "$root" "$rel"
	done | sort -k2
}

mode_check_eol() {
	local root="${1:-}" rel path file failed=0
	local -a files=()
	[ -n "$root" ] || usage
	for rel in "${LICENSE_PATHS[@]}"; do
		path="$root/$rel"
		[ -f "$path" ] && files=("$path") || files=()
		if [ -d "$path" ]; then
			while IFS= read -r -d '' file; do
				files+=("$file")
			done < <(find "$path" -type f -print0)
		fi
		for file in "${files[@]+"${files[@]}"}"; do
			if LC_ALL=C grep -qU $'\r' "$file" 2>/dev/null; then
				echo "CR byte in ${file#"$root"/}: license artifacts must be LF" >&2
				failed=1
			fi
		done
	done
	if [ "$failed" -ne 0 ]; then
		echo "license artifacts contain CRLF; see docs/design/0.12.0/docker-canonical-build/README.md §3" >&2
	fi
	return "$failed"
}

# Blob hash from a COMMIT, not the index and not the working tree.
#
# `:$path` would read the index, and that is wrong twice over: a staged
# modification becomes the expected content, and a staged addition counts as
# already committed. Either one lets an artifact that was never generated pass
# verification. Reading a named commit (default HEAD) makes "committed" mean
# committed. The worktree is a third thing again -- `eol=lf` may have rewritten it
# during checkout -- and is never the reference.
git_blob_sha256() {
	local repo="$1" path="$2" ref="$3"
	git -C "$repo" cat-file blob "$ref:$path" 2>/dev/null | sha256_stdin
}

# Paths present in a COMMIT under a given path prefix. Using ls-tree rather than
# ls-files is what keeps a staged addition out of the expected set and a staged
# deletion in it.
git_committed_paths() {
	local repo="$1" path="$2" ref="$3"
	git -C "$repo" ls-tree -r -z --name-only "$ref" -- "$path" | sort -z
}

mode_check_committed() {
	local repo="${1:-}" rel path file rel_file want got failed=0 ref
	[ -n "$repo" ] || usage
	command -v git >/dev/null 2>&1 || {
		echo "check-committed requires git" >&2
		exit 1
	}
	# Compared against a COMMIT (default HEAD), never the index and never the
	# worktree. A second argument selects a different reference, which is how the
	# tests can describe a commit other than the checked-out one.
	ref="${2:-HEAD}"
	git -C "$repo" rev-parse --verify -q "$ref" >/dev/null 2>&1 || {
		echo "cannot resolve the reference '$ref'" >&2
		exit 1
	}

	for rel in "${LICENSE_PATHS[@]}"; do
		path="$repo/$rel"

		# Files on disk that are not committed at all. A newly generated
		# license file is invisible to `git diff` when it is untracked, and
		# `check-format` only catches it because the whole tree is staged.
		if [ -f "$path" ]; then
			walk=("$path")
		else
			walk=()
		fi
		if [ -d "$path" ]; then
			while IFS= read -r -d '' file; do
				walk+=("$file")
			done < <(find "$path" -type f -print0)
		fi
		for file in "${walk[@]+"${walk[@]}"}"; do
			rel_file="${file#"$repo"/}"
			if ! git -C "$repo" cat-file -e "$ref:$rel_file" 2>/dev/null; then
				echo "not present in $ref: $rel_file" >&2
				failed=1
				continue
			fi
			want=$(git_blob_sha256 "$repo" "$rel_file" "$ref")
			got=$(sha256_of "$file")
			if [ "$want" != "$got" ]; then
				echo "content differs from committed blob: $rel_file" >&2
				failed=1
			fi
		done

		# Committed artifacts that are gone from the tree. A resolution
		# failure inside license-eye produces exactly this and still exits 0
		# (it logs npm install failures instead of returning them).
		while IFS= read -r -d '' rel_file; do
			if [ ! -f "$repo/$rel_file" ]; then
				echo "committed artifact missing from the tree: $rel_file" >&2
				failed=1
			fi
		done < <(git_committed_paths "$repo" "$rel" "$ref")
	done

	if [ "$failed" -ne 0 ]; then
		echo "license artifacts drifted from $ref; run 'make license-dep' and commit the result" >&2
	fi
	return "$failed"
}

mode_check_coverage() {
	local repo="${1:-}" project failed=0
	[ -n "$repo" ] || usage
	while IFS= read -r -d '' project; do
		# Only the leaf name matters: a project is one level below the root.
		# Basename rather than prefix-stripping, because a macOS temp dir is
		# a symlink (/var → /private/var) and find prints the resolved path.
		project="${project##*/}"
		[ "$project" = "." ] && continue
		grep -q '^license-dep:' "$repo/$project/Makefile" 2>/dev/null || continue
		case " ${LICENSE_PROJECTS[*]} " in
		*" $project "*) continue ;;
		esac
		echo "project $project has a license-dep target but is not covered by" \
			"scripts/ci/check/license_manifest.sh (LICENSE_PATHS / LICENSE_PROJECTS)" >&2
		failed=1
	done < <(find "$repo" -mindepth 1 -maxdepth 1 -type d -print0)
	return "$failed"
}

# normalize <root> — rewrite CRLF to LF in the artifact text files.
#
# Part of generation, not part of verification: license-eye copies license
# bodies verbatim out of node_modules and the Go module cache, and a handful of
# npm packages ship CRLF. With .gitattributes declaring `eol=lf`, git
# normalizes the blob on staging, so a CRLF artifact disagrees with its own
# committed blob forever and check-committed can never be satisfied. Doing this
# in the shared target — rather than inside the container — is what keeps the
# native and docker paths byte-identical, and what makes a Windows contributor's
# native run match CI.
# manifest-committed <repo-root> — the same manifest, computed from the committed
# blobs. Comparing a generated manifest against this is the strongest form of
# the cross-host check: it ties every host's output to what is actually
# committed, rather than to a working tree that `eol=lf` may have rewritten
# during checkout.
mode_manifest_committed() {
	local repo="${1:-}" rel rel_file ref
	[ -n "$repo" ] || usage
	ref="${2:-HEAD}"
	git -C "$repo" rev-parse --verify -q "$ref" >/dev/null 2>&1 || {
		echo "cannot resolve the reference '$ref'" >&2
		exit 1
	}
	for rel in "${LICENSE_PATHS[@]}"; do
		while IFS= read -r -d '' rel_file; do
			[ -n "$rel_file" ] || continue
			printf '%s  %s\n' "$(git_blob_sha256 "$repo" "$rel_file" "$ref")" "$rel_file"
		done < <(git_committed_paths "$repo" "$rel" "$ref")
	done | LC_ALL=C sort -k2
}

mode_normalize() {
	local root="${1:-}" rel path file tmp
	local -a files=()
	[ -n "$root" ] || usage
	for rel in "${LICENSE_PATHS[@]}"; do
		path="$root/$rel"
		[ -f "$path" ] && files=("$path") || files=()
		if [ -d "$path" ]; then
			while IFS= read -r -d '' file; do
				files+=("$file")
			done < <(find "$path" -type f -print0)
		fi
		# Every file under the artifact paths, the same set the manifest and the
		# LF assertion walk. Normalizing a subset would leave the others able to
		# fail the LF check right after being normalized.
		for file in "${files[@]+"${files[@]}"}"; do
			if LC_ALL=C grep -qU $'\r' "$file" 2>/dev/null; then
				echo "normalizing CRLF in ${file#"$root"/}"
				# Not `sed -i`: GNU sed takes the suffix as an argument and BSD sed
				# requires one, so the portable form is a temp file plus a rename.
				# `sed -i 's/.../' file` works on GNU and fails on stock macOS.
				tmp="$file.normalize.$$"
				LC_ALL=C sed 's/\r$//' "$file" >"$tmp" && mv "$tmp" "$file" || {
					rm -f "$tmp"
					echo "failed to normalize $file" >&2
					return 1
				}
			fi
		done
	done
}

main() {
	local mode="${1:-}"
	shift || true
	case "$mode" in
	manifest) mode_manifest "$@" ;;
	manifest-committed) mode_manifest_committed "$@" ;;
	check-committed) mode_check_committed "$@" ;;
	check-eol) mode_check_eol "$@" ;;
	check-coverage) mode_check_coverage "$@" ;;
	normalize) mode_normalize "$@" ;;
	*) usage ;;
	esac
}

main "$@"
