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
# Resolves and guards the Node version.
#
# The Node version is read from the projects that declare one — mcp and canopy —
# for the same reason Go is read from go.mod: those are the declarations the Node
# tooling itself checks at install time. Inventing a declaration in a build file
# would create a second fact that can silently disagree, and the failure mode is a
# contributor resolving a different license set from CI.
#
# The two subprojects are kept on the SAME version, both pinning it exactly, and
# this script is what enforces that: resolve fails if they differ, so the image can
# never be built from a version only one of them asked for. A floor like ">=24.6.0"
# is refused for the same reason — it does not identify one version, and the image
# needs one.
#
#   resolve <repo-root>   print the version mcp and canopy agree on
#   check   <repo-root>   verify every project's engines.node against that pin
#
# There is no generated version file. `actions/setup-node` reads engines.node
# straight from a package.json given as `node-version-file`, so the workflows
# point at canopy/package.json and there is nothing to keep in step.
#
# canopy pins an exact version and mcp declares a floor, so `check` verifies the
# resolved pin satisfies every other project rather than trusting the two to agree.

set -euo pipefail

LC_ALL=C
export LC_ALL

usage() {
	echo "usage: $0 {resolve|check} <repo-root>" >&2
	exit 2
}

repo="${1:-}"

# NODE_SOURCES — the projects whose engines.node declares the version. Both must
# exist and both must pin it exactly to the same value.
NODE_SOURCES=(mcp canopy)

engines_of() { # engines_of <project> -> the raw engines.node string
	local manifest="$repo/$1/package.json"
	[ -f "$manifest" ] || return 0
	sed -n 's/.*"node"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$manifest" | head -1
}

require_exact_pin() { # require_exact_pin <project> <value>
	local project="$1" value="$2"
	# An anchored numeric match, not a glob. A glob such as [0-9]*.[0-9]*.[0-9]*
	# also accepts "24.6.0 || 25.0.0" and "24x.6x.0x", which would resolve to two
	# versions or to none.
	if printf '%s' "$value" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
		return 0
	fi
	echo "$project/package.json declares node '$value'." >&2
	if [ -z "$value" ]; then
		echo "  It has no engines.node, so it cannot take part in the version decision." >&2
	else
		echo "  The build image needs one specific version, so this must be an exact pin." >&2
		echo "  A range leaves the choice to whoever builds it, which is exactly the drift" >&2
		echo "  this check exists to prevent." >&2
	fi
	exit 1
}

# resolve_version — the single version that every declaring project agrees on.
resolve_version() {
	local project value resolved=""
	for project in "${NODE_SOURCES[@]}"; do
		if [ ! -f "$repo/$project/package.json" ]; then
			echo "$project/package.json not found; it declares the Node version" >&2
			exit 1
		fi
		value="$(engines_of "$project")"
		require_exact_pin "$project" "$value"
		if [ -z "$resolved" ]; then
			resolved="$value"
		elif [ "$value" != "$resolved" ]; then
			echo "the Node subprojects disagree:" >&2
			for project in "${NODE_SOURCES[@]}"; do
				echo "  $project pins $(engines_of "$project")" >&2
			done >&2
			echo "  Align them first; the build image can only use one version." >&2
			exit 1
		fi
	done
	printf '%s' "$resolved"
}

mode_check() {
	local resolved manifest project requirement pinned required failed=0
	resolved="$(resolve_version)"
	pinned="$resolved"

	# Any other project that declares one must be satisfied by that same pin, so
	# the image version is never narrower than what a subproject asks for.
	while IFS= read -r -d '' manifest; do
		project="${manifest#"$repo"/}"
		project="${project%/package.json}"
		requirement="$(sed -n 's/.*"node"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$manifest" | head -1)"
		[ -n "$requirement" ] || continue

		case "$requirement" in
		">="*)
			required="${requirement#>=}"
			# A newer pin satisfies a floor.
			if [ "$(printf '%s\n%s\n' "$required" "$pinned" | sort -V | head -1)" != "$required" ]; then
				echo "$project requires node >=$required but mcp and canopy pin $pinned" >&2
				failed=1
			fi
			;;
		"^"*|"~"*|">"*|"<"*|"<="*|*" "*|*"||"*)
			echo "$project declares an engines.node range ('$requirement') this check does not" \
				"understand; extend node_version.sh or pin it exactly" >&2
			failed=1
			;;
		*)
			if [ "$requirement" != "$pinned" ]; then
				echo "$project pins node $requirement but mcp and canopy pin $pinned" >&2
				failed=1
			fi
			;;
		esac
		# Recursive, not depth-limited. canopy/web/package.json and
		# canopy/server/package.json are real examples at depth 3 that a
		# -maxdepth 2 search skipped silently -- the same class of bug as a
		# version checked in one place and missed in another.
	done < <(find "$repo" \
		-name package.json \
		-not -path '*/node_modules/*' \
		-not -path '*/dist/*' \
		-not -path '*/licenses/*' \
		-print0)

	exit "$failed"
}

main() {
	case "${1:-}" in
	resolve)
		shift
		repo="${1:-}"
		[ -n "$repo" ] || usage
		resolve_version
		printf '\n'
		;;
	check)
		shift
		repo="${1:-}"
		[ -n "$repo" ] || usage
		mode_check
		;;
	*)
		usage
		;;
	esac
}

main "$@"

