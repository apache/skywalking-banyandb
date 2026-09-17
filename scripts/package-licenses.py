#!/usr/bin/env python3
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
"""Build a binary-package LICENSE and licenses/ directory from shipped Go binaries.

ASF binary distributions must describe the exact contents of that archive, not the
union of every module in the repository go.mod. This reads `go version -m` from the
packaged executables, keeps matching rows from dist/LICENSE, and copies the matching
license-eye text files. Optional extra LICENSE fragments (UI, MCP) are appended with
their license directories.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

MAIN_MODULE = "github.com/apache/skywalking-banyandb"
# license-eye: filename := regexp.MustCompile(`[^a-zA-Z0-9\\.\-]`).ReplaceAll(dep, "-")
LICENSE_FILE_UNSAFE = re.compile(r"[^a-zA-Z0-9\\.\-]")
DEP_LINE = re.compile(r"^    (\S+) (\S+) (.+?)\s*$")
SECTION_START = re.compile(r"^={8,}\s*$")
UI_OR_MCP_MARKER = re.compile(r"(?i)(UI related licenses|mcp related licenses)")
GO_VERSION_DEP = re.compile(r"^\t(?:dep|mod)\t(\S+)\t")


def license_filename(dep_name: str) -> str:
    """Return the license-eye filename for a dependency name."""
    return "license-" + LICENSE_FILE_UNSAFE.sub("-", dep_name) + ".txt"


def modules_from_bins(bin_dir: Path) -> set[str]:
    """Return module paths linked into Go executables under bin_dir."""
    modules: set[str] = set()
    scanned = 0
    for path in sorted(bin_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.name.endswith(".lock") or path.name.startswith("._"):
            continue
        result = subprocess.run(
            ["go", "version", "-m", str(path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            continue
        scanned += 1
        for line in result.stdout.splitlines():
            match = GO_VERSION_DEP.match(line)
            if not match:
                continue
            module = match.group(1)
            if module != MAIN_MODULE:
                modules.add(module)
    if scanned == 0:
        raise SystemExit(f"no Go executables found under {bin_dir}")
    if not modules:
        raise SystemExit(f"no third-party Go modules reported by go version -m under {bin_dir}")
    return modules


def modules_from_file(path: Path) -> set[str]:
    """Return module paths listed one per line in path."""
    modules = {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.startswith("#")}
    modules.discard(MAIN_MODULE)
    if not modules:
        raise SystemExit(f"no modules listed in {path}")
    return modules


def split_go_license(text: str) -> str:
    """Keep the Apache header and Go dependency listing; drop concatenated UI/MCP sections."""
    lines = text.splitlines(keepends=True)
    kept: list[str] = []
    for line in lines:
        if UI_OR_MCP_MARKER.search(line):
            break
        kept.append(line)
    return "".join(kept).rstrip() + "\n"


def filter_license(go_license: str, allowed: set[str]) -> tuple[str, set[str]]:
    """Filter dist/LICENSE Go groups to allowed modules. Returns (text, kept names)."""
    lines = split_go_license(go_license).splitlines()
    header: list[str] = []
    idx = 0
    while idx < len(lines) and not SECTION_START.match(lines[idx]):
        header.append(lines[idx])
        idx += 1
    groups: list[tuple[list[str], list[str]]] = []
    while idx < len(lines):
        if not SECTION_START.match(lines[idx]):
            idx += 1
            continue
        group_header = [lines[idx]]
        idx += 1
        while idx < len(lines) and not SECTION_START.match(lines[idx]) and not DEP_LINE.match(lines[idx]):
            group_header.append(lines[idx])
            idx += 1
        if idx < len(lines) and SECTION_START.match(lines[idx]):
            group_header.append(lines[idx])
            idx += 1
        deps: list[str] = []
        while idx < len(lines) and not SECTION_START.match(lines[idx]):
            line = lines[idx]
            idx += 1
            dep_match = DEP_LINE.match(line)
            if dep_match and dep_match.group(1) in allowed:
                deps.append(line)
        if deps:
            groups.append((group_header, deps))
    kept_names = {DEP_LINE.match(dep).group(1) for _, deps in groups for dep in deps if DEP_LINE.match(dep)}
    out: list[str] = list(header)
    while out and out[-1] == "":
        out.pop()
    out.append("")
    for group_header, deps in groups:
        out.append("")
        out.extend(group_header)
        out.append("")
        out.extend(deps)
    out.append("")
    return "\n".join(out), kept_names


def copy_license_texts(licenses_dir: Path, dest_dir: Path, names: set[str]) -> None:
    """Copy license-eye text files for names from licenses_dir into dest_dir."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    missing: list[str] = []
    for name in sorted(names):
        filename = license_filename(name)
        src = licenses_dir / filename
        if not src.is_file():
            missing.append(f"{name} -> {filename}")
            continue
        shutil.copy2(src, dest_dir / filename)
    if missing:
        print("warning: no license text file for:", file=sys.stderr)
        for item in missing:
            print(f"  {item}", file=sys.stderr)


def copy_tree(src: Path, dest: Path) -> None:
    """Copy src directory to dest, replacing dest if it exists."""
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--license", required=True, type=Path, help="dist/LICENSE from license-eye")
    parser.add_argument("--licenses-dir", required=True, type=Path, help="dist/licenses from license-eye")
    parser.add_argument("--out", required=True, type=Path, help="package root to write LICENSE and licenses/")
    parser.add_argument("--bins", type=Path, help="directory of packaged Go executables")
    parser.add_argument("--modules-file", type=Path, help="module paths, one per line (test seam)")
    parser.add_argument("--extra-license", action="append", default=[], type=Path, help="LICENSE fragment to append (UI/MCP)")
    parser.add_argument("--extra-licenses-dir", action="append", default=[], type=Path, help="license text dir to copy next to extras")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if bool(args.bins) == bool(args.modules_file):
        raise SystemExit("exactly one of --bins or --modules-file is required")
    if len(args.extra_license) != len(args.extra_licenses_dir):
        raise SystemExit("--extra-license and --extra-licenses-dir must be given in pairs")
    modules = modules_from_bins(args.bins) if args.bins else modules_from_file(args.modules_file)
    go_license = args.license.read_text(encoding="utf-8")
    filtered, kept = filter_license(go_license, modules)
    missing_from_license = sorted(modules - kept)
    if missing_from_license:
        print("error: binaries link modules that are not listed in dist/LICENSE:", file=sys.stderr)
        for name in missing_from_license:
            print(f"  {name}", file=sys.stderr)
        return 1
    args.out.mkdir(parents=True, exist_ok=True)
    dest_licenses = args.out / "licenses"
    if dest_licenses.exists():
        shutil.rmtree(dest_licenses)
    dest_licenses.mkdir(parents=True)
    copy_license_texts(args.licenses_dir, dest_licenses, kept)
    extras: list[str] = []
    for license_path, extra_dir in zip(args.extra_license, args.extra_licenses_dir):
        extras.append(license_path.read_text(encoding="utf-8").rstrip() + "\n")
        copy_tree(extra_dir, dest_licenses / extra_dir.name)
    (args.out / "LICENSE").write_text(filtered.rstrip() + "\n\n" + "\n".join(extras), encoding="utf-8")
    print(f"wrote {args.out / 'LICENSE'} with {len(kept)} Go modules", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
