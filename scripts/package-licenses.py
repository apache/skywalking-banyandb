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
"""Build package-specific LICENSE, NOTICE, README, and licenses/ from shipped binaries.

ASF binary distributions must describe the exact contents of that archive. This
reads `go version -m` from packaged executables, keeps matching rows from
dist/LICENSE, copies matching license-eye texts, assembles NOTICE from the ASF
base plus reviewed obligation fragments, and appends prominent Category B
disclosures to the packaged README.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

MAIN_MODULE = "github.com/apache/skywalking-banyandb"
# license-eye: filename := regexp.MustCompile(`[^a-zA-Z0-9\\.\-]`).ReplaceAll(dep, "-")
LICENSE_FILE_UNSAFE = re.compile(r"[^a-zA-Z0-9\\.\-]")
DEP_LINE = re.compile(r"^    (\S+) (\S+) (.+?)\s*$")
SECTION_START = re.compile(r"^={8,}\s*$")
UI_OR_MCP_MARKER = re.compile(r"(?i)(UI related licenses|mcp related licenses)")
GO_VERSION_DEP = re.compile(r"^\t(?:dep|mod)\t(\S+)\t(\S+)\t")
GO_VERSION_REPLACE = re.compile(r"^\t=>\t(\S+)\t(\S+)\t")
LICENSE_SECTION = re.compile(r"(?im)^## License\s*$")


@dataclass(frozen=True)
class ModuleRef:
    """A resolved Go module identity linked into a package."""

    name: str
    version: str


@dataclass
class Inventory:
    """Modules linked into packaged Go executables."""

    modules: dict[str, ModuleRef] = field(default_factory=dict)
    by_executable: dict[str, list[ModuleRef]] = field(default_factory=dict)

    def names(self) -> set[str]:
        return set(self.modules)


def license_filename(dep_name: str) -> str:
    """Return the license-eye filename for a dependency name."""
    return "license-" + LICENSE_FILE_UNSAFE.sub("-", dep_name) + ".txt"


def modules_from_version_m(text: str) -> dict[str, ModuleRef]:
    """Return resolved module refs from `go version -m` output.

    When a dep is replaced, license-eye lists the replacement path (the code
    actually linked), so prefer the `=>` path and version over the require path.
    """
    modules: dict[str, ModuleRef] = {}
    pending: ModuleRef | None = None
    for line in text.splitlines():
        replace_match = GO_VERSION_REPLACE.match(line)
        if replace_match and pending is not None:
            modules.pop(pending.name, None)
            replacement = ModuleRef(replace_match.group(1), replace_match.group(2))
            if replacement.name != MAIN_MODULE:
                modules[replacement.name] = replacement
            pending = None
            continue
        match = GO_VERSION_DEP.match(line)
        if not match:
            pending = None
            continue
        pending = ModuleRef(match.group(1), match.group(2))
        if pending.name != MAIN_MODULE:
            modules[pending.name] = pending
    return modules


def inventory_from_bins(bin_dir: Path) -> Inventory:
    """Return inventory for Go executables under bin_dir."""
    inventory = Inventory()
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
        mods = modules_from_version_m(result.stdout)
        inventory.by_executable[str(path.relative_to(bin_dir))] = list(mods.values())
        inventory.modules.update(mods)
    if scanned == 0:
        raise SystemExit(f"no Go executables found under {bin_dir}")
    if not inventory.modules:
        raise SystemExit(f"no third-party Go modules reported by go version -m under {bin_dir}")
    return inventory


def inventory_from_modules_file(path: Path) -> Inventory:
    """Return inventory from lines of `name version` (test seam)."""
    inventory = Inventory()
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split()
        if len(parts) == 1:
            name, version = parts[0], "unknown"
        else:
            name, version = parts[0], parts[1]
        if name == MAIN_MODULE:
            continue
        inventory.modules[name] = ModuleRef(name, version)
    if not inventory.modules:
        raise SystemExit(f"no modules listed in {path}")
    return inventory


def split_go_license(text: str) -> str:
    """Keep the Apache header and Go dependency listing; drop concatenated UI/MCP sections."""
    lines = text.splitlines(keepends=True)
    kept: list[str] = []
    for line in lines:
        if UI_OR_MCP_MARKER.search(line):
            break
        kept.append(line)
    return "".join(kept).rstrip() + "\n"


def filter_license(go_license: str, allowed: set[str]) -> tuple[str, set[str], dict[str, str], dict[str, str]]:
    """Filter dist/LICENSE Go groups to allowed modules.

    Returns (text, kept names, name->version, name->license expression).
    """
    lines = split_go_license(go_license).splitlines()
    header: list[str] = []
    idx = 0
    while idx < len(lines) and not SECTION_START.match(lines[idx]):
        header.append(lines[idx])
        idx += 1
    groups: list[tuple[list[str], list[str]]] = []
    versions: dict[str, str] = {}
    expressions: dict[str, str] = {}
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
                versions[dep_match.group(1)] = dep_match.group(2)
                expressions[dep_match.group(1)] = dep_match.group(3)
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
    return "\n".join(out), kept_names, versions, expressions


def copy_license_texts(licenses_dir: Path, dest_dir: Path, names: set[str], fallback_dir: Path | None = None) -> None:
    """Copy license-eye text files for names from licenses_dir into dest_dir."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    missing: list[str] = []
    for name in sorted(names):
        filename = license_filename(name)
        src = licenses_dir / filename
        if (not src.is_file() or src.stat().st_size == 0) and fallback_dir is not None:
            fallback = fallback_dir / filename
            if fallback.is_file() and fallback.stat().st_size > 0:
                src = fallback
        if not src.is_file() or src.stat().st_size == 0:
            missing.append(f"{name} -> {filename}")
            continue
        shutil.copy2(src, dest_dir / filename)
    if missing:
        print("error: missing required license text file(s):", file=sys.stderr)
        for item in missing:
            print(f"  {item}", file=sys.stderr)
        raise SystemExit(1)


def copy_tree(src: Path, dest: Path) -> None:
    """Copy src directory to dest, replacing dest if it exists."""
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest)


def load_obligations(path: Path | None) -> list[dict]:
    """Load reviewed obligation components, or an empty list when unset."""
    if path is None:
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise SystemExit(f"unsupported obligations schema_version in {path}")
    components = data.get("components")
    if not isinstance(components, list):
        raise SystemExit(f"obligations components must be a list in {path}")
    return components


def assemble_notice(base_notice: str, legal_root: Path, components: list[dict], inventory: Inventory) -> str:
    """Assemble package NOTICE from ASF base plus applicable reviewed fragments."""
    parts = [base_notice.rstrip() + "\n"]
    for component in components:
        if component.get("ecosystem") != "go":
            continue
        name = component["name"]
        version = component["version"]
        linked = inventory.modules.get(name)
        if linked is None:
            continue
        if linked.version != version:
            raise SystemExit(
                f"bundled {name} version {linked.version} does not match reviewed obligation version {version}"
            )
        notice = component.get("notice") or {}
        decision = notice.get("decision")
        if decision is None:
            raise SystemExit(f"obligation for {name}@{version} is missing notice.decision")
        if decision == "none":
            continue
        if decision != "include":
            raise SystemExit(f"unknown notice.decision {decision!r} for {name}@{version}")
        fragment = notice.get("fragment")
        if not fragment:
            raise SystemExit(f"obligation for {name}@{version} requires notice.fragment")
        fragment_path = legal_root / fragment
        if not fragment_path.is_file():
            raise SystemExit(f"missing NOTICE fragment {fragment_path}")
        text = fragment_path.read_text(encoding="utf-8").rstrip() + "\n"
        parts.append("\n--------------------------------------------------------------------\n\n")
        parts.append(text)
    return "".join(parts)


def render_disclosures(components: list[dict], inventory: Inventory, license_expressions: dict[str, str]) -> str:
    """Render prominent bundled third-party disclosures for the packaged README."""
    rows: list[str] = []
    for component in components:
        if component.get("ecosystem") != "go":
            continue
        disclosure = component.get("disclosure") or {}
        if not disclosure.get("required"):
            continue
        name = component["name"]
        version = component["version"]
        linked = inventory.modules.get(name)
        if linked is None:
            continue
        if linked.version != version:
            raise SystemExit(
                f"bundled {name} version {linked.version} does not match reviewed obligation version {version}"
            )
        license_expr = license_expressions.get(name, "see LICENSE")
        short = name.rsplit("/", 1)[-1]
        rows.append(f"This distribution includes {short} {version} ({name}) under {license_expr}.")
        rows.append("")
        rows.append(f"- Homepage: {disclosure['homepage']}")
        rows.append(f"- Corresponding source: {disclosure['source_url']}")
        rows.append(f"- License: licenses/{license_filename(name)}")
        rows.append("")
    if not rows:
        return ""
    return "## Bundled third-party components\n\n" + "\n".join(rows).rstrip() + "\n"


def write_readme(base_readme: str, disclosures: str, dest: Path) -> None:
    """Write README with disclosures inserted before the License section when present."""
    if not disclosures:
        dest.write_text(base_readme if base_readme.endswith("\n") else base_readme + "\n", encoding="utf-8")
        return
    match = LICENSE_SECTION.search(base_readme)
    if match:
        text = base_readme[: match.start()] + disclosures + "\n" + base_readme[match.start() :]
    else:
        text = base_readme.rstrip() + "\n\n" + disclosures
    dest.write_text(text if text.endswith("\n") else text + "\n", encoding="utf-8")


def write_inventory(path: Path | None, inventory: Inventory) -> None:
    """Optionally write a machine-readable package inventory."""
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "modules": [
            {"name": ref.name, "version": ref.version}
            for ref in sorted(inventory.modules.values(), key=lambda item: item.name)
        ],
        "executables": {
            exe: [{"name": ref.name, "version": ref.version} for ref in refs]
            for exe, refs in sorted(inventory.by_executable.items())
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--license", required=True, type=Path, help="dist/LICENSE from license-eye")
    parser.add_argument("--licenses-dir", required=True, type=Path, help="dist/licenses from license-eye")
    parser.add_argument("--out", required=True, type=Path, help="package root to write LICENSE and licenses/")
    parser.add_argument("--bins", type=Path, help="directory of packaged Go executables")
    parser.add_argument("--modules-file", type=Path, help="module paths, one per line (test seam)")
    parser.add_argument("--extra-license", action="append", default=[], type=Path, help="LICENSE fragment to append (UI/MCP)")
    parser.add_argument("--extra-licenses-dir", action="append", default=[], type=Path, help="license text dir to copy next to extras")
    parser.add_argument("--notice", type=Path, help="base ASF NOTICE (dist/NOTICE)")
    parser.add_argument("--readme", type=Path, help="base README.md to enrich with disclosures")
    parser.add_argument("--obligations", type=Path, help="dist/legal/obligations.json")
    parser.add_argument("--legal-root", type=Path, help="root for notice fragments (defaults to obligations parent)")
    parser.add_argument("--inventory-out", type=Path, help="optional machine-readable inventory path")
    parser.add_argument(
        "--license-texts-fallback",
        type=Path,
        help="optional directory of reviewed license texts for Eyes empties",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if bool(args.bins) == bool(args.modules_file):
        raise SystemExit("exactly one of --bins or --modules-file is required")
    if len(args.extra_license) != len(args.extra_licenses_dir):
        raise SystemExit("--extra-license and --extra-licenses-dir must be given in pairs")
    inventory = inventory_from_bins(args.bins) if args.bins else inventory_from_modules_file(args.modules_file)
    go_license = args.license.read_text(encoding="utf-8")
    filtered, kept, license_versions, license_expressions = filter_license(go_license, inventory.names())
    missing_from_license = sorted(inventory.names() - kept)
    if missing_from_license:
        print("error: binaries link modules that are not listed in dist/LICENSE:", file=sys.stderr)
        for name in missing_from_license:
            print(f"  {name}", file=sys.stderr)
        return 1
    for name, ref in sorted(inventory.modules.items()):
        listed = license_versions.get(name)
        if listed is not None and listed != ref.version and ref.version != "unknown":
            print(
                f"error: version mismatch for {name}: binary links {ref.version}, dist/LICENSE lists {listed}",
                file=sys.stderr,
            )
            return 1
    args.out.mkdir(parents=True, exist_ok=True)
    dest_licenses = args.out / "licenses"
    if dest_licenses.exists():
        shutil.rmtree(dest_licenses)
    dest_licenses.mkdir(parents=True)
    copy_license_texts(args.licenses_dir, dest_licenses, kept, fallback_dir=args.license_texts_fallback)
    extras: list[str] = []
    for license_path, extra_dir in zip(args.extra_license, args.extra_licenses_dir):
        extras.append(license_path.read_text(encoding="utf-8").rstrip() + "\n")
        copy_tree(extra_dir, dest_licenses / extra_dir.name)
    (args.out / "LICENSE").write_text(filtered.rstrip() + "\n\n" + "\n".join(extras), encoding="utf-8")
    components = load_obligations(args.obligations)
    legal_root = args.legal_root
    if legal_root is None and args.obligations is not None:
        legal_root = args.obligations.parent
    if args.notice is not None:
        if legal_root is None:
            raise SystemExit("--legal-root or --obligations is required when assembling NOTICE")
        notice_text = assemble_notice(args.notice.read_text(encoding="utf-8"), legal_root, components, inventory)
        (args.out / "NOTICE").write_text(notice_text, encoding="utf-8")
    if args.readme is not None:
        disclosures = render_disclosures(components, inventory, license_expressions)
        write_readme(args.readme.read_text(encoding="utf-8"), disclosures, args.out / "README.md")
    write_inventory(args.inventory_out, inventory)
    print(f"wrote {args.out / 'LICENSE'} with {len(kept)} Go modules", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
