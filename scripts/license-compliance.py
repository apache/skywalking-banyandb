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
"""License-compliance helpers shared by local Make targets and CI.

Subcommands:
  fingerprint   Print the reviewed-input fingerprint for an obligation.
  review-check  Validate obligations.json against local fragments and catalog.
  drift-check   Fail when generated licensing files differ from git.
  validate-dir  Validate a staged package directory for LICENSE/NOTICE/disclosures.
  validate-tgz  Validate a release archive tarball.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

DEP_LINE = re.compile(r"^    (\S+) (\S+) (.+?)\s*$")
SECTION_START = re.compile(r"^={8,}\s*$")
UI_OR_MCP_MARKER = re.compile(r"(?i)(UI related licenses|mcp related licenses)")

DRIFT_PATHS = (
    "dist/LICENSE",
    "dist/licenses",
    "ui/LICENSE",
    "mcp/LICENSE",
    "dist/legal/catalog",
)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def input_fingerprint(component: dict, legal_root: Path) -> str:
    """Compute the reviewed-input fingerprint for an obligation component."""
    ecosystem = component["ecosystem"].encode()
    name = component["name"].encode()
    version = component["version"].encode()
    notice = component.get("notice") or {}
    decision = notice.get("decision")
    disclosure = component.get("disclosure") or {}
    parts: list[bytes] = [ecosystem, name, version]
    if decision == "include":
        upstream = component.get("upstream_legal_files") or []
        if not upstream:
            raise SystemExit(f"{component['name']}: include notice requires upstream_legal_files")
        for item in upstream:
            parts.append(item["kind"].encode())
            parts.append(bytes.fromhex(item["sha256"]))
        fragment = notice.get("fragment")
        if not fragment:
            raise SystemExit(f"{component['name']}: include notice requires fragment")
        frag_path = legal_root / fragment
        parts.append(b"include")
        parts.append(hashlib.sha256(frag_path.read_bytes()).digest())
    elif decision == "none":
        parts.append(b"none")
        if disclosure.get("required"):
            parts.append(b"disclosure")
        else:
            parts.append(b"no-disclosure")
    else:
        raise SystemExit(f"{component['name']}: unknown or missing notice.decision")
    return sha256_bytes(b"|".join(parts))


def load_obligations(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise SystemExit(f"unsupported obligations schema in {path}")
    return data


def load_catalog(path: Path | None) -> dict[tuple[str, str], str]:
    """Return (name, version) -> license_expression from a generated catalog."""
    if path is None or not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    out: dict[tuple[str, str], str] = {}
    for dep in data.get("dependencies", []):
        out[(dep["name"], dep["version"])] = dep["license_expression"]
    return out


def parse_license_versions(license_text: str) -> dict[str, str]:
    versions: dict[str, str] = {}
    for line in license_text.splitlines():
        if UI_OR_MCP_MARKER.search(line):
            break
        match = DEP_LINE.match(line)
        if match:
            versions[match.group(1)] = match.group(2)
    return versions


def cmd_fingerprint(args: argparse.Namespace) -> int:
    data = load_obligations(args.obligations)
    legal_root = args.legal_root or args.obligations.parent
    for component in data["components"]:
        if args.name and component["name"] != args.name:
            continue
        print(f"{component['name']}@{component['version']} {input_fingerprint(component, legal_root)}")
    return 0


def cmd_review_check(args: argparse.Namespace) -> int:
    data = load_obligations(args.obligations)
    legal_root = args.legal_root or args.obligations.parent
    catalog = load_catalog(args.catalog)
    license_versions = parse_license_versions(args.license.read_text(encoding="utf-8")) if args.license else {}
    errors: list[str] = []
    for component in data["components"]:
        name = component["name"]
        version = component["version"]
        review = component.get("review") or {}
        if review.get("status") != "approved":
            errors.append(f"{name}@{version}: review.status is not approved")
            continue
        try:
            expected = input_fingerprint(component, legal_root)
        except SystemExit as exc:
            errors.append(str(exc))
            continue
        actual = review.get("input_fingerprint")
        if actual != expected:
            errors.append(f"{name}@{version}: input_fingerprint mismatch (have {actual}, want {expected})")
        notice = component.get("notice") or {}
        if notice.get("decision") == "include":
            fragment = notice.get("fragment")
            frag_path = legal_root / fragment
            if not frag_path.is_file():
                errors.append(f"{name}@{version}: missing fragment {frag_path}")
            for item in component.get("upstream_legal_files") or []:
                if notice.get("decision") == "include" and frag_path.is_file():
                    # Local fragment must match the reviewed upstream hash when it is the NOTICE body.
                    if item.get("kind") == "NOTICE" and sha256_file(frag_path) != item["sha256"]:
                        errors.append(
                            f"{name}@{version}: fragment sha256 {sha256_file(frag_path)} "
                            f"!= upstream {item['sha256']}"
                        )
        if catalog and (name, version) not in catalog:
            # Catalog may be incomplete during bootstrapping; treat as error when provided.
            errors.append(f"{name}@{version}: not present in catalog {args.catalog}")
        if license_versions and license_versions.get(name) not in (None, version):
            errors.append(
                f"{name}@{version}: dist/LICENSE lists version {license_versions[name]}"
            )
        disclosure = component.get("disclosure") or {}
        if disclosure.get("required"):
            for key in ("homepage", "source_url"):
                if not disclosure.get(key):
                    errors.append(f"{name}@{version}: disclosure missing {key}")
    if errors:
        print("license review-check failed:", file=sys.stderr)
        for err in errors:
            print(f"  {err}", file=sys.stderr)
        return 1
    print(f"license review-check ok ({len(data['components'])} components)")
    return 0


def cmd_drift_check(args: argparse.Namespace) -> int:
    repo = args.repo
    report_dir = args.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)
    patch = report_dir / "generated.patch"
    result = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all", "--", *DRIFT_PATHS],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        return result.returncode
    dirty = [line for line in result.stdout.splitlines() if line.strip()]
    diff = subprocess.run(
        ["git", "diff", "--", *DRIFT_PATHS],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    patch.write_text(diff.stdout, encoding="utf-8")
    report = {
        "schema_version": 1,
        "dirty": dirty,
        "paths": list(DRIFT_PATHS),
    }
    (report_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (report_dir / "report.md").write_text(
        "# License drift report\n\n"
        + ("No drift detected.\n" if not dirty else "Drift detected:\n\n" + "\n".join(f"- `{line}`" for line in dirty) + "\n"),
        encoding="utf-8",
    )
    if dirty:
        print("license drift-check failed; generated licensing files differ from git:", file=sys.stderr)
        for line in dirty:
            print(f"  {line}", file=sys.stderr)
        print(f"see {patch}", file=sys.stderr)
        return 1
    print("license drift-check ok")
    return 0


def _require_file(path: Path, label: str, errors: list[str]) -> str | None:
    if not path.is_file() or path.stat().st_size == 0:
        errors.append(f"missing or empty {label}: {path}")
        return None
    return path.read_text(encoding="utf-8")


LICENSE_FILE_UNSAFE = re.compile(r"[^a-zA-Z0-9\\.\-]")


def license_filename(dep_name: str) -> str:
    return "license-" + LICENSE_FILE_UNSAFE.sub("-", dep_name) + ".txt"


def validate_package_dir(
    pkg_dir: Path,
    obligations: Path | None,
    legal_root: Path | None,
    *,
    source_layout: bool = False,
) -> list[str]:
    """Validate a staged package directory. Returns error messages."""
    errors: list[str] = []
    license_text = _require_file(pkg_dir / "LICENSE", "LICENSE", errors)
    notice_text = _require_file(pkg_dir / "NOTICE", "NOTICE", errors)
    if source_layout:
        # Source archives ship project LICENSE/NOTICE and keep dependency texts under dist/.
        if not (pkg_dir / "dist" / "licenses").is_dir() and not (pkg_dir / "dist" / "LICENSE").is_file():
            errors.append("source archive missing dist/LICENSE or dist/licenses/")
        return errors
    readme_text = _require_file(pkg_dir / "README.md", "README.md", errors)
    licenses_dir = pkg_dir / "licenses"
    if not licenses_dir.is_dir():
        errors.append(f"missing licenses directory: {licenses_dir}")
    if obligations is None or license_text is None:
        return errors
    data = load_obligations(obligations)
    root = legal_root or obligations.parent
    license_versions = parse_license_versions(license_text)
    for component in data["components"]:
        name = component["name"]
        version = component["version"]
        if license_versions.get(name) != version:
            continue  # not bundled in this package
        notice = component.get("notice") or {}
        if notice.get("decision") == "include":
            fragment = (root / notice["fragment"]).read_text(encoding="utf-8").strip()
            if notice_text is not None and fragment not in notice_text:
                errors.append(f"NOTICE missing attribution for {name}@{version}")
        disclosure = component.get("disclosure") or {}
        if disclosure.get("required"):
            if readme_text is None or "Bundled third-party components" not in readme_text:
                errors.append(f"README missing bundled third-party disclosures for {name}")
            elif name not in readme_text and name.rsplit("/", 1)[-1] not in readme_text:
                errors.append(f"README missing disclosure for {name}@{version}")
            elif disclosure.get("homepage") and disclosure["homepage"] not in (readme_text or ""):
                errors.append(f"README missing homepage for {name}")
        lic_file = licenses_dir / license_filename(name)
        if not lic_file.is_file() or lic_file.stat().st_size == 0:
            errors.append(f"missing license text for bundled {name}: {lic_file.name}")
    return errors


def cmd_validate_dir(args: argparse.Namespace) -> int:
    errors = validate_package_dir(
        args.package,
        args.obligations,
        args.legal_root,
        source_layout=args.source,
    )
    if errors:
        print("package validation failed:", file=sys.stderr)
        for err in errors:
            print(f"  {err}", file=sys.stderr)
        return 1
    print(f"package validation ok: {args.package}")
    return 0


def cmd_validate_tgz(args: argparse.Namespace) -> int:
    with tempfile.TemporaryDirectory(prefix="license-validate-") as tmp:
        tmp_path = Path(tmp)
        with tarfile.open(args.tarball, "r:gz") as archive:
            for member in archive.getmembers():
                name = member.name
                if name.startswith("/") or ".." in Path(name).parts:
                    print(f"unsafe archive member: {name}", file=sys.stderr)
                    return 1
            # Python 3.12+ supports filter=; require data extraction semantics when available.
            if "filter" in tarfile.TarFile.extractall.__code__.co_varnames:
                archive.extractall(tmp_path, filter="data")
            else:
                archive.extractall(tmp_path)
        children = [child for child in tmp_path.iterdir() if not child.name.startswith(".")]
        pkg_dir = children[0] if len(children) == 1 and children[0].is_dir() else tmp_path
        errors = validate_package_dir(
            pkg_dir,
            args.obligations,
            args.legal_root,
            source_layout=args.source,
        )
        if args.require_bins:
            bin_dir = pkg_dir / "bin"
            if not bin_dir.is_dir() or not any(bin_dir.iterdir()):
                errors.append("binary archive missing bin/")
        if errors:
            print(f"archive validation failed for {args.tarball}:", file=sys.stderr)
            for err in errors:
                print(f"  {err}", file=sys.stderr)
            return 1
    print(f"archive validation ok: {args.tarball}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    fingerprint = sub.add_parser("fingerprint", help="print obligation input fingerprints")
    fingerprint.add_argument("--obligations", type=Path, required=True)
    fingerprint.add_argument("--legal-root", type=Path)
    fingerprint.add_argument("--name")
    fingerprint.set_defaults(func=cmd_fingerprint)

    review = sub.add_parser("review-check", help="validate reviewed obligations")
    review.add_argument("--obligations", type=Path, required=True)
    review.add_argument("--legal-root", type=Path)
    review.add_argument("--catalog", type=Path)
    review.add_argument("--license", type=Path, help="dist/LICENSE for version cross-check")
    review.set_defaults(func=cmd_review_check)

    drift = sub.add_parser("drift-check", help="detect generated licensing drift")
    drift.add_argument("--repo", type=Path, default=Path.cwd())
    drift.add_argument("--report-dir", type=Path, default=Path("build/license-compliance"))
    drift.set_defaults(func=cmd_drift_check)

    validate_dir = sub.add_parser("validate-dir", help="validate staged package directory")
    validate_dir.add_argument("--package", type=Path, required=True)
    validate_dir.add_argument("--obligations", type=Path)
    validate_dir.add_argument("--legal-root", type=Path)
    validate_dir.add_argument("--source", action="store_true", help="source-archive layout checks")
    validate_dir.set_defaults(func=cmd_validate_dir)

    validate_tgz = sub.add_parser("validate-tgz", help="validate a release tarball")
    validate_tgz.add_argument("--tarball", type=Path, required=True)
    validate_tgz.add_argument("--obligations", type=Path)
    validate_tgz.add_argument("--legal-root", type=Path)
    validate_tgz.add_argument("--require-bins", action="store_true")
    validate_tgz.add_argument("--source", action="store_true", help="source-archive layout checks")
    validate_tgz.set_defaults(func=cmd_validate_tgz)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
