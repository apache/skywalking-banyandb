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

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_PKG_SCRIPT = Path(__file__).with_name("package-licenses.py")
_COMP_SCRIPT = Path(__file__).with_name("license-compliance.py")


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


_PKG = _load("package_licenses", _PKG_SCRIPT)
_COMP = _load("license_compliance", _COMP_SCRIPT)

filter_license = _PKG.filter_license
license_filename = _PKG.license_filename
modules_from_version_m = _PKG.modules_from_version_m
split_go_license = _PKG.split_go_license
ModuleRef = _PKG.ModuleRef
Inventory = _PKG.Inventory
assemble_notice = _PKG.assemble_notice
render_disclosures = _PKG.render_disclosures
write_readme = _PKG.write_readme
copy_license_texts = _PKG.copy_license_texts

input_fingerprint = _COMP.input_fingerprint
validate_package_dir = _COMP.validate_package_dir

SAMPLE = """Apache License
Version 2.0

========================================================================
Apache-2.0 licenses
========================================================================

    cloud.google.com/go/storage v1.64.0 Apache-2.0
    github.com/spf13/cobra v1.8.1 Apache-2.0
    github.com/prometheus/common v0.70.1 Apache-2.0

========================================================================
MIT licenses
========================================================================

    github.com/mattn/go-isatty v0.0.20 MIT

========================================================================
MPL-2.0 licenses
========================================================================

    github.com/hashicorp/golang-lru v1.0.2 MPL-2.0
    github.com/shoenig/go-m1cpu v0.2.2 MPL-2.0

========================================================================
MIT and Apache-2.0 licenses
========================================================================

    gopkg.in/yaml.v3 v3.0.1 MIT and Apache-2.0

========================================================================
UI related licenses
========================================================================

    vue 3.5.41 MIT
"""


class PackageLicenseTest(unittest.TestCase):
    def test_license_filename_matches_license_eye(self):
        self.assertEqual(license_filename("github.com/spf13/cobra"), "license-github.com-spf13-cobra.txt")
        self.assertEqual(
            license_filename("@modelcontextprotocol/sdk"),
            "license--modelcontextprotocol-sdk.txt",
        )
        self.assertEqual(license_filename("gopkg.in/yaml.v3"), "license-gopkg.in-yaml.v3.txt")

    def test_split_drops_ui_section(self):
        text = split_go_license(SAMPLE)
        self.assertIn("github.com/spf13/cobra", text)
        self.assertNotIn("vue 3.5.41", text)

    def test_filter_keeps_only_linked_modules(self):
        filtered, kept, versions, expressions = filter_license(
            SAMPLE, {"github.com/spf13/cobra", "github.com/mattn/go-isatty", "gopkg.in/yaml.v3"}
        )
        self.assertEqual(kept, {"github.com/spf13/cobra", "github.com/mattn/go-isatty", "gopkg.in/yaml.v3"})
        self.assertEqual(versions["github.com/spf13/cobra"], "v1.8.1")
        self.assertEqual(expressions["github.com/spf13/cobra"], "Apache-2.0")
        self.assertIn("github.com/spf13/cobra", filtered)
        self.assertIn("github.com/mattn/go-isatty", filtered)
        self.assertIn("gopkg.in/yaml.v3 v3.0.1 MIT and Apache-2.0", filtered)
        self.assertNotIn("cloud.google.com/go/storage", filtered)
        self.assertNotIn("vue 3.5.41", filtered)
        self.assertIn("Apache-2.0 licenses", filtered)
        self.assertIn("MIT licenses", filtered)

    def test_replace_uses_replacement_module_path_and_version(self):
        text = (
            "\tdep\tgithub.com/blugelabs/bluge\tv0.2.2\th1:abc=\n"
            "\t=>\tgithub.com/SkyAPM/bluge\tv0.0.0-20260625022800-42385daf66b8\th1:def=\n"
            "\tdep\tgithub.com/blugelabs/bluge_segment_api\tv0.2.0\th1:abc=\n"
            "\t=>\tgithub.com/zinclabs/bluge_segment_api\tv1.0.0\th1:def=\n"
            "\tdep\tgithub.com/spf13/cobra\tv1.10.2\th1:abc=\n"
        )
        modules = modules_from_version_m(text)
        self.assertEqual(
            set(modules),
            {
                "github.com/SkyAPM/bluge",
                "github.com/zinclabs/bluge_segment_api",
                "github.com/spf13/cobra",
            },
        )
        self.assertEqual(modules["github.com/SkyAPM/bluge"].version, "v0.0.0-20260625022800-42385daf66b8")
        self.assertNotIn("github.com/blugelabs/bluge", modules)

    def test_prometheus_notice_included_when_bundled(self):
        obligations = json.loads((_ROOT / "dist/legal/obligations.json").read_text(encoding="utf-8"))
        inventory = Inventory(
            modules={"github.com/prometheus/common": ModuleRef("github.com/prometheus/common", "v0.70.1")}
        )
        notice = assemble_notice(
            "Apache SkyWalking\n",
            _ROOT / "dist/legal",
            obligations["components"],
            inventory,
        )
        self.assertIn("Prometheus Authors", notice)
        self.assertIn("SoundCloud", notice)

    def test_prometheus_notice_excluded_when_absent(self):
        obligations = json.loads((_ROOT / "dist/legal/obligations.json").read_text(encoding="utf-8"))
        inventory = Inventory(modules={"github.com/spf13/cobra": ModuleRef("github.com/spf13/cobra", "v1.8.1")})
        notice = assemble_notice("Apache SkyWalking\n", _ROOT / "dist/legal", obligations["components"], inventory)
        self.assertNotIn("Prometheus Authors", notice)

    def test_mpl_disclosure_when_bundled(self):
        obligations = json.loads((_ROOT / "dist/legal/obligations.json").read_text(encoding="utf-8"))
        inventory = Inventory(
            modules={"github.com/hashicorp/golang-lru": ModuleRef("github.com/hashicorp/golang-lru", "v1.0.2")}
        )
        text = render_disclosures(
            obligations["components"],
            inventory,
            {"github.com/hashicorp/golang-lru": "MPL-2.0"},
        )
        self.assertIn("Bundled third-party components", text)
        self.assertIn("golang-lru", text)
        self.assertIn("MPL-2.0", text)
        self.assertIn("https://github.com/hashicorp/golang-lru", text)

    def test_missing_license_text_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "licenses"
            with self.assertRaises(SystemExit):
                copy_license_texts(Path(tmp) / "missing", dest, {"github.com/spf13/cobra"})

    def test_readme_inserts_before_license_section(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "README.md"
            write_readme("# Title\n\n## License\n\nApache\n", "## Bundled third-party components\n\nx\n", dest)
            text = dest.read_text(encoding="utf-8")
            self.assertLess(text.index("Bundled third-party"), text.index("## License"))


class LicenseComplianceTest(unittest.TestCase):
    def test_obligation_fingerprints_match_checked_in_metadata(self):
        data = json.loads((_ROOT / "dist/legal/obligations.json").read_text(encoding="utf-8"))
        legal_root = _ROOT / "dist/legal"
        for component in data["components"]:
            self.assertEqual(
                component["review"]["input_fingerprint"],
                input_fingerprint(component, legal_root),
                component["name"],
            )

    def test_validate_package_requires_notice_attribution(self):
        with tempfile.TemporaryDirectory() as tmp:
            pkg = Path(tmp)
            (pkg / "LICENSE").write_text(
                "========================================================================\n"
                "Apache-2.0 licenses\n"
                "========================================================================\n\n"
                "    github.com/prometheus/common v0.70.1 Apache-2.0\n",
                encoding="utf-8",
            )
            (pkg / "NOTICE").write_text("Apache SkyWalking\n", encoding="utf-8")
            (pkg / "README.md").write_text("# x\n", encoding="utf-8")
            (pkg / "licenses").mkdir()
            (pkg / "licenses" / "license-github.com-prometheus-common.txt").write_text("Apache\n", encoding="utf-8")
            errors = validate_package_dir(pkg, _ROOT / "dist/legal/obligations.json", _ROOT / "dist/legal")
            self.assertTrue(any("NOTICE missing attribution" in err for err in errors))


if __name__ == "__main__":
    unittest.main()
