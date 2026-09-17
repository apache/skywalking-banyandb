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
import unittest
from pathlib import Path

_SCRIPT = Path(__file__).with_name("package-licenses.py")
_SPEC = importlib.util.spec_from_file_location("package_licenses", _SCRIPT)
_MOD = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MOD)
filter_license = _MOD.filter_license
license_filename = _MOD.license_filename
split_go_license = _MOD.split_go_license


SAMPLE = """Apache License
Version 2.0

========================================================================
Apache-2.0 licenses
========================================================================

    cloud.google.com/go/storage v1.64.0 Apache-2.0
    github.com/spf13/cobra v1.8.1 Apache-2.0

========================================================================
MIT licenses
========================================================================

    github.com/mattn/go-isatty v0.0.20 MIT

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
        filtered, kept = filter_license(SAMPLE, {"github.com/spf13/cobra", "github.com/mattn/go-isatty", "gopkg.in/yaml.v3"})
        self.assertEqual(kept, {"github.com/spf13/cobra", "github.com/mattn/go-isatty", "gopkg.in/yaml.v3"})
        self.assertIn("github.com/spf13/cobra", filtered)
        self.assertIn("github.com/mattn/go-isatty", filtered)
        self.assertIn("gopkg.in/yaml.v3 v3.0.1 MIT and Apache-2.0", filtered)
        self.assertNotIn("cloud.google.com/go/storage", filtered)
        self.assertNotIn("vue 3.5.41", filtered)
        self.assertIn("Apache-2.0 licenses", filtered)
        self.assertIn("MIT licenses", filtered)


if __name__ == "__main__":
    unittest.main()
