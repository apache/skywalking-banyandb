// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package transfer

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

// dryRunPlanYAML is the dry-run subset of the design §2.2 example, which strict parsing
// must accept.
const dryRunPlanYAML = `
connection:
  nodes: [liaison-1:17912, liaison-2:17912]
  nodesTLS:  {enable: true, insecure: true, cert: "/c.pem"}
export:
  parallelism: max
  selectors:
    - catalog: stream
      groups: [sw_record, sw_log]
    - catalog: measure
      groups: [sw_metric]
    - catalog: property
`

func TestLoadPlanFile_AcceptsTheDryRunFields(t *testing.T) {
	p := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(p, []byte(dryRunPlanYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	plan, err := LoadPlanFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Connection.Nodes) != 2 || !plan.Connection.NodesTLS.Enable || !plan.Connection.NodesTLS.Insecure || plan.Connection.NodesTLS.Cert != "/c.pem" {
		t.Fatalf("connection = %+v", plan.Connection)
	}
	if plan.Export.Parallelism.String() != "max" {
		t.Fatalf("export = %+v", plan.Export)
	}
	if len(plan.Export.Selectors) != 3 || plan.Export.Selectors[0].Groups[1] != "sw_log" || plan.Export.Selectors[2].Catalog != "property" {
		t.Fatalf("selectors = %+v", plan.Export.Selectors)
	}
}

func TestLoadPlanFile_RejectsUnknownFields(t *testing.T) {
	// A misspelled field, and the design fields that only a real export or an import reads
	// (they land with those steps), are all unknown to this step's strict parser.
	for _, body := range []string{
		"connection:\n  nodes: [a:17912]\nexport:\n  outpt: ./x\n",
		"connection:\n  nodes: [a:17912]\nexport:\n  output: ./x\n",
		"connection:\n  nodes: [a:17912]\nexport:\n  format: csv\n",
		"connection:\n  nodes: [a:17912]\n  rateLimit: 0\n",
		"connection:\n  nodes: [a:17912]\nimport:\n  input: ./x\n",
	} {
		p := filepath.Join(t.TempDir(), "plan.yaml")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPlanFile(p); err == nil {
			t.Fatalf("an unknown field must be rejected:\n%s", body)
		}
	}
	if _, err := LoadPlanFile(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("a missing file must be reported")
	}
}

func TestParallelismUnmarshal(t *testing.T) {
	for raw, want := range map[string]string{"parallelism: max": "max", "parallelism: \"max\"": "max", "parallelism: 7": "7", "parallelism: \"7\"": "7"} {
		var cfg ExportConfig
		if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil || cfg.Parallelism.String() != want {
			t.Fatalf("%s -> %q, %v; want %q", raw, cfg.Parallelism, err, want)
		}
	}
	var cfg ExportConfig
	if err := yaml.Unmarshal([]byte("parallelism: true"), &cfg); err == nil {
		t.Fatal("a boolean parallelism must be rejected")
	}
}

func TestParseParallelism(t *testing.T) {
	if n, err := ParseParallelism("max"); err != nil || n != math.MaxInt {
		t.Fatalf("max -> %d, %v", n, err)
	}
	if n, err := ParseParallelism("4"); err != nil || n != 4 {
		t.Fatalf("4 -> %d, %v", n, err)
	}
	for _, bad := range []string{"0", "-1", "", "fast", "1.5"} {
		if _, err := ParseParallelism(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
	if got, clamped := ClampParallelism(10, 5); got != 5 || !clamped {
		t.Fatalf("clamp(10,5) = %d,%v", got, clamped)
	}
	if got, clamped := ClampParallelism(3, 5); got != 3 || clamped {
		t.Fatalf("clamp(3,5) = %d,%v", got, clamped)
	}
	if got, _ := ClampParallelism(math.MaxInt, 0); got != 1 {
		t.Fatalf("clamp with no nodes = %d, want 1", got)
	}
}

func TestParseSelector(t *testing.T) {
	s, err := ParseSelector("catalog=stream,groups=g1;g2")
	if err != nil || s.Catalog != "stream" || len(s.Groups) != 2 || s.Groups[1] != "g2" {
		t.Fatalf("%+v, %v", s, err)
	}
	s, err = ParseSelector("catalog=measure")
	if err != nil || s.Catalog != "measure" || len(s.Groups) != 0 {
		t.Fatalf("%+v, %v", s, err)
	}
	for _, bad := range []string{"", "groups=g1", "catalog=", "catalog=stream,foo=bar", "catalog=stream,groups="} {
		if _, badErr := ParseSelector(bad); badErr == nil {
			t.Fatalf("%q must be rejected as syntax", bad)
		}
	}
	for _, bad := range []string{"catalog=logs", "catalog=stream,groups=a;;b"} {
		cfg, parseErr := ParseSelector(bad)
		if parseErr != nil {
			t.Fatalf("%q is valid syntax: %v", bad, parseErr)
		}
		if _, protoErr := cfg.ToProto(); protoErr == nil {
			t.Fatalf("%q must be rejected by ToProto", bad)
		}
	}
	proto, err := (&SelectorConfig{Catalog: "trace", Groups: []string{"t"}}).ToProto()
	if err != nil || proto.Catalog != commonv1.Catalog_CATALOG_TRACE || proto.Groups[0] != "t" {
		t.Fatalf("%+v, %v", proto, err)
	}
	if _, err := (&SelectorConfig{Catalog: "nope"}).ToProto(); err == nil {
		t.Fatal("unknown catalog in plan.yaml must be rejected")
	}
	if CatalogName(commonv1.Catalog_CATALOG_PROPERTY) != "property" {
		t.Fatal("CatalogName must invert the selector names")
	}
}
