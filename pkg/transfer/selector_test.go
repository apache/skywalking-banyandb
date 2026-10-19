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
	"strings"
	"testing"
)

func TestParseSelector_GroupNameContainsEquals(t *testing.T) {
	// A group name containing '=' cannot be expressed in the selector syntax; the plan file is required.
	_, err := ParseSelector("catalog=stream,groups=g=1")
	if err == nil {
		t.Fatal("expected error for group name containing '='")
	}
	if !strings.Contains(err.Error(), "plan file") {
		t.Fatalf("error message must point at the plan file: %v", err)
	}
}

func TestParseSelector_DuplicateGroup(t *testing.T) {
	cfg, err := ParseSelector("catalog=stream,groups=g1;g1")
	if err != nil {
		t.Fatalf("a duplicate group is valid syntax: %v", err)
	}
	if _, err = cfg.ToProto(); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("ToProto must reject the group given twice: %v", err)
	}
}

func TestParseSelector_HappyPath(t *testing.T) {
	cfg, err := ParseSelector("catalog=stream,groups=a;b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Catalog != "stream" {
		t.Fatalf("catalog = %q, want stream", cfg.Catalog)
	}
	if len(cfg.Groups) != 2 || cfg.Groups[0] != "a" || cfg.Groups[1] != "b" {
		t.Fatalf("groups = %v, want [a b]", cfg.Groups)
	}
}

func TestParseSelector_CatalogOnly(t *testing.T) {
	cfg, err := ParseSelector("catalog=measure")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Catalog != "measure" || len(cfg.Groups) != 0 {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestParseSelector_UnknownCatalog(t *testing.T) {
	cfg, err := ParseSelector("catalog=nosuchcatalog")
	if err != nil {
		t.Fatalf("an unknown catalog is valid syntax: %v", err)
	}
	if _, err = cfg.ToProto(); err == nil || !strings.Contains(err.Error(), "unknown catalog") {
		t.Fatalf("ToProto must reject the unknown catalog: %v", err)
	}
}

func TestParseSelector_MissingCatalog(t *testing.T) {
	_, err := ParseSelector("groups=a;b")
	if err == nil {
		t.Fatal("expected error when catalog is omitted")
	}
	if !strings.Contains(err.Error(), "catalog") {
		t.Fatalf("error must mention 'catalog': %v", err)
	}
}

func TestParseSelector_EmptyInput(t *testing.T) {
	_, err := ParseSelector("")
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestParseSelector_DuplicateCatalog(t *testing.T) {
	_, err := ParseSelector("catalog=stream,catalog=measure")
	if err == nil || !strings.Contains(err.Error(), "catalog given twice") {
		t.Fatalf("a repeated catalog key must be rejected, got %v", err)
	}
}

func TestParseSelector_DuplicateGroupsKey(t *testing.T) {
	_, err := ParseSelector("catalog=stream,groups=a,groups=b")
	if err == nil || !strings.Contains(err.Error(), "groups given twice") {
		t.Fatalf("expected 'groups given twice' error, got %v", err)
	}
}

func TestSelectorConfigToProto_RejectsEmptyAndDuplicateGroups(t *testing.T) {
	for _, groups := range [][]string{{""}, {"a", "a"}} {
		s := SelectorConfig{Catalog: "stream", Groups: groups}
		if _, err := s.ToProto(); err == nil {
			t.Fatalf("groups %q must be rejected", groups)
		}
	}
	s := SelectorConfig{Catalog: "stream", Groups: []string{"a", "b"}}
	if p, err := s.ToProto(); err != nil || len(p.GetGroups()) != 2 {
		t.Fatalf("distinct groups are accepted: %v %v", p, err)
	}
}
