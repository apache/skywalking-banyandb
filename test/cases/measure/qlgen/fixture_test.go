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

package qlgen

import (
	"strings"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func TestFixtureContracts(t *testing.T) {
	base := time.Unix(1700000000, 0)
	writes := FixtureWrites(base)
	if len(writes) != 8 || FixtureSchema().GetMetadata().GetName() != FixtureName {
		t.Fatal("fixture schema and row count differ from the oracle contract")
	}
	entity := FixtureSchema().GetEntity().GetTagNames()
	if len(entity) != 1 || entity[0] != "rid" {
		t.Fatal("indexed fixture tags vary by row and therefore require a unique row series")
	}
	for index, write := range writes {
		if !write.GetDataPoint().GetTimestamp().AsTime().Equal(base.Add(time.Duration(index) * time.Second)) {
			t.Fatalf("row %d has wrong timestamp", index)
		}
		if len(write.GetDataPoint().GetTagFamilies()[0].GetTags()) != 5 || len(write.GetDataPoint().GetFields()) != 2 {
			t.Fatalf("row %d shape disagrees with schema", index)
		}
	}
	firstDigest := FixtureDigest()
	if len(firstDigest) != 64 || firstDigest != FixtureDigest() {
		t.Fatal("fixture digest must be stable SHA256")
	}
	writes[0].Metadata.Name = "mutated"
	if FixtureWrites(base)[0].GetMetadata().GetName() != FixtureName {
		t.Fatal("fixture callers share mutable state")
	}
}

func TestFixtureNullableArraysStayExplicitlyNull(t *testing.T) {
	row := FixtureWrites(time.Unix(1700000000, 0))[4].GetDataPoint().GetTagFamilies()[0]
	if row.GetTags()[3].GetStrArray() != nil || row.GetTags()[4].GetIntArray() != nil {
		t.Fatal("nullable array fixture must write explicit null values, not empty arrays")
	}
	generated, err := BuildCase("null-arrays", Query{Columns: []string{"labels", "nums"}, BeginMillis: 4000, EndMillis: 4000})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(generated.WantJSON), `"null":0`) != 2 {
		t.Fatal(string(generated.WantJSON))
	}
}

func TestFixtureIndexesCoverGeneratedPredicates(t *testing.T) {
	rules := FixtureIndexRules()
	binding := FixtureIndexBinding()
	if len(rules) != 5 || len(binding.GetRules()) != 5 {
		t.Fatal("all five filterable fixture tags need indexes")
	}
	for _, rule := range rules {
		if err := rule.ValidateAll(); err != nil {
			t.Fatal(err)
		}
		if len(rule.GetTags()) != 1 || rule.GetType() != databasev1.IndexRule_TYPE_INVERTED {
			t.Fatal(rule)
		}
	}
	if err := binding.ValidateAll(); err != nil {
		t.Fatal(err)
	}
	if binding.GetSubject().GetName() != FixtureName || binding.GetSubject().GetCatalog() != commonv1.Catalog_CATALOG_MEASURE {
		t.Fatal(binding)
	}
	rules[0].Metadata.Name = "mutated"
	if FixtureIndexRules()[0].GetMetadata().GetName() == "mutated" {
		t.Fatal("mutable registry fixture is shared")
	}
}
