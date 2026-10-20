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
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

func TestCatalogNames(t *testing.T) {
	for name, want := range catalogByName {
		got, ok := CatalogByName(name)
		if !ok || got != want || CatalogName(got) != name {
			t.Fatalf("%s -> %v, %v; back to %q", name, got, ok, CatalogName(got))
		}
	}
	if _, ok := CatalogByName("logs"); ok {
		t.Fatal("an unknown catalog name must not resolve")
	}
	if CatalogName(commonv1.Catalog_CATALOG_UNSPECIFIED) != "CATALOG_UNSPECIFIED" {
		t.Fatal("an unnamed catalog renders as its enum name")
	}
}
