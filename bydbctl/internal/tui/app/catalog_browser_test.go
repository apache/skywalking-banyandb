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

package app

import (
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestCatalogBrowserTracksLoadedResources(t *testing.T) {
	browser := newCatalogBrowser()
	browser.setLoading()
	if !browser.loading {
		t.Fatal("expected the browser to report a load in progress")
	}
	browser.setCatalog(session.SchemaCatalog{
		Groups: []string{"sw_metrics", "default"},
		Entries: []session.CatalogEntry{
			{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_endpoint_latency"},
			{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"},
			{Group: "default", Type: session.ResourceTypeStream, Name: "access_log"},
		},
	})
	if browser.loading {
		t.Fatal("a loaded catalog must clear the loading flag")
	}
	if browser.resourceCount() != 3 {
		t.Fatalf("expected three resources, got %d", browser.resourceCount())
	}
	if browser.groupCount() != 2 {
		t.Fatalf("expected two groups, got %d", browser.groupCount())
	}
}

func TestCatalogBrowserLoadErrorClearsStaleEntries(t *testing.T) {
	browser := newCatalogBrowser()
	browser.setCatalog(session.SchemaCatalog{
		Groups:  []string{"sw_metrics"},
		Entries: []session.CatalogEntry{{Group: "sw_metrics", Type: session.ResourceTypeMeasure, Name: "service_cpm"}},
	})
	browser.setLoadError("connection refused")
	if browser.loadError != "connection refused" {
		t.Fatalf("unexpected load error: %q", browser.loadError)
	}
	if browser.resourceCount() != 0 {
		t.Fatalf("a failed load must not leave stale resources, got %d", browser.resourceCount())
	}
}
