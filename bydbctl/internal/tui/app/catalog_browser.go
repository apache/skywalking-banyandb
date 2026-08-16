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
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// catalogBrowser holds the discovered BanyanDB catalog and its load state.
//
// Browsing and filtering live in the composer's @ search, which scores groups, names, and types,
// so this type only owns the data that search and the cold-start guidance read.
type catalogBrowser struct {
	loadError string
	catalog   session.SchemaCatalog
	loading   bool
}

func newCatalogBrowser() catalogBrowser {
	return catalogBrowser{}
}

func (browser *catalogBrowser) setCatalog(catalog session.SchemaCatalog) {
	browser.catalog = catalog
	browser.loading = false
	browser.loadError = ""
}

func (browser *catalogBrowser) setLoadError(loadError string) {
	browser.loading = false
	browser.loadError = loadError
	browser.catalog = session.SchemaCatalog{}
}

func (browser *catalogBrowser) setLoading() {
	browser.loading = true
	browser.loadError = ""
}

// resourceCount reports how many resources the catalog holds.
func (browser catalogBrowser) resourceCount() int {
	return len(browser.catalog.Entries)
}

// groupCount reports how many groups the catalog spans.
func (browser catalogBrowser) groupCount() int {
	return len(browser.catalog.Groups)
}
