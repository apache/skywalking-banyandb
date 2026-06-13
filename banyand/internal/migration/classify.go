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

package migration

import (
	"fmt"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/reader"
)

// Classified is the outcome of ClassifyGroups: each plan group bucketed by
// its catalog, plus the Group protos (catalog + ResourceOpts) per group.
type Classified struct {
	// Buckets holds the plan's groups keyed by catalog, preserving the
	// plan's group order inside each bucket. Only catalogs with a
	// registered executor ever appear.
	Buckets map[commonv1.Catalog][]string
	// Groups carries the schema-property Group proto per group name.
	Groups map[string]*commonv1.Group
	// SchemaRoot is the resolved `_schema` bluge directory.
	SchemaRoot string
}

// ClassifyGroups resolves the catalog of every plan group from the KindGroup
// docs of the source's schema-property catalog and buckets them. The catalog
// is mandatory: a source without it, or a plan group missing from it, is an
// error. Only catalogs with a registered executor are accepted, so a
// misrouted group can never read the wrong on-disk format and silently
// produce zero/garbage output.
func (p *CopyPlan) ClassifyGroups(executors []CatalogExecutor) (*Classified, error) {
	supported := make(map[commonv1.Catalog]bool, len(executors))
	supportedNames := make([]string, 0, len(executors))
	for _, exec := range executors {
		supported[exec.Catalog()] = true
		supportedNames = append(supportedNames, exec.Catalog().String())
	}
	cls := &Classified{
		Buckets: map[commonv1.Catalog][]string{},
		Groups:  map[string]*commonv1.Group{},
	}
	schemaRoot, err := p.SchemaRoot()
	if err != nil {
		return nil, err
	}
	cls.SchemaRoot = schemaRoot
	groups, err := reader.LoadGroups(schemaRoot, p.Groups)
	if err != nil {
		return nil, fmt.Errorf("load groups from schema-property catalog: %w", err)
	}
	for name, g := range groups {
		cls.Groups[name] = g
	}
	for _, g := range p.Groups {
		grp := cls.Groups[g]
		if grp == nil {
			return nil, fmt.Errorf("group %q not found in the source schema-property catalog "+
				"(check source path and that the group exists)", g)
		}
		cat := grp.GetCatalog()
		if !supported[cat] {
			return nil, fmt.Errorf("group %q has catalog %v: no executor registered for it (supported: %s)",
				g, cat, strings.Join(supportedNames, ", "))
		}
		cls.Buckets[cat] = append(cls.Buckets[cat], g)
	}
	return cls, nil
}
