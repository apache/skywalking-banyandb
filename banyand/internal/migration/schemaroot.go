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
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	backupsnapshot "github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// errSchemaPropertyMissing signals "no schema-property/_schema under
// backupDir". Returned by findSchemaPropertyRoot when a caller asks for
// a backup-tree discovery but the snapshot omitted the catalog.
var errSchemaPropertyMissing = errors.New("schema-property catalog not found in backup")

// resolveSchemaRoot picks the `_schema` bluge directory to read schemas from.
func resolveSchemaRoot(backupDir, date, schemaPropertyPath string) (string, error) {
	if schemaPropertyPath != "" {
		return schemaPropertyPath, nil
	}
	if backupDir == "" {
		return "", errors.New("either backup-dir or schema-property-path is required")
	}
	return findSchemaPropertyRoot(backupDir, date)
}

// SchemaRoot resolves the `_schema` bluge directory for the plan's source.
func (p *CopyPlan) SchemaRoot() (string, error) {
	if p.Source.Backup != nil {
		return resolveSchemaRoot(p.Source.Backup.Root, p.Source.Backup.Date, "")
	}
	return resolveSchemaRoot("", "", p.Source.Live.SchemaPropertyPath)
}

// findSchemaPropertyRoot walks <backup>/<node>/<date>/ and returns the
// first schema-property/_schema directory it finds.
func findSchemaPropertyRoot(backupDir, date string) (string, error) {
	nodes, err := os.ReadDir(backupDir)
	if err != nil {
		return "", fmt.Errorf("read backup dir %q: %w", backupDir, err)
	}
	tryDate := func(nodeRoot, d string) (string, bool) {
		p := filepath.Join(nodeRoot, d, backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup)
		info, statErr := os.Stat(p)
		return p, statErr == nil && info.IsDir()
	}
	for _, node := range nodes {
		if !node.IsDir() {
			continue
		}
		nodeRoot := filepath.Join(backupDir, node.Name())
		candidates := []string{date}
		if date == "" {
			dates, readErr := os.ReadDir(nodeRoot)
			if readErr != nil {
				continue
			}
			candidates = candidates[:0]
			for _, d := range dates {
				if d.IsDir() {
					candidates = append(candidates, d.Name())
				}
			}
		}
		for _, c := range candidates {
			if p, ok := tryDate(nodeRoot, c); ok {
				return p, nil
			}
		}
	}
	dateClause := ""
	if date != "" {
		dateClause = fmt.Sprintf(" for date %q", date)
	}
	return "", errors.WithMessagef(errSchemaPropertyMissing,
		"no %s/%s directory found under %q%s (only hot/schema-server node backups carry schemas)",
		backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup, backupDir, dateClause)
}
