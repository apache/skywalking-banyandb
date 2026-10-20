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
	"fmt"
	"slices"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

var catalogByName = map[string]commonv1.Catalog{
	"stream":   commonv1.Catalog_CATALOG_STREAM,
	"measure":  commonv1.Catalog_CATALOG_MEASURE,
	"trace":    commonv1.Catalog_CATALOG_TRACE,
	"property": commonv1.Catalog_CATALOG_PROPERTY,
}

// ParseSelector parses the syntax of one selector "catalog=<name>[,groups=<g1>;<g2>]";
// ToProto validates the catalog and group names like a plan.yaml selector. Group names
// containing any of ",;=" cannot be expressed in this syntax and must be given in the plan file.
func ParseSelector(raw string) (SelectorConfig, error) {
	var cfg SelectorConfig
	if strings.TrimSpace(raw) == "" {
		return cfg, fmt.Errorf("selector must not be empty")
	}
	groupsSeen := false
	for _, kv := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(kv, "=")
		if !ok || value == "" {
			return cfg, fmt.Errorf("selector %q: expected key=value, got %q", raw, kv)
		}
		switch key {
		case "catalog":
			if cfg.Catalog != "" {
				return cfg, fmt.Errorf("selector %q: catalog given twice", raw)
			}
			cfg.Catalog = value
		case "groups":
			if groupsSeen {
				return cfg, fmt.Errorf("selector %q: groups given twice", raw)
			}
			groupsSeen = true
			for _, g := range strings.Split(value, ";") {
				if strings.Contains(g, "=") {
					return cfg, fmt.Errorf("selector %q: group name %q contains '='; names with ',', ';' or '=' must be given in the plan file", raw, g)
				}
				cfg.Groups = append(cfg.Groups, g)
			}
		default:
			return cfg, fmt.Errorf("selector %q: unknown key %q (only catalog and groups are accepted)", raw, key)
		}
	}
	if cfg.Catalog == "" {
		return cfg, fmt.Errorf("selector %q: catalog is required", raw)
	}
	return cfg, nil
}

// ToProto converts the config selector to the wire selector, rejecting an unknown catalog
// and empty or duplicate group names.
func (s *SelectorConfig) ToProto() (*transferv1.Selector, error) {
	c, ok := catalogByName[s.Catalog]
	if !ok {
		return nil, fmt.Errorf("unknown catalog %q (stream|measure|trace|property)", s.Catalog)
	}
	for i, g := range s.Groups {
		if g == "" {
			return nil, fmt.Errorf("empty group name")
		}
		if slices.Contains(s.Groups[:i], g) {
			return nil, fmt.Errorf("group %q given twice", g)
		}
	}
	return &transferv1.Selector{Catalog: c, Groups: append([]string(nil), s.Groups...)}, nil
}

// CatalogName is the inverse of the selector catalog names, for rendering.
func CatalogName(c commonv1.Catalog) string {
	for name, v := range catalogByName {
		if v == c {
			return name
		}
	}
	return c.String()
}
