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

package partition

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// IndexRuleLocator is a helper struct to locate the index rule by tag name.
type IndexRuleLocator struct {
	EntitySet      map[string]struct{}
	TagFamilyTRule []map[string]*databasev1.IndexRule
}

// FieldWithType is a helper struct to store the field type.
type FieldWithType struct {
	Key  index.FieldKey
	Type pbv1.ValueType
}

// FieldIndexLocation is a helper struct to store the field index location.
type FieldIndexLocation map[string]map[string]FieldWithType

// ParseIndexRuleLocators returns a IndexRuleLocator based on the tag family spec and index rules.
func ParseIndexRuleLocators(entity *databasev1.Entity, families []*databasev1.TagFamilySpec,
	indexRules []*databasev1.IndexRule,
) (locators IndexRuleLocator, fil FieldIndexLocation) {
	locators.EntitySet = make(map[string]struct{}, len(entity.TagNames))
	fil = make(FieldIndexLocation)
	for i := range entity.TagNames {
		locators.EntitySet[entity.TagNames[i]] = struct{}{}
	}
	findIndexRuleByTagName := func(tagName string) *databasev1.IndexRule {
		for i := range indexRules {
			for j := range indexRules[i].Tags {
				if indexRules[i].Tags[j] == tagName {
					return indexRules[i]
				}
			}
		}
		return nil
	}
	for i := range families {
		ttr := make(map[string]*databasev1.IndexRule)
		locators.TagFamilyTRule = append(locators.TagFamilyTRule, ttr)
		for j := range families[i].Tags {
			ir := findIndexRuleByTagName(families[i].Tags[j].Name)
			if ir != nil {
				ttr[families[i].Tags[j].Name] = ir
			}
			tagFamily, ok := fil[families[i].Name]
			if !ok {
				tagFamily = make(map[string]FieldWithType)
				fil[families[i].Name] = tagFamily
			}
			fwt := FieldWithType{
				Type: pbv1.MustTagValueSpecToValueType(families[i].Tags[j].Type),
			}
			if ir != nil {
				fwt.Key = index.FieldKey{
					IndexRuleID: ir.Metadata.Id,
				}
			} else {
				fwt.Key = index.FieldKey{
					TagName: families[i].Tags[j].Name,
				}
			}
			tagFamily[families[i].Tags[j].Name] = fwt
		}
	}
	return locators, fil
}
