// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package measure

import (
	"context"
	"fmt"
	"strings"

	compat "github.com/blugelabs/bluge"
	compatsearch "github.com/blugelabs/bluge/search"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type storedFieldDocument interface {
	VisitStoredFields(visit func(name string, value []byte) bool) error
}

type retainedStoredDocument struct {
	match *compatsearch.DocumentMatch
}

func (d retainedStoredDocument) VisitStoredFields(visit func(name string, value []byte) bool) error {
	return d.match.VisitStoredFields(func(name string, value []byte) bool {
		return visit(name, value)
	})
}

func readIndexModeDocs(ctx context.Context, sidxDir string, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo,
) ([]index.Document, error) {
	reader, openErr := compat.OpenReader(compat.DefaultConfig(sidxDir))
	if openErr != nil {
		if strings.Contains(openErr.Error(), "unable to find a usable snapshot") {
			return nil, nil
		}
		return nil, fmt.Errorf("open sidx reader %s: %w", sidxDir, openErr)
	}
	defer func() { _ = reader.Close() }()
	matches, searchErr := reader.Search(ctx, compat.NewAllMatches(compat.NewMatchAllQuery()))
	if searchErr != nil {
		return nil, fmt.Errorf("search sidx %s: %w", sidxDir, searchErr)
	}
	var documents []index.Document
	var timeless []uint64
	tagNames := collectTagNames(schemasBySubject)
	missingRules := map[uint32]int{}
	for {
		match, nextErr := matches.Next()
		if nextErr != nil {
			return nil, fmt.Errorf("iterate sidx %s: %w", sidxDir, nextErr)
		}
		if match == nil {
			break
		}
		document, rebuildErr := rebuildOneDoc(retainedStoredDocument{match: match}, ruleByID, schemasBySubject, tagNames, missingRules)
		if rebuildErr != nil {
			return nil, fmt.Errorf("rebuild doc in %s: %w", sidxDir, rebuildErr)
		}
		if document.Timestamp == 0 {
			timeless = append(timeless, document.DocID)
			continue
		}
		documents = append(documents, document)
	}
	if len(timeless) > 0 {
		sample := timeless
		if len(sample) > 10 {
			sample = sample[:10]
		}
		return nil, fmt.Errorf("sidx %s: %d index-mode doc(s) have a missing or undecodable _timestamp (ts==0), "+
			"which never occurs for valid data; the source is corrupt or unexpected — sample series IDs (first %d) %v",
			sidxDir, len(timeless), len(sample), sample)
	}
	warnMissingRules(sidxDir, missingRules)
	return documents, nil
}

func rebuildOneDoc(source storedFieldDocument, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo, tagNames map[string]struct{}, missingRules map[uint32]int,
) (index.Document, error) {
	var entityValues []byte
	var timestamp, version int64
	var fields []index.Field
	visitErr := source.VisitStoredFields(func(name string, value []byte) bool {
		switch name {
		case imDocIDField:
			entityValues = append([]byte(nil), value...)
		case imTimestampField:
			if decodedTime, decodeErr := compat.DecodeDateTime(value); decodeErr == nil {
				timestamp = decodedTime.UnixNano()
			}
		case imVersionField:
			version = convert.BytesToInt64(value)
		default:
			key, indexed, missingRuleID := classifyStoredField(name, tagNames, ruleByID)
			field := index.NewBytesField(key, value)
			field.Store = true
			field.Index = indexed
			if rule, found := ruleByID[key.IndexRuleID]; indexed && found {
				field.NoSort = rule.NoSort
				field.Key.Analyzer = rule.Analyzer
			}
			if missingRuleID != 0 && missingRules != nil {
				missingRules[missingRuleID]++
			}
			fields = append(fields, field)
		}
		return true
	})
	if visitErr != nil {
		return index.Document{}, fmt.Errorf("visit stored fields: %w", visitErr)
	}
	var series pbv1.Series
	if unmarshalErr := series.Unmarshal(entityValues); unmarshalErr != nil {
		return index.Document{}, fmt.Errorf("unmarshal series from _id: %w", unmarshalErr)
	}
	fields = appendRegeneratedEntityFields(fields, &series, schemasBySubject[series.Subject])
	return index.Document{
		Fields:       fields,
		EntityValues: entityValues,
		Timestamp:    timestamp,
		DocID:        uint64(series.ID),
		Version:      version,
	}, nil
}
