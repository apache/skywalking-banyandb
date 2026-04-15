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

package crashcollector

import (
	"encoding/json"
	"fmt"
)

// collectionLister returns the latest crash collection records.
type collectionLister interface {
	ListCollections() []CollectionRecord
}

// MultiCollectionProvider combines multiple collection sources into a single provider.
// It deduplicates by artifact directory name so the same crash is not reported twice
// when both the filesystem watcher and the HTTP collector detect it.
type MultiCollectionProvider struct {
	providers []collectionLister
}

// NewMultiCollectionProvider returns a MultiCollectionProvider that merges results
// from all given providers.
func NewMultiCollectionProvider(providers ...collectionLister) *MultiCollectionProvider {
	return &MultiCollectionProvider{providers: providers}
}

// MarshalCollections merges, deduplicates, and marshals collections from all providers.
func (m *MultiCollectionProvider) MarshalCollections() ([]byte, error) {
	seen := make(map[string]struct{})
	var all []CollectionRecord
	for _, provider := range m.providers {
		for _, record := range provider.ListCollections() {
			if _, exists := seen[record.Collection.ArtifactDir]; exists {
				continue
			}
			seen[record.Collection.ArtifactDir] = struct{}{}
			all = append(all, record)
		}
	}
	if all == nil {
		all = make([]CollectionRecord, 0)
	}
	data, err := json.Marshal(all)
	if err != nil {
		return nil, fmt.Errorf("marshal collections: %w", err)
	}
	return data, nil
}
