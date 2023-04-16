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

package meter

import (
	"sync"
)

// HierarchicalScope is a Scope implementation that supports hierarchical scopes.
type HierarchicalScope struct {
	parent *HierarchicalScope
	labels LabelPairs
	sep    string
	name   string
	mu     sync.RWMutex
}

// NewHierarchicalScope creates a new hierarchical scope.
func NewHierarchicalScope(name, sep string) Scope {
	return &HierarchicalScope{sep: sep, name: name}
}

// ConstLabels merges the given labels with the labels of the parent scope.
func (s *HierarchicalScope) ConstLabels(labels LabelPairs) Scope {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.parent != nil {
		labels = s.parent.GetLabels().Merge(labels)
	}
	s.labels = labels
	return s
}

// SubScope creates a new sub-scope with the given name.
func (s *HierarchicalScope) SubScope(name string) Scope {
	s.mu.Lock()
	defer s.mu.Unlock()

	return &HierarchicalScope{
		parent: s,
		name:   name,
		sep:    s.sep,
	}
}

// GetNamespace returns the namespace of this scope.
func (s *HierarchicalScope) GetNamespace() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.parent == nil {
		return s.name
	}
	return s.parent.GetNamespace() + s.sep + s.name
}

// GetLabels returns the labels of this scope.
func (s *HierarchicalScope) GetLabels() LabelPairs {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.labels
}
