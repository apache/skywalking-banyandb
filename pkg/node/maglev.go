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

package node

import (
	"github.com/kkdai/maglev"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var _ Selector = (*maglevSelector)(nil)

type maglevSelector struct {
	maglev *maglev.Maglev
}

func (m *maglevSelector) AddNode(node *databasev1.Node) {
	_ = m.maglev.Add(node.GetMetadata().GetName())
}

func (m *maglevSelector) RemoveNode(node *databasev1.Node) {
	_ = m.maglev.Remove(node.GetMetadata().GetName())
}

func (m *maglevSelector) Pick(group, name string, shardID uint32) (string, error) {
	return m.maglev.Get(formatSearchKey(group, name, shardID))
}

// NewMaglevSelector creates a new backend selector based on Maglev hashing algorithm.
func NewMaglevSelector() (Selector, error) {
	alg, err := maglev.NewMaglev(nil, 65537)
	if err != nil {
		return nil, err
	}
	return &maglevSelector{
		maglev: alg,
	}, nil
}
