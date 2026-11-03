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
	"context"
	"errors"
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
)

// ErrInvalidNodeSelector is returned by NewStageSelector for a node selector it cannot parse.
var ErrInvalidNodeSelector = errors.New("invalid node selector")

// GroupLister lists every group a selector lays its shards out by.
type GroupLister func(ctx context.Context) ([]*commonv1.Group, error)

// NewStageSelector builds the selector that places the shards of a lifecycle stage: a
// round-robin selector that admits only the nodes matching nodeSelector, laid out by every
// group listGroups returns. Lifecycle migration and data import both place a stage with it,
// so they agree on where its shards go. The caller adds the candidate nodes; the returned
// label selector is the parsed nodeSelector.
func NewStageSelector(ctx context.Context, nodeSelector string, listGroups GroupLister) (Selector, *pub.LabelSelector, error) {
	ls, err := pub.ParseLabelSelector(nodeSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("%w %q: %w", ErrInvalidNodeSelector, nodeSelector, err)
	}
	groups, err := listGroups(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list the groups: %w", err)
	}
	sel := &roundRobinSelector{
		nodes:        make([]string, 0),
		lookupTable:  make([]key, 0),
		nodeSelector: ls,
	}
	sel.layout(groups)
	return sel, ls, nil
}
