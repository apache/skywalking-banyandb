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

package common

import (
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

type (
	// SeriesID identities a series in a shard.
	SeriesID uint64

	// ShardID identities a shard in a tsdb.
	ShardID uint32

	// ItemID identities an item in a series.
	ItemID uint64
)

// Marshal encodes series id to bytes.
func (s SeriesID) Marshal() []byte {
	return convert.Uint64ToBytes(uint64(s))
}

// positionKey is a context key to store the module position.
var positionKey = contextPositionKey{}

type contextPositionKey struct{}

// Position is stored in the context.
// The logger could attach it for debugging.
type Position struct {
	Module   string
	Database string
	Shard    string
	Segment  string
	Block    string
}

// LabelNames returns the label names of Position.
func LabelNames() []string {
	return []string{"module", "database", "shard", "seg", "block"}
}

// ShardLabelNames returns the label names of Position. It is used for shard level metrics.
func ShardLabelNames() []string {
	return []string{"module", "database", "shard"}
}

// LabelValues returns the label values of Position.
func (p Position) LabelValues() []string {
	return []string{p.Module, p.Database, p.Shard, p.Segment, p.Block}
}

// ShardLabelValues returns the label values of Position. It is used for shard level metrics.
func (p Position) ShardLabelValues() []string {
	return []string{p.Module, p.Database, p.Shard}
}

// SetPosition sets a position returned from fn to attach it to ctx, then return a new context.
func SetPosition(ctx context.Context, fn func(p Position) Position) context.Context {
	val := ctx.Value(positionKey)
	var p Position
	if val == nil {
		p = Position{}
	} else {
		p = val.(Position)
	}
	return context.WithValue(ctx, positionKey, fn(p))
}

// GetPosition returns the position from ctx.
func GetPosition(ctx context.Context) Position {
	val := ctx.Value(positionKey)
	if val == nil {
		return Position{}
	}
	return val.(Position)
}

// Error wraps a error msg.
type Error struct {
	msg string
}

// NewError returns a new Error.
func NewError(tpl string, args ...any) Error {
	return Error{msg: fmt.Sprintf(tpl, args...)}
}

// Msg shows the string msg.
func (e Error) Msg() string {
	return e.msg
}
