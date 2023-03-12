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

	"github.com/prometheus/client_golang/prometheus"

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

// PositionKey is a context key to store the module position.
var PositionKey = contextPositionKey{}

type contextPositionKey struct{}

// Position is stored in the context.
// The logger could attach it for debugging.
type Position struct {
	Module   string
	Database string
	Shard    string
	Segment  string
	Block    string
	KV       string
}

// Labels converts Position to Prom Labels.
func (p Position) Labels() prometheus.Labels {
	return prometheus.Labels{
		"module":   p.Module,
		"database": p.Database,
		"shard":    p.Shard,
		"seg":      p.Segment,
		"block":    p.Block,
		"kv":       p.KV,
	}
}

// SetPosition sets a position returned from fn to attach it to ctx, then return a new context.
func SetPosition(ctx context.Context, fn func(p Position) Position) context.Context {
	val := ctx.Value(PositionKey)
	var p Position
	if val == nil {
		p = Position{}
	} else {
		p = val.(Position)
	}
	return context.WithValue(ctx, PositionKey, fn(p))
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
