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
	"net"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/host"
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

// Node contains the node id and address.
type Node struct {
	NodeID      string
	GrpcAddress string
	HTTPAddress string
}

var (
	// FlagNodeHost is the node id from flag.
	FlagNodeHost string
	// FlagNodeHostProvider is the node id provider from flag.
	FlagNodeHostProvider NodeHostProvider
)

// NodeHostProvider is the provider of node id.
type NodeHostProvider int

// NodeIDProvider constants.
const (
	NodeHostProviderHostname NodeHostProvider = iota
	NodeHostProviderIP
	NodeHostProviderFlag
)

// String returns the string representation of NodeIDProvider.
func (n *NodeHostProvider) String() string {
	return [...]string{"Hostname", "IP", "Flag"}[*n]
}

// ParseNodeHostProvider parses the string to NodeIDProvider.
func ParseNodeHostProvider(s string) (NodeHostProvider, error) {
	switch strings.ToLower(s) {
	case "hostname":
		return NodeHostProviderHostname, nil
	case "ip":
		return NodeHostProviderIP, nil
	case "flag":
		return NodeHostProviderFlag, nil
	default:
		return 0, fmt.Errorf("unknown node id provider %s", s)
	}
}

// GenerateNode generates a node id.
func GenerateNode(grpcPort, httpPort *uint32) (Node, error) {
	port := grpcPort
	if port == nil {
		port = httpPort
	}
	if port == nil {
		return Node{}, fmt.Errorf("no port found")
	}
	node := Node{}
	var nodeHost string
	switch FlagNodeHostProvider {
	case NodeHostProviderHostname:
		h, err := host.Name()
		if err != nil {
			return Node{}, err
		}
		nodeHost = h
	case NodeHostProviderIP:
		ip, err := host.IP()
		if err != nil {
			return Node{}, err
		}
		nodeHost = ip
	case NodeHostProviderFlag:
		nodeHost = FlagNodeHost
	default:
		return Node{}, fmt.Errorf("unknown node id provider %d", FlagNodeHostProvider)
	}
	node.NodeID = net.JoinHostPort(nodeHost, strconv.FormatUint(uint64(*port), 10))
	if grpcPort != nil {
		node.GrpcAddress = net.JoinHostPort(nodeHost, strconv.FormatUint(uint64(*grpcPort), 10))
	}
	if httpPort != nil {
		node.HTTPAddress = net.JoinHostPort(nodeHost, strconv.FormatUint(uint64(*httpPort), 10))
	}
	return node, nil
}

// ContextNodeKey is a context key to store the node id.
var ContextNodeKey = contextNodeKey{}

type contextNodeKey struct{}

// ContextNodeRolesKey is a context key to store the node roles.
var ContextNodeRolesKey = contextNodeRolesKey{}

type contextNodeRolesKey struct{}
