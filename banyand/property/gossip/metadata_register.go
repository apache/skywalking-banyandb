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

package gossip

import (
	"context"

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// MessengerProvider provides access to a Messenger that may be lazily resolved.
type MessengerProvider interface {
	GetGossIPMessenger() Messenger
}

type metadataRegister struct {
	provider MessengerProvider
	metadata metadata.Repo
}

// NewMetadataRegister creates a new unit that registers metadata handlers during PreRun phase.
func NewMetadataRegister(provider MessengerProvider, meta metadata.Repo) run.Unit {
	return &metadataRegister{
		provider: provider,
		metadata: meta,
	}
}

func (r *metadataRegister) Name() string {
	return "gossip-metadata-register"
}

// PreRun registers metadata handlers so they are ready before watchers start.
func (r *metadataRegister) PreRun(ctx context.Context) error {
	messenger := r.provider.GetGossIPMessenger()
	if messenger == nil || r.metadata == nil {
		return nil
	}
	gossipSvc := messenger.(*service)
	gossipSvc.sel.OnInit([]schema.Kind{schema.KindGroup})
	r.metadata.RegisterHandler("property-repair-nodes", schema.KindNode, gossipSvc)
	r.metadata.RegisterHandler("property-repair-groups", schema.KindGroup, gossipSvc)
	if initErr := gossipSvc.initTracing(ctx); initErr != nil {
		gossipSvc.log.Error().Err(initErr).Msg("failed to initialize internal tracing")
	}
	return nil
}
