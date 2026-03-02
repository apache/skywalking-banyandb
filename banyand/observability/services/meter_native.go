// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package services

import (
	"context"
	"sync"

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/native"
)

type nativeProviderFactory struct {
	metadata  metadata.Repo
	nodeInfo  native.NodeInfo
	providers []meter.Provider
	mu        sync.Mutex
}

func (f *nativeProviderFactory) provider(scope meter.Scope) meter.Provider {
	p := native.NewProvider(scope, f.metadata, f.nodeInfo)
	f.mu.Lock()
	f.providers = append(f.providers, p)
	f.mu.Unlock()
	return p
}

func (f *nativeProviderFactory) initAllSchemas(ctx context.Context) {
	f.mu.Lock()
	providers := append([]meter.Provider(nil), f.providers...)
	f.mu.Unlock()
	for _, p := range providers {
		native.InitSchema(ctx, p)
	}
}
