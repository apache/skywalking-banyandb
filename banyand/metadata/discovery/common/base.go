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

// DiscoveryServiceBase combines NodeCacheBase and RetryManager for discovery services.
type DiscoveryServiceBase struct {
	*NodeCacheBase
	*RetryManager
}

// NewServiceBase create a new base service for gRPC-based and file-based service.
func NewServiceBase(
	loggerName string,
	fetcher NodeFetcher,
	retryConfig RetryConfig,
) *DiscoveryServiceBase {
	cacheBase := NewNodeCacheBase(loggerName)
	retryManager := NewRetryManager(fetcher, cacheBase, retryConfig, nil)
	return &DiscoveryServiceBase{
		NodeCacheBase: cacheBase,
		RetryManager:  retryManager,
	}
}

// SetMetrics sets metrics for the retry manager.
func (b *DiscoveryServiceBase) SetMetrics(metrics RetryMetrics) {
	if b.RetryManager != nil {
		b.RetryManager.SetMetrics(metrics)
	}
}
