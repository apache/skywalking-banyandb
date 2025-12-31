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

package dns

import (
	"context"

	"google.golang.org/grpc"
)

// Test exports for external tests (package dns_test).

// NewServiceWithResolver creates a service with a custom resolver for testing.
func NewServiceWithResolver(cfg Config, resolver Resolver) (*Service, error) {
	return newServiceWithResolver(cfg, resolver)
}

// QueryAllSRVRecords is exported for testing DNS query behavior.
func (s *Service) QueryAllSRVRecords(ctx context.Context) ([]string, error) {
	return s.queryAllSRVRecords(ctx)
}

// QueryDNSAndUpdateNodes is exported for testing DNS query and cache update behavior.
func (s *Service) QueryDNSAndUpdateNodes(ctx context.Context) error {
	return s.queryDNSAndUpdateNodes(ctx)
}

// GetLastSuccessfulDNS returns the cached successful DNS addresses for testing.
func (s *Service) GetLastSuccessfulDNS() []string {
	s.lastSuccessMutex.RLock()
	defer s.lastSuccessMutex.RUnlock()
	result := make([]string, len(s.lastSuccessfulDNS))
	copy(result, s.lastSuccessfulDNS)
	return result
}

// GetTLSDialOptions is exported for testing TLS configuration.
func (s *Service) GetTLSDialOptions() ([]grpc.DialOption, error) {
	return s.getTLSDialOptions()
}
