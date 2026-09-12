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

package grpc

import (
	"context"
	"testing"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

// Liaison roles do not enable the memory protector, so property list-all uses the
// 64 MiB fallback pool (8 MiB max query). OAP UI-template bootstrap must fit.
func TestPropertySourceChargeFitsFallbackScanBudget(t *testing.T) {
	budget := protector.NewQueryBudget(nil)
	ctx, release, err := budget.AdmitScanContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// ~1 MiB of UI-template JSON (matches Cluster E2E property shard footprint).
	const properties, payload = 80, 12 << 10
	for i := 0; i < properties; i++ {
		source := make([]byte, payload)
		if chargeErr := query.ChargeResult(ctx, uint64(len(source))+256); chargeErr != nil {
			t.Fatalf("property %d exhausted fallback scan budget: %v", i, chargeErr)
		}
		if chargeErr := query.Charge(ctx, 128); chargeErr != nil {
			t.Fatal(chargeErr)
		}
	}
	resp := &propertyv1.QueryResponse{Properties: make([]*propertyv1.Property, properties)}
	for i := range resp.Properties {
		resp.Properties[i] = &propertyv1.Property{Id: string(make([]byte, payload/4))}
	}
	if chargeErr := query.ChargeResponse(ctx, resp); chargeErr != nil {
		t.Fatalf("response encoding exhausted fallback scan budget: %v", chargeErr)
	}
}
