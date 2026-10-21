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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package measure_test

import (
	"context"
	"fmt"
	"time"

	gm "github.com/onsi/gomega"

	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func generatedReuseAuthorization(group string) func(context.Context) context.Context {
	// Fixed test-only credentials mint a real principal through the production
	// snapshot compiler. The hook replaces request context, never its decision.
	policy := fmt.Sprintf(`
users:
  - username: "ql-cache-reader"
    password: "ql-cache-fixture"
rbac:
  enabled: true
  bindings:
    - principal: "ql-cache-reader"
      role: "reader"
      groups: ["%s"]
`, group)
	snapshot, snapshotErr := auth.CompileSnapshot(1, []byte(policy))
	gm.Expect(snapshotErr).NotTo(gm.HaveOccurred())
	principal, authenticated := snapshot.Authenticate("ql-cache-reader", "ql-cache-fixture")
	gm.Expect(authenticated).To(gm.BeTrue())
	return func(ctx context.Context) context.Context {
		return liaisongrpc.ContextWithSnapshot(liaisongrpc.ContextWithPlanCachePrincipalForTest(ctx, principal), snapshot)
	}
}

func verifyReusePermissionsAndFallback(base time.Time, scenario qlgen.ReuseScenario) {
	before := liaisongrpc.BydbQLPlanCacheStats()
	for index, group := range []string{qlgen.FixtureGroup, "revoked_scope", qlgen.FixtureGroup} {
		func() {
			restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{Context: generatedReuseAuthorization(group)})
			defer restore()
			gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[index])).To(gm.Succeed())
		}()
	}
	after := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(after.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "permissions must be reevaluated on the same cached immutable plan")
	gm.Expect(after.Hits - before.Hits).To(gm.Equal(uint64(2)))
	func() {
		restoreSchema := liaisongrpc.WithholdBydbQLMeasureSchemaForTest(qlgen.FixtureGroup, qlgen.T05FixtureName)
		defer restoreSchema()
		restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{
			Context: generatedReuseAuthorization(qlgen.FixtureGroup),
		})
		defer restore()
		gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[3])).To(gm.Succeed())
	}()
	fallback := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(fallback.Bypass - after.Bypass).To(gm.Equal(uint64(1)))
	gm.Expect(fallback.Compiles-after.Compiles).To(gm.Equal(uint64(1)), "missing local schema must resolve through the real registry once")
	gm.Expect(fallback.Entries).To(gm.BeZero(), "registry fallback must execute without publishing a reusable plan")
}

func verifyReuseBoundsAndBypass(base time.Time, scenario qlgen.ReuseScenario) {
	if scenario.Kind == "oversized-fallback" {
		restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{MaxBytes: 1})
		defer restore()
	}
	before := liaisongrpc.BydbQLPlanCacheStats()
	for _, c := range scenario.Cases {
		gm.Expect(executeGeneratedReuseCall(context.Background(), base, c)).To(gm.Succeed())
	}
	after := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(after.Hits).To(gm.Equal(before.Hits))
	gm.Expect(after.Entries).To(gm.BeZero())
	if scenario.Kind == "oversized-fallback" {
		gm.Expect(after.Oversize - before.Oversize).To(gm.Equal(uint64(3)))
	} else {
		gm.Expect(after.Bypass-before.Bypass).To(gm.Equal(uint64(3)), "literal queries retain their cache bypass policy")
	}
}
