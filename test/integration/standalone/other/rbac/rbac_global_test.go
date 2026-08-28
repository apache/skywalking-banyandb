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

package rbac_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	serverauth "github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	clientauth "github.com/apache/skywalking-banyandb/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// The fixed policy family of issue #14014. `bydb-monitor` holds the flat custom role of
// round A4; `bydb-unbound` has credentials and no binding.
const enabledPolicy = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
  - username: "bydb-monitor"
    password: "monitor-secret"
  - username: "bydb-reader"
    password: "reader-secret"
  - username: "bydb-writer"
    password: "writer-secret"
  - username: "bydb-unbound"
    password: "unbound-secret"
rbac:
  enabled: true
  roles:
    monitor:
      permissions: ["cluster:read"]
  bindings:
    - principal: "bydb-admin"
      role: "admin"
      groups: ["*"]
    - principal: "bydb-monitor"
      role: "monitor"
      groups: ["*"]
    - principal: "bydb-reader"
      role: "reader"
      groups: ["sw_metric"]
    - principal: "bydb-writer"
      role: "writer"
      groups: ["*"]
`

// demotedPolicy revokes cluster:read from bydb-monitor. Applying it live is how the suite
// observes a reload without restarting the liaison.
const demotedPolicy = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
  - username: "bydb-monitor"
    password: "monitor-secret"
rbac:
  enabled: true
  roles:
    monitor:
      permissions: []
  bindings:
    - principal: "bydb-admin"
      role: "admin"
      groups: ["*"]
    - principal: "bydb-monitor"
      role: "monitor"
      groups: ["*"]
`

// malformedPolicy is a truncated document: what a reader observes if it wakes between the
// writer's open and its final write.
const malformedPolicy = "users:\n  - username: \"bydb-admin\"\n    password: \"admin-secret\"\n" +
	"rbac:\n  enabled: true\n  roles:\n    - name: \"admin\"\n      permissions: [\"cluster"

// invalidEnabledPolicy is well-formed YAML that does not describe a usable policy: the
// binding names a role nobody declared.
const invalidEnabledPolicy = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
rbac:
  enabled: true
  roles: {}
  bindings:
    - principal: "bydb-admin"
      role: "ghost"
      groups: ["*"]
`

type actor struct {
	name     string
	password string
}

var (
	adminActor   = actor{name: "bydb-admin", password: "admin-secret"}
	monitorActor = actor{name: "bydb-monitor", password: "monitor-secret"}
	readerActor  = actor{name: "bydb-reader", password: "reader-secret"}
	writerActor  = actor{name: "bydb-writer", password: "writer-secret"}
	unboundActor = actor{name: "bydb-unbound", password: "unbound-secret"}
)

func (a actor) ctx() context.Context {
	return metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("username", a.name, "password", a.password))
}

func writePolicy(path, content string) {
	gm.ExpectWithOffset(1, os.WriteFile(path, []byte(content), 0o600)).To(gm.Succeed())
}

func countSnapshots(root string) int {
	entries, err := os.ReadDir(filepath.Join(root, "snapshots"))
	if err != nil {
		return 0
	}
	return len(entries)
}

func httpStatus(httpAddr, path string, a actor, forged map[string]string) int {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		fmt.Sprintf("http://%s%s", httpAddr, path), nil)
	gm.ExpectWithOffset(1, err).NotTo(gm.HaveOccurred())
	if a.name != "" {
		req.Header.Set("Authorization", clientauth.GenerateBasicAuthHeader(a.name, a.password))
	}
	for name, value := range forged {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	gm.ExpectWithOffset(1, err).NotTo(gm.HaveOccurred())
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	return resp.StatusCode
}

var _ = g.Describe("rbac-global authorization through the real liaison", func() {
	var (
		grpcAddr, httpAddr string
		dataRoot           string
		policyFile         string
		conn               *grpclib.ClientConn
		deferFn            func()
	)

	g.BeforeEach(func() {
		var spaceErr error
		var releaseSpace func()
		dataRoot, releaseSpace, spaceErr = test.NewSpace()
		gm.Expect(spaceErr).NotTo(gm.HaveOccurred())

		policyFile = filepath.Join(dataRoot, "security.yaml")
		writePolicy(policyFile, enabledPolicy)

		ports, portErr := test.AllocateFreePorts(5)
		gm.Expect(portErr).NotTo(gm.HaveOccurred())

		var closeServer func()
		grpcAddr, httpAddr, closeServer = setup.EmptyClosableStandalone(nil, dataRoot, ports,
			"--auth-config-file="+policyFile)

		var dialErr error
		conn, dialErr = grpclib.NewClient(grpcAddr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(dialErr).NotTo(gm.HaveOccurred())

		deferFn = func() {
			_ = conn.Close()
			closeServer()
			releaseSpace()
		}
	})

	g.AfterEach(func() {
		deferFn()
	})

	// R3: the fixed global gRPC matrix. Every expected code is read off issue #14014's
	// role definitions and the global method table, not recomputed from the snapshot.
	g.It("decides the fixed global gRPC method matrix", func() {
		commonService := commonv1.NewServiceClient(conn)
		clusterState := databasev1.NewClusterStateServiceClient(conn)
		nodeQuery := databasev1.NewNodeQueryServiceClient(conn)
		groups := databasev1.NewGroupRegistryServiceClient(conn)
		snapshot := databasev1.NewSnapshotServiceClient(conn)
		_, apiVersionErr := commonService.GetAPIVersion(unboundActor.ctx(), &commonv1.GetAPIVersionRequest{})
		gm.Expect(apiVersionErr).NotTo(gm.HaveOccurred(), "an authenticated unbound user may negotiate the API version")

		// cluster:read — admin and monitor only.
		for _, a := range []actor{adminActor, monitorActor} {
			_, clusterErr := clusterState.GetClusterState(a.ctx(), &databasev1.GetClusterStateRequest{})
			gm.Expect(clusterErr).NotTo(gm.HaveOccurred(), "%s must be allowed GetClusterState", a.name)
			_, nodeErr := nodeQuery.GetCurrentNode(a.ctx(), &databasev1.GetCurrentNodeRequest{})
			gm.Expect(nodeErr).NotTo(gm.HaveOccurred(), "%s must be allowed GetCurrentNode", a.name)
		}
		for _, a := range []actor{readerActor, writerActor, unboundActor} {
			_, clusterErr := clusterState.GetClusterState(a.ctx(), &databasev1.GetClusterStateRequest{})
			gm.Expect(status.Code(clusterErr)).To(gm.Equal(codes.PermissionDenied), "%s must be denied GetClusterState", a.name)
			_, nodeErr := nodeQuery.GetCurrentNode(a.ctx(), &databasev1.GetCurrentNodeRequest{})
			gm.Expect(status.Code(nodeErr)).To(gm.Equal(codes.PermissionDenied), "%s must be denied GetCurrentNode", a.name)
		}
		for _, a := range []actor{adminActor, monitorActor} {
			_, inspectErr := groups.Inspect(a.ctx(), &databasev1.GroupRegistryServiceInspectRequest{Group: "missing"})
			gm.Expect(status.Code(inspectErr)).NotTo(gm.Equal(codes.PermissionDenied), "%s must pass RBAC for Inspect", a.name)
			_, queryErr := groups.Query(a.ctx(), &databasev1.GroupRegistryServiceQueryRequest{Group: "missing"})
			gm.Expect(status.Code(queryErr)).NotTo(gm.Equal(codes.PermissionDenied), "%s must pass RBAC for deletion-task Query", a.name)
		}
		_, inspectErr := groups.Inspect(readerActor.ctx(), &databasev1.GroupRegistryServiceInspectRequest{Group: "missing"})
		gm.Expect(status.Code(inspectErr)).To(gm.Equal(codes.PermissionDenied), "reader must be denied Inspect")
		_, queryErr := groups.Query(readerActor.ctx(), &databasev1.GroupRegistryServiceQueryRequest{Group: "missing"})
		gm.Expect(status.Code(queryErr)).To(gm.Equal(codes.PermissionDenied), "reader must be denied deletion-task Query")

		// cluster:admin — admin only.
		_, snapshotErr := snapshot.Snapshot(adminActor.ctx(), &databasev1.SnapshotRequest{})
		gm.Expect(status.Code(snapshotErr)).NotTo(gm.Equal(codes.PermissionDenied), "bydb-admin must be allowed Snapshot")
		for _, a := range []actor{monitorActor, readerActor, writerActor, unboundActor} {
			_, denyErr := snapshot.Snapshot(a.ctx(), &databasev1.SnapshotRequest{})
			gm.Expect(status.Code(denyErr)).To(gm.Equal(codes.PermissionDenied), "%s must be denied Snapshot", a.name)
		}
	})

	// R3: authentication precedes authorization. bydb-admin is allowed on this method, so
	// a wrong password yielding Unauthenticated rather than PermissionDenied is what shows
	// the order.
	g.It("rejects bad credentials before it consults the grants", func() {
		clusterState := databasev1.NewClusterStateServiceClient(conn)
		wrong := actor{name: adminActor.name, password: "not-the-password"}
		_, err := clusterState.GetClusterState(wrong.ctx(), &databasev1.GetClusterStateRequest{})
		gm.Expect(status.Code(err)).To(gm.Equal(codes.Unauthenticated))

		_, err = clusterState.GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		gm.Expect(status.Code(err)).To(gm.Equal(codes.Unauthenticated))
	})

	// R3/D3: the generated Unimplemented fallback. DeleteExpiredSegments is registered on
	// the liaison and has no handler, so authorization must run first: admin passes it and
	// reaches the generated fallback, everyone else is stopped before it.
	g.It("authorizes generated Unimplemented methods before their fallback runs", func() {
		measure := measurev1.NewMeasureServiceClient(conn)
		_, err := measure.DeleteExpiredSegments(adminActor.ctx(), &measurev1.DeleteExpiredSegmentsRequest{})
		gm.Expect(status.Code(err)).To(gm.Equal(codes.Unimplemented),
			"bydb-admin must be authorized through to the generated fallback")
		for _, a := range []actor{monitorActor, readerActor, writerActor, unboundActor} {
			_, denyErr := measure.DeleteExpiredSegments(a.ctx(), &measurev1.DeleteExpiredSegmentsRequest{})
			gm.Expect(status.Code(denyErr)).To(gm.Equal(codes.PermissionDenied),
				"%s must be stopped before the generated fallback", a.name)
		}
		_, err = measure.InternalQuery(adminActor.ctx(), &measurev1.InternalQueryRequest{})
		gm.Expect(status.Code(err)).To(gm.Equal(codes.Unimplemented),
			"bydb-admin must be authorized through to the InternalQuery fallback")
		_, err = measure.InternalQuery(readerActor.ctx(), &measurev1.InternalQueryRequest{})
		gm.Expect(status.Code(err)).To(gm.Equal(codes.PermissionDenied),
			"bydb-reader must be stopped before the InternalQuery fallback")
	})

	// R6: schema and data permissions have no activated executor in this release, so every
	// method carrying one fails closed for every actor, admin included.
	g.It("fails closed on every not-yet-activated schema and data method", func() {
		measure := measurev1.NewMeasureServiceClient(conn)
		groups := databasev1.NewGroupRegistryServiceClient(conn)
		for _, a := range []actor{adminActor, monitorActor, readerActor, writerActor, unboundActor} {
			_, err := measure.Query(a.ctx(), &measurev1.QueryRequest{})
			gm.Expect(status.Code(err)).To(gm.Equal(codes.PermissionDenied),
				"%s must fail closed on MeasureService/Query", a.name)
			_, err = groups.List(a.ctx(), &databasev1.GroupRegistryServiceListRequest{})
			gm.Expect(status.Code(err)).To(gm.Equal(codes.PermissionDenied),
				"%s must fail closed on GroupRegistryService/List", a.name)
		}
	})

	// R3/D2: a denied call to a handler with a real side effect must leave no trace. The
	// Snapshot handler writes snapshot directories under the data root, so the count of
	// those directories is an observation independent of any status code.
	g.It("leaves no side effect behind a denied Snapshot", func() {
		before := countSnapshots(dataRoot)
		snapshot := databasev1.NewSnapshotServiceClient(conn)
		for _, a := range []actor{monitorActor, readerActor, writerActor, unboundActor} {
			_, err := snapshot.Snapshot(a.ctx(), &databasev1.SnapshotRequest{})
			gm.Expect(status.Code(err)).To(gm.Equal(codes.PermissionDenied))
		}
		gm.Consistently(func() int { return countSnapshots(dataRoot) }, 3*time.Second).
			Should(gm.Equal(before), "a denied Snapshot must not create a snapshot")
	})

	// R3: the same matrix over the bound HTTP routes. grpc-gateway maps PermissionDenied
	// to 403 and Unauthenticated to 401; the Basic-auth middleware answers 401 itself.
	g.It("decides the fixed global matrix over the bound HTTP routes", func() {
		gm.Expect(httpStatus(httpAddr, "/api/v1/cluster/state", monitorActor, nil)).To(gm.Equal(http.StatusOK))
		gm.Expect(httpStatus(httpAddr, "/api/v1/cluster/state", adminActor, nil)).To(gm.Equal(http.StatusOK))
		gm.Expect(httpStatus(httpAddr, "/api/v1/cluster/state", readerActor, nil)).To(gm.Equal(http.StatusForbidden))
		gm.Expect(httpStatus(httpAddr, "/api/v1/cluster/state", unboundActor, nil)).To(gm.Equal(http.StatusForbidden))
		gm.Expect(httpStatus(httpAddr, "/api/v1/snapshot", monitorActor, nil)).To(gm.Equal(http.StatusForbidden))
		gm.Expect(httpStatus(httpAddr, "/api/v1/cluster/state",
			actor{name: adminActor.name, password: "not-the-password"}, nil)).To(gm.Equal(http.StatusUnauthorized))
	})

	// R4: a forged gateway identity cannot replace Basic auth. The reader's credentials
	// decide the call no matter what identity the request also asserts.
	g.It("refuses a forged gateway identity", func() {
		forged := map[string]string{
			"Grpc-Metadata-Username": adminActor.name,
			"Grpc-Metadata-Password": adminActor.password,
		}
		gm.Expect(httpStatus(httpAddr, "/api/v1/snapshot", readerActor, forged)).To(gm.Equal(http.StatusForbidden))
		gm.Expect(httpStatus(httpAddr, "/api/v1/cluster/state", readerActor, forged)).To(gm.Equal(http.StatusForbidden))
		// With no credentials at all, a forged identity must not authenticate anybody.
		gm.Expect(httpStatus(httpAddr, "/api/v1/cluster/state", actor{}, forged)).To(gm.Equal(http.StatusUnauthorized))
	})

	// R5: a valid policy is applied live, and a malformed one that lands afterwards leaves
	// the last known good revision serving. The liaison is never restarted.
	g.It("reloads a valid policy and retains the last known good on a malformed one", func() {
		clusterState := databasev1.NewClusterStateServiceClient(conn)
		_, err := clusterState.GetClusterState(monitorActor.ctx(), &databasev1.GetClusterStateRequest{})
		gm.Expect(err).NotTo(gm.HaveOccurred(), "bydb-monitor starts out holding cluster:read")

		writePolicy(policyFile, demotedPolicy)
		gm.Eventually(func() codes.Code {
			_, reloadErr := clusterState.GetClusterState(monitorActor.ctx(), &databasev1.GetClusterStateRequest{})
			return status.Code(reloadErr)
		}, 30*time.Second).Should(gm.Equal(codes.PermissionDenied), "the demotion must take effect without a restart")

		writePolicy(policyFile, malformedPolicy)
		gm.Consistently(func() codes.Code {
			_, badErr := clusterState.GetClusterState(adminActor.ctx(), &databasev1.GetClusterStateRequest{})
			return status.Code(badErr)
		}, 5*time.Second).Should(gm.Equal(codes.OK), "a malformed reload must leave the previous revision serving")
		gm.Expect(func() codes.Code {
			_, denyErr := clusterState.GetClusterState(monitorActor.ctx(), &databasev1.GetClusterStateRequest{})
			return status.Code(denyErr)
		}()).To(gm.Equal(codes.PermissionDenied), "the malformed file must not resurrect the revoked grant")
	})

	// R1: an invalid enabled policy is refused at configuration time, which is what makes
	// the liaison stop before it starts a watcher or a listener. This is checked outside
	// the running server on purpose: a server that reached readiness has already passed it.
	g.It("refuses an invalid enabled policy at startup", func() {
		bad := filepath.Join(dataRoot, "invalid-security.yaml")
		writePolicy(bad, invalidEnabledPolicy)
		reloader := serverauth.InitAuthReloader()
		gm.Expect(reloader.ConfigAuthReloader(bad, false, logger.GetLogger("rbac-invalid-startup"))).
			To(gm.HaveOccurred(), "an invalid enabled policy must stop startup")
	})
})
