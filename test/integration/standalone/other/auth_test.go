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

package integration_other_test

import (
	"embed"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sigs.k8s.io/yaml"

	serverAuth "github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

//go:embed testdata/config.yaml
var serverConfigFS embed.FS

var _ = g.Describe("Query service_cpm_minute with authentication", func() {
	var deferFn func()
	var baseTime time.Time
	var interval time.Duration
	var conn *grpclib.ClientConn
	var goods []gleak.Goroutine
	var grpcAddr, httpAddr string
	var testUser serverAuth.User
	var authCfgFile string
	var httpClient *http.Client

	g.BeforeEach(func() {
		// load server config.yaml
		cfgBytes, err := serverConfigFS.ReadFile("testdata/config.yaml")
		gm.Expect(err).NotTo(gm.HaveOccurred())
		tempServerCfg := filepath.Join(os.TempDir(), fmt.Sprintf(".bydb-%s.yaml", uuid.New().String()))
		err = os.WriteFile(tempServerCfg, cfgBytes, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		authCfgFile = tempServerCfg
		info, _ := os.Stat(authCfgFile)
		gm.Expect(info.Mode().Perm()).To(gm.Equal(os.FileMode(0o600)))

		var cfg serverAuth.Config
		err = yaml.Unmarshal(cfgBytes, &cfg)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		// take the first user as the test user
		gm.Expect(len(cfg.Users)).Should(gm.BeNumerically(">", 0))
		testUser = cfg.Users[0]
		// Username and password must be provided because health checks require authentication when --enable-health-auth=true is enabled.
		grpcAddr, httpAddr, deferFn = setup.StandaloneWithAuth(
			testUser.Username, testUser.Password,
			fmt.Sprintf("--auth-config-file=%s", authCfgFile),
			"--enable-health-auth=true",
		)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		conn, err = grpchelper.ConnWithAuth(grpcAddr, 10*time.Second, testUser.Username, testUser.Password, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		ns := timestamp.NowMilli().UnixNano()
		baseTime = time.Unix(0, ns-ns%int64(time.Minute))
		interval = 500 * time.Millisecond
		casesMeasureData.WriteWithAuth(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", baseTime, interval, testUser.Username, testUser.Password)
		httpClient = &http.Client{}
		goods = gleak.Goroutines()
	})

	g.AfterEach(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		deferFn()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	g.It("grpc query and healthcheck with correct username and password", func() {
		verifyGRPCQuery(conn, baseTime, testUser.Username, testUser.Password, true)
		opts := make([]grpclib.DialOption, 0, 1)
		opts, err := grpchelper.SecureOptions(opts, false, true, "")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		checkGRPCHealth(grpcAddr, testUser.Username, testUser.Password, opts, true)
	})

	g.It("grpc query and healthcheck with wrong username and password", func() {
		verifyGRPCQuery(conn, baseTime, testUser.Username, testUser.Password+"wrong", false)
		opts := make([]grpclib.DialOption, 0, 1)
		opts, err := grpchelper.SecureOptions(opts, false, true, "")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		checkGRPCHealth(grpcAddr, testUser.Username, testUser.Password+"wrong", opts, false)
	})

	g.It("http query and healthcheck with correct username and password", func() {
		groupListURL := fmt.Sprintf("http://%s/api/v1/group/schema/lists", httpAddr)
		gm.Eventually(func() error {
			return doHTTPCheck(httpClient, groupListURL, testUser.Username, testUser.Password, true)
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		healthCheckURL := fmt.Sprintf("http://%s/api/healthz", httpAddr)
		gm.Eventually(func() error {
			return doHTTPCheck(httpClient, healthCheckURL, testUser.Username, testUser.Password, true)
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	})

	g.It("http query and healthcheck with wrong username and password", func() {
		groupListURL := fmt.Sprintf("http://%s/api/v1/group/schema/lists", httpAddr)
		gm.Eventually(func() error {
			return doHTTPCheck(httpClient, groupListURL, testUser.Username, testUser.Password+"wrong", true)
		}, flags.EventuallyTimeout).ShouldNot(gm.Succeed())

		healthCheckURL := fmt.Sprintf("http://%s/api/healthz", httpAddr)
		gm.Eventually(func() error {
			return doHTTPCheck(httpClient, healthCheckURL, testUser.Username, testUser.Password+"wrong", true)
		}, flags.EventuallyTimeout).ShouldNot(gm.Succeed())
	})

	g.It("queries auth config reload with both gRPC and HTTP", func() {
		// query with original username and password
		groupListURL := fmt.Sprintf("http://%s/api/v1/group/schema/lists", httpAddr)
		gm.Eventually(func() error {
			return doHTTPCheck(httpClient, groupListURL, testUser.Username, testUser.Password, true)
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		// 1. Generate a new username/password and modify the original auth configuration file
		newUser := serverAuth.User{Username: "updated-user", Password: "updated-pass"}
		cfgBytes, err := os.ReadFile(authCfgFile)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		var cfg serverAuth.Config
		err = yaml.Unmarshal(cfgBytes, &cfg)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		cfg.Users = []serverAuth.User{newUser}
		newCfgBytes, err := yaml.Marshal(cfg)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		err = os.WriteFile(authCfgFile, newCfgBytes, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// 2. Waiting for server to reload configuration
		time.Sleep(2 * time.Second)

		// 3. New gRPC connections use the updated username/password
		newConn, err := grpchelper.ConnWithAuth(grpcAddr, 10*time.Second,
			newUser.Username, newUser.Password,
			grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		defer newConn.Close()
		verifyGRPCQuery(newConn, baseTime, newUser.Username, newUser.Password, true)

		// 4. Verify that old username/password cannot access gRPC
		_, err = grpchelper.ConnWithAuth(grpcAddr, 10*time.Second,
			testUser.Username, testUser.Password,
			grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).To(gm.HaveOccurred())

		// 5. New HTTP clients use updated username/password
		groupListURL = fmt.Sprintf("http://%s/api/v1/group/schema/lists", httpAddr)
		gm.Eventually(func() error {
			return doHTTPCheck(httpClient, groupListURL, newUser.Username, newUser.Password, true)
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		// 6. Verification of old HTTP username/password access failed
		gm.Eventually(func() error {
			return doHTTPCheck(httpClient, groupListURL, testUser.Username, testUser.Password, true)
		}, flags.EventuallyTimeout).ShouldNot(gm.Succeed())
	})
})

//nolint:unparam
func doHTTPCheck(client *http.Client, url, username, password string, expectOK bool) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", auth.GenerateBasicAuthHeader(username, password))
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if expectOK && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	if !expectOK && resp.StatusCode == http.StatusOK {
		return fmt.Errorf("expected failure but got %d", resp.StatusCode)
	}
	return nil
}

func verifyGRPCQuery(conn *grpclib.ClientConn, baseTime time.Time, username, password string, expectOK bool) {
	checkFn := func(innerGm gm.Gomega) {
		casesMeasureData.VerifyFnWithAuth(innerGm, helpers.SharedContext{
			Connection: conn,
			BaseTime:   baseTime,
		}, helpers.Args{Input: "all", Duration: 25 * time.Minute, Offset: -20 * time.Minute},
			username, password)
	}
	if expectOK {
		gm.Eventually(checkFn, flags.EventuallyTimeout).Should(gm.Succeed())
	} else {
		gm.Eventually(checkFn, flags.EventuallyTimeout).ShouldNot(gm.Succeed())
	}
}

func checkGRPCHealth(grpcAddr string, username, password string, opts []grpclib.DialOption, expectOK bool) {
	checkFn := helpers.HealthCheckWithAuth(grpcAddr, 10*time.Second, 10*time.Second, username, password, opts...)
	if expectOK {
		gm.Eventually(checkFn, flags.EventuallyTimeout).Should(gm.Succeed())
	} else {
		gm.Eventually(checkFn, flags.EventuallyTimeout).ShouldNot(gm.Succeed())
	}
}
