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

package test

import (
	"context"
	"sync"

	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// SetupModules wires input modules to build a testing ready runtime.
func SetupModules(flags []string, units ...run.Unit) func() {
	closer, deferFn := run.NewTester("closer")
	g := run.NewGroup("standalone-test")
	g.Register(append([]run.Unit{closer}, units...)...)
	err := g.RegisterFlags().Parse(flags)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer func() {
			wg.Done()
		}()
		errRun := g.Run(context.WithValue(context.Background(), common.ContextNodeKey, common.Node{NodeID: "test"}))
		gomega.Expect(errRun).ShouldNot(gomega.HaveOccurred())
	}()
	g.WaitTillReady()
	return func() {
		deferFn()
		wg.Wait()
	}
}
