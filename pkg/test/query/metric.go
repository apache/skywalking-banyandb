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

package query

import (
	"flag"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/apache/skywalking-cli/pkg/graphql/dependency"
	"github.com/apache/skywalking-cli/pkg/graphql/metrics"
	"github.com/apache/skywalking-cli/pkg/graphql/utils"
	"github.com/urfave/cli/v2"
	api "skywalking.apache.org/repo/goapi/query"
)

var (
	isNormal   = true
	serviceTpl = "g%d::service_0"
)

// ServiceList verifies the service list.
func ServiceList(basePath string, timeout time.Duration, groupNum int, fs *flag.FlagSet) {
	basePath = path.Join(basePath, "svc-list")
	err := os.MkdirAll(basePath, 0o755)
	if err != nil {
		panic(err)
	}
	stopCh := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		close(stopCh)
	}()
	metricNames := []string{"service_sla", "service_cpm", "service_resp_time", "service_apdex"}
	size := len(metricNames) * groupNum
	header := make([]string, size)
	for k, mName := range metricNames {
		for i := 0; i < groupNum; i++ {
			offset := k*groupNum + i
			header[offset] = fmt.Sprintf("%s-%d", mName, i)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		collect(basePath, func() ([]float64, error) {
			data := make([]float64, size)
			var subWg sync.WaitGroup
			for k, mName := range metricNames {
				for i := 0; i < groupNum; i++ {
					offset := k*groupNum + i
					svc := fmt.Sprintf(serviceTpl, i)
					subWg.Add(1)
					go func(mName, svc string) {
						defer subWg.Done()
						d, err := execute(mName, svc, "", fs)
						if err != nil {
							fmt.Printf("query metric %s %s error: %v \n", mName, svc, err)
						}
						data[offset] = d.Seconds()
					}(mName, svc)
				}
			}
			subWg.Wait()
			return data, nil
		}, 500*time.Millisecond, stopCh)
		analyze(header, basePath)
	}()
	wg.Wait()
}

func execute(exp, svc, instance string, fs *flag.FlagSet) (time.Duration, error) {
	ctx := cli.NewContext(cli.NewApp(), fs, nil)
	entity := &api.Entity{
		ServiceName:         &svc,
		Normal:              &isNormal,
		ServiceInstanceName: &instance,
	}
	duration := api.Duration{
		Start: time.Now().Add(-30 * time.Minute).Format(utils.StepFormats[api.StepMinute]),
		End:   time.Now().Format(utils.StepFormats[api.StepMinute]),
		Step:  api.StepMinute,
	}
	start := time.Now()
	result, err := metrics.Execute(ctx, exp, entity, duration)
	elapsed := time.Since(start)
	if err != nil {
		return 0, err
	}
	if len(result.Results) < 1 {
		return 0, fmt.Errorf("no result")
	}
	return elapsed, nil
}

// TopN verifies the top N.
func TopN(basePath string, timeout time.Duration, groupNum int, fs *flag.FlagSet) {
	basePath = path.Join(basePath, "topn")
	err := os.MkdirAll(basePath, 0o755)
	if err != nil {
		panic(err)
	}
	stopCh := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		close(stopCh)
	}()
	metricNames := []string{"service_instance_resp_time", "service_instance_cpm"}
	size := len(metricNames) * groupNum
	header := make([]string, size)
	for k, mName := range metricNames {
		for i := 0; i < groupNum; i++ {
			offset := k*groupNum + i
			header[offset] = fmt.Sprintf("%s-%d", mName, i)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		collect(basePath, func() ([]float64, error) {
			data := make([]float64, size)
			var subWg sync.WaitGroup
			for k, mName := range metricNames {
				for i := 0; i < groupNum; i++ {
					offset := k*groupNum + i
					svc := fmt.Sprintf(serviceTpl, i)
					subWg.Add(1)
					go func(mName, svc string) {
						defer subWg.Done()
						d, err := sortMetrics(mName, svc, 5, api.OrderDes, fs)
						if err != nil {
							data[offset] = -1
							fmt.Printf("query metric %s %s error: %v \n", mName, svc, err)
						} else {
							data[offset] = d.Seconds()
						}
					}(mName, svc)
				}
			}
			subWg.Wait()
			return data, nil
		}, 500*time.Millisecond, stopCh)
		analyze(header, basePath)
	}()
	wg.Wait()
}

func sortMetrics(name, svc string, limit int, order api.Order, fs *flag.FlagSet) (time.Duration, error) {
	ctx := cli.NewContext(cli.NewApp(), fs, nil)
	duration := api.Duration{
		Start: time.Now().Add(-30 * time.Minute).Format(utils.StepFormats[api.StepMinute]),
		End:   time.Now().Format(utils.StepFormats[api.StepMinute]),
		Step:  api.StepMinute,
	}
	scope := api.ScopeServiceInstance
	cond := api.TopNCondition{
		Name:          name,
		ParentService: &svc,
		Normal:        &isNormal,
		Scope:         &scope,
		TopN:          limit,
		Order:         order,
	}
	start := time.Now()
	result, err := metrics.SortMetrics(ctx, cond, duration)
	elapsed := time.Since(start)
	if err != nil {
		return 0, err
	}
	if len(result) < 1 {
		return 0, fmt.Errorf("no result")
	}
	return elapsed, nil
}

// Topology verifies the topology.
func Topology(basePath string, timeout time.Duration, groupNum int, fs *flag.FlagSet) {
	basePath = path.Join(basePath, "topology")
	err := os.MkdirAll(basePath, 0o755)
	if err != nil {
		panic(err)
	}
	stopCh := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		close(stopCh)
	}()
	header := make([]string, groupNum)
	for i := 0; i < groupNum; i++ {
		header[i] = fmt.Sprintf("topology-%d", i)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		collect(basePath, func() ([]float64, error) {
			data := make([]float64, groupNum)
			var subWg sync.WaitGroup
			for i := 0; i < groupNum; i++ {
				subWg.Add(1)
				go func() {
					defer subWg.Done()
					d, err := globalTopology(fs)
					if err != nil {
						fmt.Printf("query topology error: %v \n", err)
					}
					data[i] = d.Seconds()
				}()
			}
			subWg.Wait()
			return data, nil
		}, 500*time.Millisecond, stopCh)
		analyze(header, basePath)
	}()
	wg.Wait()
}

func globalTopology(fs *flag.FlagSet) (time.Duration, error) {
	ctx := cli.NewContext(cli.NewApp(), fs, nil)
	duration := api.Duration{
		Start: time.Now().Add(-200 * time.Minute).Format(utils.StepFormats[api.StepMinute]),
		End:   time.Now().Format(utils.StepFormats[api.StepMinute]),
		Step:  api.StepMinute,
	}
	start := time.Now()
	result, err := dependency.GlobalTopologyWithoutLayer(ctx, duration)
	elapsed := time.Since(start)
	if err != nil {
		return 0, err
	}
	if len(result.Nodes) < 1 {
		return 0, fmt.Errorf("no result")
	}
	return elapsed, nil
}
