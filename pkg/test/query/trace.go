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
	"time"

	"github.com/apache/skywalking-cli/pkg/graphql/trace"
	"github.com/apache/skywalking-cli/pkg/graphql/utils"
	"github.com/urfave/cli/v2"
	api "skywalking.apache.org/repo/goapi/query"
)

// TraceListOrderByDuration verifies the trace list order by duration.
func TraceListOrderByDuration(basePath string, timeout time.Duration, fs *flag.FlagSet) {
	basePath = path.Join(basePath, "trace-duration")
	err := os.MkdirAll(basePath, 0o755)
	if err != nil {
		panic(err)
	}
	stopCh := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		close(stopCh)
	}()
	collect(basePath, func() ([]float64, error) {
		d, _, err := traceList(api.QueryOrderByDuration, fs)
		if err != nil {
			return nil, err
		}
		return []float64{d.Seconds()}, nil
	}, time.Second, stopCh)
	analyze([]string{"result"}, basePath)
}

// TraceListOrderByTime verifies the trace list order by time.
func TraceListOrderByTime(basePath string, timeout time.Duration, fs *flag.FlagSet) {
	basePath = path.Join(basePath, "trace-time")
	err := os.MkdirAll(basePath, 0o755)
	if err != nil {
		panic(err)
	}
	stopCh := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		close(stopCh)
	}()
	collect(basePath, func() ([]float64, error) {
		d, _, err := traceList(api.QueryOrderByStartTime, fs)
		if err != nil {
			return nil, err
		}
		return []float64{d.Seconds()}, nil
	}, time.Second, stopCh)
	analyze([]string{"result"}, basePath)
}

// TraceByID verifies the trace by ID.
func TraceByID(basePath string, timeout time.Duration, fs *flag.FlagSet) {
	basePath = path.Join(basePath, "trace-by-id")
	err := os.MkdirAll(basePath, 0o755)
	if err != nil {
		panic(err)
	}
	stopCh := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		close(stopCh)
	}()
	collect(basePath, func() ([]float64, error) {
		d, err := traceByID(api.QueryOrderByStartTime, fs)
		if err != nil {
			return nil, err
		}
		return []float64{d.Seconds()}, nil
	}, 500*time.Millisecond, stopCh)
	analyze([]string{"result"}, basePath)
}

const (
	defaultPageSize = 15
)

func traceList(order api.QueryOrder, fs *flag.FlagSet) (time.Duration, []*api.BasicTrace, error) {
	ctx := cli.NewContext(cli.NewApp(), fs, nil)
	duration := api.Duration{
		Start: time.Now().Add(-30 * time.Minute).Format(utils.StepFormats[api.StepMinute]),
		End:   time.Now().Format(utils.StepFormats[api.StepMinute]),
		Step:  api.StepMinute,
	}
	pageNum := 1
	serviceID := ctx.String("service-id")
	condition := &api.TraceQueryCondition{
		ServiceID:     &serviceID,
		QueryDuration: &duration,
		QueryOrder:    order,
		TraceState:    api.TraceStateAll,
		Paging: &api.Pagination{
			PageNum:  &pageNum,
			PageSize: defaultPageSize,
		},
	}
	start := time.Now()
	result, err := trace.Traces(ctx, condition)
	elapsed := time.Since(start)
	if err != nil {
		return 0, nil, err
	}
	if len(result.Traces) < 1 {
		return 0, nil, fmt.Errorf("no result")
	}
	return elapsed, result.Traces, nil
}

func traceByID(order api.QueryOrder, fs *flag.FlagSet) (time.Duration, error) {
	_, traces, err := traceList(order, fs)
	if err != nil {
		return 0, err
	}
	if len(traces) < 1 {
		return 0, fmt.Errorf("no result")
	}
	start := time.Now()
	numTraces := int64(0)
	for _, bt := range traces {
		for _, id := range bt.TraceIds {
			ctx := cli.NewContext(cli.NewApp(), fs, nil)
			t, err := trace.Trace(ctx, id)
			if err != nil {
				return 0, err
			}
			if len(t.Spans) < 1 {
				return 0, fmt.Errorf("no result span")
			}
			numTraces++
		}
	}
	elapsed := time.Since(start)
	return time.Duration(int64(elapsed) / numTraces), nil
}
