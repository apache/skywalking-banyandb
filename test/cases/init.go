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

// Package cases provides some tools to access test data.
package cases

import (
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

// Initialize test data.
func Initialize(addr string, now time.Time) {
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer conn.Close()
	interval := 500 * time.Millisecond
	// stream
	casesstreamdata.Write(conn, "sw", now, interval)
	casesstreamdata.Write(conn, "duplicated", now, 0)
	casesstreamdata.WriteToGroup(conn, "sw", "updated", "sw_updated", now.Add(time.Minute), interval)
	// measure
	interval = time.Minute
	casesmeasuredata.Write(conn, "service_traffic", "index_mode", "service_traffic_data_old.json", now.AddDate(0, 0, -2), interval)
	casesmeasuredata.Write(conn, "service_traffic", "index_mode", "service_traffic_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_traffic", "sw_metric", "service_instance_traffic_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "instance_clr_cpu_minute", "sw_metric", "instance_clr_cpu_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_cpm_minute", "sw_metric", "service_instance_cpm_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_cpm_minute", "sw_metric", "service_instance_cpm_minute_data1.json", now.Add(10*time.Second), interval)
	casesmeasuredata.Write(conn, "service_instance_cpm_minute", "sw_metric", "service_instance_cpm_minute_data2.json", now.Add(10*time.Minute), interval)
	casesmeasuredata.Write(conn, "service_instance_endpoint_cpm_minute", "sw_metric", "service_instance_endpoint_cpm_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_endpoint_cpm_minute", "sw_metric", "service_instance_endpoint_cpm_minute_data1.json", now.Add(10*time.Second), interval)
	casesmeasuredata.Write(conn, "service_instance_endpoint_cpm_minute", "sw_metric", "service_instance_endpoint_cpm_minute_data2.json", now.Add(10*time.Minute), interval)
	casesmeasuredata.Write(conn, "service_latency_minute", "sw_metric", "service_latency_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_latency_minute", "sw_metric", "service_instance_latency_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_latency_minute", "sw_metric", "service_instance_latency_minute_data1.json", now.Add(1*time.Minute), interval)
	casesmeasuredata.Write(conn, "endpoint_traffic", "sw_metric", "endpoint_traffic.json", now, interval)
	casesmeasuredata.Write(conn, "duplicated", "exception", "duplicated.json", now, 0)
	casesmeasuredata.Write(conn, "service_cpm_minute", "sw_updated", "service_cpm_minute_updated_data.json", now.Add(10*time.Minute), interval)
	time.Sleep(5 * time.Second)
	// trace
	// nolint:gocritic
	// interval = 500 * time.Millisecond
	// casestrace.Write(conn, "sw", now, interval)
}
