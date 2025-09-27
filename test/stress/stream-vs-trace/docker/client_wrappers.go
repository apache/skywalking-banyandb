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

package docker

import (
	"google.golang.org/grpc"
	
	streamvstrace "github.com/apache/skywalking-banyandb/test/stress/stream-vs-trace"
)

// DockerStreamClient wraps StreamClient with connection exposed
type DockerStreamClient struct {
	*streamvstrace.StreamClient
	conn *grpc.ClientConn
}

// NewDockerStreamClient creates a new DockerStreamClient instance
func NewDockerStreamClient(conn *grpc.ClientConn) *DockerStreamClient {
	return &DockerStreamClient{
		StreamClient: streamvstrace.NewStreamClient(conn),
		conn:         conn,
	}
}

// DockerTraceClient wraps TraceClient with connection exposed
type DockerTraceClient struct {
	*streamvstrace.TraceClient
	conn *grpc.ClientConn
}

// NewDockerTraceClient creates a new DockerTraceClient instance
func NewDockerTraceClient(conn *grpc.ClientConn) *DockerTraceClient {
	return &DockerTraceClient{
		TraceClient: streamvstrace.NewTraceClient(conn),
		conn:        conn,
	}
}