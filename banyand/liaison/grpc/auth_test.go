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
	"log"
	"net"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var (
	cfg = &auth.Config{
		Users: []auth.User{
			{
				Username: "admin",
				Password: "password",
			},
		},
	}
	errUsernameNotProvided       = status.Errorf(codes.Unauthenticated, "username is not provided correctly")
	errInvalidUsernameOrPassword = status.Errorf(codes.Unauthenticated, "invalid username or password")
)

func TestAuthInterceptor(t *testing.T) {
	ports, err := test.AllocateFreePorts(1)
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "localhost:"+strconv.Itoa(ports[0]))
	if err != nil {
		t.Fatal(err)
	}
	preCfg := auth.Cfg
	auth.Cfg = cfg
	server := grpc.NewServer(grpc.UnaryInterceptor(authInterceptor))
	defer server.GracefulStop()

	databasev1.RegisterSnapshotServiceServer(server, &databasev1.UnimplementedSnapshotServiceServer{})

	go func() {
		if err = server.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := databasev1.NewSnapshotServiceClient(conn)

	// no username and password.
	ctx := context.Background()
	_, err = client.Snapshot(ctx, new(databasev1.SnapshotRequest))
	if errors.Is(err, errInvalidUsernameOrPassword) {
		t.Errorf("Expect error invalid username or password, but got %v", err)
	}

	// invalid password.
	md := metadata.Pairs("username", "admin", "password", "wrong")
	ctx = metadata.NewOutgoingContext(ctx, md)
	_, err = client.Snapshot(ctx, new(databasev1.SnapshotRequest))
	if errors.Is(err, errUsernameNotProvided) {
		t.Errorf("Expect error invalid username or password, but got %v", err)
	}

	// valid password.
	md = metadata.Pairs("username", "admin", "password", "password")
	ctx = metadata.NewOutgoingContext(ctx, md)
	_, err = client.Snapshot(ctx, new(databasev1.SnapshotRequest))
	if errors.Is(err, errInvalidUsernameOrPassword) || errors.Is(err, errUsernameNotProvided) {
		t.Errorf("Expect no error, but got %v", err)
	}

	auth.Cfg = preCfg
}
