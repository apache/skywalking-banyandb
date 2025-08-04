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

// Package snapshot provides the snapshot backup and restore functionality.
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Conn connects to the gRPC server and executes the given function.
func Conn[T any](gRPCAddr string, enableTLS, insecure bool, cert string, delFn func(conn *grpc.ClientConn) (T, error)) (T, error) {
	opts, err := grpchelper.SecureOptions(nil, enableTLS, insecure, cert)
	if err != nil {
		var zero T
		return zero, err
	}
	connection, err := grpchelper.Conn(gRPCAddr, 10*time.Second, opts...)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}
	defer connection.Close()
	return delFn(connection)
}

// Get retrieves the snapshots from the gRPC server.
func Get(gRPCAddr string, enableTLS, insecure bool, cert string, groups ...*databasev1.SnapshotRequest_Group) ([]*databasev1.Snapshot, error) {
	return Conn(gRPCAddr, enableTLS, insecure, cert, func(conn *grpc.ClientConn) ([]*databasev1.Snapshot, error) {
		ctx := context.Background()
		client := databasev1.NewSnapshotServiceClient(conn)
		snapshotResp, err := client.Snapshot(ctx, &databasev1.SnapshotRequest{Groups: groups})
		if err != nil {
			return nil, fmt.Errorf("failed to request snapshot: %w", err)
		}
		return snapshotResp.Snapshots, nil
	})
}

// Dir returns the directory path of the snapshot.
func Dir(snapshot *databasev1.Snapshot, streamRoot, measureRoot, propertyRoot string) (string, error) {
	var baseDir string
	switch snapshot.Catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		baseDir = LocalDir(streamRoot, snapshot.Catalog)
	case commonv1.Catalog_CATALOG_MEASURE:
		baseDir = LocalDir(measureRoot, snapshot.Catalog)
	case commonv1.Catalog_CATALOG_PROPERTY:
		baseDir = LocalDir(propertyRoot, snapshot.Catalog)
	default:
		return "", errors.New("unknown catalog type")
	}
	return filepath.Join(baseDir, storage.SnapshotsDir, snapshot.Name), nil
}

// LocalDir returns the local directory path of the snapshot.
func LocalDir(rootPath string, catalog commonv1.Catalog) string {
	return filepath.Join(rootPath, CatalogName(catalog))
}

// CatalogName returns the name of the catalog.
func CatalogName(catalog commonv1.Catalog) string {
	switch catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		return "stream"
	case commonv1.Catalog_CATALOG_MEASURE:
		return "measure"
	case commonv1.Catalog_CATALOG_PROPERTY:
		return "property"
	default:
		logger.Panicf("unknown catalog type: %v", catalog)
		return ""
	}
}
