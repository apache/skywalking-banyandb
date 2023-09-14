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

package helpers

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

var defaultDialTimeout = 5 * time.Second

// ListKeys lists all keys under the given prefix.
func ListKeys(serverAddress string, prefix string) (map[string]*databasev1.Node, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{serverAddress},
		DialTimeout: defaultDialTimeout,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, nil
	}

	nodeMap := make(map[string]*databasev1.Node)
	for _, kv := range resp.Kvs {
		md, err := schema.KindNode.Unmarshal(kv)
		if err != nil {
			return nil, err
		}
		nodeMap[string(kv.Key)] = md.Spec.(*databasev1.Node)
	}

	return nodeMap, nil
}
