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

package property

import (
	"context"
	"fmt"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestMergeDeleted(t *testing.T) {
	var defers []func()
	defer func() {
		for _, f := range defers {
			f()
		}
	}()

	dir, deferFunc, err := test.NewSpace()
	if err != nil {
		t.Fatal(err)
	}
	defers = append(defers, deferFunc)
	db, err := openDB(context.Background(), dir, 3*time.Second, observability.BypassRegistry, fs.NewLocalFileSystem())
	if err != nil {
		t.Fatal(err)
	}
	defers = append(defers, func() {
		_ = db.close()
	})

	newShard, err := db.loadShard(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}

	propertyCount := 6

	properties := make([]*propertyv1.Property, 0, propertyCount)
	for i := 0; i < propertyCount; i++ {
		property := &propertyv1.Property{
			Metadata: &commonv1.Metadata{
				Group:       "test-group",
				Name:        "test-name",
				ModRevision: time.Now().Unix(),
			},
			Id: fmt.Sprintf("test-id%d", i),
			Tags: []*modelv1.Tag{
				{Key: "tag1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(i)}}}},
			},
		}
		properties = append(properties, property)
	}

	// apply the property
	for _, p := range properties {
		if err = newShard.update(GetPropertyID(p), p); err != nil {
			t.Fatal(err)
		}
	}

	// delete current property
	for _, p := range properties {
		if err = newShard.delete(context.Background(), [][]byte{GetPropertyID(p)}); err != nil {
			t.Fatal(err)
		}
	}

	// check if the property is deleted from shard including deleted, should be no document (delete by merge phase)
	resp, err := db.query(context.Background(), &propertyv1.QueryRequest{Groups: []string{"test-group"}})
	if err != nil {
		t.Fatal(err)
	}

	if len(resp) != 0 {
		t.Fail()
	}
}
