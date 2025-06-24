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
	propertyCount := 6
	tests := []struct {
		verify             func(t *testing.T, queriedProperties []*queryProperty)
		name               string
		expireDeletionTime time.Duration
		deleteTime         int64
	}{
		{
			name:               "delete expired properties from db",
			expireDeletionTime: 1 * time.Second,
			deleteTime:         time.Now().Add(-3 * time.Second).UnixNano(),
			verify: func(t *testing.T, queriedProperties []*queryProperty) {
				// the count of properties in shard should be less than the total properties count
				if len(queriedProperties) >= propertyCount {
					t.Fatal(fmt.Errorf("expect only %d results, got %d", propertyCount, len(queriedProperties)))
				}
				for _, p := range queriedProperties {
					// and the property should be marked as deleteTime
					if p.deleteTime <= 0 {
						t.Fatal(fmt.Errorf("expect all results to be deleted"))
					}
				}
			},
		},
		{
			name:               "deleted properties still exist in db",
			expireDeletionTime: time.Hour,
			deleteTime:         time.Now().UnixNano(),
			verify: func(t *testing.T, queriedProperties []*queryProperty) {
				if len(queriedProperties) != propertyCount {
					t.Fatal(fmt.Errorf("expect %d results, got %d", propertyCount, len(queriedProperties)))
				}
				for _, p := range queriedProperties {
					// and the property should be marked as deleteTime
					if p.deleteTime <= 0 {
						t.Fatal(fmt.Errorf("expect all results to be deleted"))
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			db, err := openDB(context.Background(), dir, 3*time.Second, tt.expireDeletionTime, observability.BypassRegistry, fs.NewLocalFileSystem())
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

			properties := make([]*propertyv1.Property, 0, propertyCount)
			unix := time.Now().Unix()
			unix -= 10
			for i := 0; i < propertyCount; i++ {
				property := &propertyv1.Property{
					Metadata: &commonv1.Metadata{
						Group:       "test-group",
						Name:        "test-name",
						ModRevision: unix,
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

			resp, err := db.query(context.Background(), &propertyv1.QueryRequest{Groups: []string{"test-group"}})
			if err != nil {
				t.Fatal(err)
			}
			if len(resp) != propertyCount {
				t.Fatal(fmt.Errorf("expect %d results before delete, got %d", propertyCount, len(resp)))
			}

			// delete current property
			for _, p := range properties {
				if err = newShard.deleteFromTime(context.Background(), [][]byte{GetPropertyID(p)}, tt.deleteTime); err != nil {
					t.Fatal(err)
				}
			}

			// waiting for the merge phase to complete
			time.Sleep(time.Second * 1)

			// check if the property is deleteTime from shard including deleteTime, should be no document (delete by merge phase)
			resp, err = db.query(context.Background(), &propertyv1.QueryRequest{Groups: []string{"test-group"}})
			if err != nil {
				t.Fatal(err)
			}
			tt.verify(t, resp)
		})
	}
}
