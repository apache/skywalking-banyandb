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
	"sort"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const (
	testPropertyGroup = "test-group"
	testPropertyName  = "test-name"
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

			dataDir, dataDeferFunc, err := test.NewSpace()
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, dataDeferFunc)
			snapshotDir, snapshotDeferFunc, err := test.NewSpace()
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, snapshotDeferFunc)
			db, err := openDB(context.Background(), dataDir, 3*time.Second, tt.expireDeletionTime, 32, observability.BypassRegistry, fs.NewLocalFileSystem(),
				true, snapshotDir, "@every 10m", time.Second*10, "* 2 * * *", nil, nil, nil)
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
				properties = append(properties,
					generateProperty(fmt.Sprintf("test-id%d", i), unix, i))
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

func TestRepair(t *testing.T) {
	now := time.Now()
	tests := []struct {
		beforeApply func() []*propertyv1.Property
		repair      func(ctx context.Context, s *shard) (bool, *queryProperty, error)
		verify      func(t *testing.T, ctx context.Context, s *shard) error
		name        string
	}{
		{
			name: "repair normal with no properties",
			repair: func(ctx context.Context, s *shard) (bool, *queryProperty, error) {
				property := generateProperty(fmt.Sprintf("test-id%d", 0), now.UnixNano(), 0)
				return s.repair(ctx, GetPropertyID(property), property, 0)
			},
			verify: func(t *testing.T, ctx context.Context, s *shard) error {
				resp := queryShard(ctx, t, s, "")
				if len(resp) != 1 {
					t.Fatal(fmt.Errorf("expect 1 property, got %d", len(resp)))
				}
				verifyDeleteTime(t, resp[0], false)
				return nil
			},
		},
		{
			name: "repair deleted with no properties",
			repair: func(ctx context.Context, s *shard) (bool, *queryProperty, error) {
				property := generateProperty(fmt.Sprintf("test-id%d", 0), now.UnixNano(), 0)
				return s.repair(ctx, GetPropertyID(property), property, now.UnixNano())
			},
			verify: func(t *testing.T, ctx context.Context, s *shard) error {
				resp := queryShard(ctx, t, s, "")
				if len(resp) != 1 {
					t.Fatal(fmt.Errorf("expect 1 property, got %d", len(resp)))
				}
				verifyDeleteTime(t, resp[0], true)
				return nil
			},
		},
		{
			name: "repair new normal property with existing properties",
			beforeApply: func() (res []*propertyv1.Property) {
				for i := 1; i <= 3; i++ {
					res = append(res, generateProperty(fmt.Sprintf("test-id%d", i), now.UnixNano()-1000+int64(i), i))
				}
				return
			},
			repair: func(ctx context.Context, s *shard) (bool, *queryProperty, error) {
				property := generateProperty("test-id3", now.UnixNano(), 1000)
				return s.repair(ctx, GetPropertyID(property), property, 0)
			},
			verify: func(t *testing.T, ctx context.Context, s *shard) error {
				resp := queryShard(ctx, t, s, "test-id3")
				if len(resp) != 2 {
					t.Fatal(fmt.Errorf("expect 2 properties, got %d", len(resp)))
				}
				sort.Sort(queryPropertySlice(resp))
				verifyDeleteTime(t, resp[1], false)
				verifyTagIntValue(t, unmarshalProperty(t, resp[1].source), 1, 1000)
				return nil
			},
		},
		{
			name: "repair deleted with existing properties",
			beforeApply: func() (res []*propertyv1.Property) {
				return []*propertyv1.Property{
					generateProperty("test-id", now.UnixNano()-100, 0),
				}
			},
			repair: func(ctx context.Context, s *shard) (bool, *queryProperty, error) {
				property := generateProperty("test-id", now.UnixNano(), 1000)
				return s.repair(ctx, GetPropertyID(property), property, now.UnixNano())
			},
			verify: func(t *testing.T, ctx context.Context, s *shard) error {
				resp := queryShard(ctx, t, s, "test-id")
				if len(resp) != 2 {
					t.Fatal(fmt.Errorf("expect 2 property, got %d", len(resp)))
				}
				sort.Sort(queryPropertySlice(resp))
				verifyDeleteTime(t, resp[0], true)
				verifyTagIntValue(t, unmarshalProperty(t, resp[1].source), 1, 1000)
				return nil
			},
		},
		{
			name: "repair a older version property with new versions",
			beforeApply: func() (res []*propertyv1.Property) {
				for i := 1; i <= 3; i++ {
					res = append(res, generateProperty("test-id", now.UnixNano()+int64(i), i))
				}
				return res
			},
			repair: func(ctx context.Context, s *shard) (bool, *queryProperty, error) {
				// Create a property with an older mod revision
				property := generateProperty("test-id", now.UnixNano()-1000, 2000)
				return s.repair(ctx, GetPropertyID(property), property, 0)
			},
			verify: func(t *testing.T, ctx context.Context, s *shard) error {
				resp := queryShard(ctx, t, s, "test-id")
				if len(resp) != 3 {
					t.Fatal(fmt.Errorf("expect 3 properties, got %d", len(resp)))
				}
				sort.Sort(queryPropertySlice(resp))
				// all properties should be kept
				for i := 0; i < 3; i++ {
					verifyDeleteTime(t, resp[i], false)
				}
				return nil
			},
		},
		{
			name: "repair deleted version property with same data",
			beforeApply: func() (res []*propertyv1.Property) {
				return []*propertyv1.Property{
					generateProperty("test-id", now.UnixNano(), 0),
				}
			},
			repair: func(ctx context.Context, s *shard) (bool, *queryProperty, error) {
				// Create a property with the same data but marked as deleted
				property := generateProperty("test-id", now.UnixNano(), 0)
				return s.repair(ctx, GetPropertyID(property), property, now.UnixNano())
			},
			verify: func(t *testing.T, ctx context.Context, s *shard) error {
				resp := queryShard(ctx, t, s, "test-id")
				if len(resp) != 2 {
					t.Fatal(fmt.Errorf("expect 2 properties, got %d", len(resp)))
				}
				sort.Sort(queryPropertySlice(resp))
				verifyDeleteTime(t, resp[0], true)
				verifyDeleteTime(t, resp[1], true)
				return nil
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

			dataDir, dataDeferFunc, err := test.NewSpace()
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, dataDeferFunc)
			snapshotDir, snapshotDeferFunc, err := test.NewSpace()
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, snapshotDeferFunc)
			db, err := openDB(context.Background(), dataDir, 3*time.Second, 1*time.Hour, 32, observability.BypassRegistry, fs.NewLocalFileSystem(),
				true, snapshotDir, "@every 10m", time.Second*10, "* 2 * * *", nil, nil, nil)
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

			if tt.beforeApply != nil {
				properties := tt.beforeApply()
				for _, prop := range properties {
					if err = newShard.update(GetPropertyID(prop), prop); err != nil {
						t.Fatal(err)
					}
				}
			}

			if _, _, err = tt.repair(context.Background(), newShard); err != nil {
				t.Fatal(err)
			}

			if err = tt.verify(t, context.Background(), newShard); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func generateProperty(id string, modReversion int64, val int) *propertyv1.Property {
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group:       testPropertyGroup,
			Name:        testPropertyName,
			ModRevision: modReversion,
		},
		Id: id,
		Tags: []*modelv1.Tag{
			{
				Key: "tag1",
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(val)}},
				},
			},
		},
	}
}

func queryShard(ctx context.Context, t *testing.T, s *shard, id string) []*queryProperty {
	req := &propertyv1.QueryRequest{
		Groups: []string{testPropertyGroup},
	}
	if id != "" {
		req.Ids = []string{id}
	}
	iq, err := inverted.BuildPropertyQuery(req, groupField, entityID)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.search(ctx, iq, 100)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func unmarshalProperty(t *testing.T, source []byte) *propertyv1.Property {
	var p propertyv1.Property
	if err := protojson.Unmarshal(source, &p); err != nil {
		t.Fatal(fmt.Errorf("fail to unmarshal property: %w", err))
	}
	return &p
}

func verifyTagIntValue(t *testing.T, p *propertyv1.Property, count int, vals ...int64) {
	if len(p.Tags) != count {
		t.Fatal(fmt.Errorf("expect %d tags, got %d", count, len(p.Tags)))
	}
	for i, v := range vals {
		if v != p.Tags[i].Value.GetInt().Value {
			t.Fatal("expect tag value to be", v, "but got", p.Tags[i].Value.GetInt().Value)
		}
	}
}

func verifyDeleteTime(t *testing.T, p *queryProperty, deleted bool) {
	if deleted && p.deleteTime <= 0 {
		t.Fatal(fmt.Errorf("expect deleteTime > 0, got %d", p.deleteTime))
	} else if !deleted && p.deleteTime > 0 {
		t.Fatal(fmt.Errorf("expect deleteTime 0, got %d", p.deleteTime))
	}
}
