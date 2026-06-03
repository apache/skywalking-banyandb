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

package partition_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// TestTraceShardID verifies hash-based shard routing is deterministic and
// guards against a zero shardNum (which would panic with integer
// divide-by-zero in production write paths).
func TestTraceShardID(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		s1 := partition.TraceShardID("trace-abc", 8)
		s2 := partition.TraceShardID("trace-abc", 8)
		assert.Equal(t, s1, s2)
		assert.True(t, uint32(s1) < 8)
	})
	t.Run("spread across shards", func(t *testing.T) {
		seen := make(map[common.ShardID]struct{})
		for i := 0; i < 64; i++ {
			seen[partition.TraceShardID("trace-"+string(rune('A'+i)), 4)] = struct{}{}
		}
		assert.GreaterOrEqual(t, len(seen), 2, "hash must spread across multiple shards")
	})
	t.Run("zero shardNum returns 0 (no panic)", func(t *testing.T) {
		assert.Equal(t, common.ShardID(0), partition.TraceShardID("any-trace-id", 0))
	})
}

// fixedRouter satisfies partition.Router and returns predetermined results,
// letting the test assert that ApplyLocators wires the right router into
// the right slot and that the shardingKey override fires only when supplied.
type fixedRouter struct {
	err          error
	entityValues pbv1.EntityValues
	calls        int
	shardID      common.ShardID
}

func (f *fixedRouter) Locate(_ string, _ []*modelv1.TagFamilyForWrite, _ uint32) (pbv1.EntityValues, common.ShardID, error) {
	f.calls++
	return f.entityValues, f.shardID, f.err
}

// TestApplyLocators_EntityOnly checks the no-shardingKey path: entityRouter's
// output is returned unchanged.
func TestApplyLocators_EntityOnly(t *testing.T) {
	entity := &fixedRouter{
		entityValues: pbv1.EntityValues{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "subject"}}}},
		shardID:      common.ShardID(3),
	}
	values, shardID, err := partition.ApplyLocators("svc", nil, entity, nil, 8)
	require.NoError(t, err)
	assert.Equal(t, common.ShardID(3), shardID)
	require.Len(t, values, 1)
	assert.Equal(t, 1, entity.calls)
}

// TestApplyLocators_ShardingKeyOverride checks that a non-nil shardingKey
// router overrides the entity-based shardID while entityValues stay from the
// entity router.
func TestApplyLocators_ShardingKeyOverride(t *testing.T) {
	entity := &fixedRouter{
		entityValues: pbv1.EntityValues{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "subject"}}}},
		shardID:      common.ShardID(3),
	}
	sk := &fixedRouter{shardID: common.ShardID(5)}
	values, shardID, err := partition.ApplyLocators("svc", nil, entity, sk, 8)
	require.NoError(t, err)
	assert.Equal(t, common.ShardID(5), shardID, "shardingKey must override entity-based shardID")
	require.Len(t, values, 1)
	assert.Equal(t, 1, entity.calls)
	assert.Equal(t, 1, sk.calls)
}

// TestApplyLocators_EntityError propagates errors from the entity router and
// does not consult the shardingKey router.
func TestApplyLocators_EntityError(t *testing.T) {
	entity := &fixedRouter{err: errors.New("entity locate failed")}
	sk := &fixedRouter{shardID: common.ShardID(5)}
	_, _, err := partition.ApplyLocators("svc", nil, entity, sk, 8)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entity locate failed")
	assert.Equal(t, 0, sk.calls, "shardingKey router must not run after entity error")
}

// TestApplyLocators_ShardingKeyError propagates errors from the shardingKey
// router even though the entity router succeeded.
func TestApplyLocators_ShardingKeyError(t *testing.T) {
	entity := &fixedRouter{
		entityValues: pbv1.EntityValues{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "subject"}}}},
		shardID:      common.ShardID(3),
	}
	sk := &fixedRouter{err: errors.New("sharding key locate failed")}
	_, _, err := partition.ApplyLocators("svc", nil, entity, sk, 8)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sharding key locate failed")
}

// TestApplyLocators_LocatorIntegration verifies that partition.Locator (the
// concrete value type used by both the cached entity locator and sharding-key
// locator in production) satisfies the Router interface and produces the
// same shardID as a direct .Locate call.
func TestApplyLocators_LocatorIntegration(t *testing.T) {
	families := []*databasev1.TagFamilySpec{{
		Name: "primary",
		Tags: []*databasev1.TagSpec{
			{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}}
	entity := &databasev1.Entity{TagNames: []string{"service", "region"}}
	entityLocator := partition.NewEntityLocator(families, entity, 1)

	tagFamilies := []*modelv1.TagFamilyForWrite{{
		Tags: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "checkout"}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "us-east-1"}}},
		},
	}}

	wantValues, wantShard, wantErr := entityLocator.Locate("svc", tagFamilies, 8)
	require.NoError(t, wantErr)

	gotValues, gotShard, err := partition.ApplyLocators("svc", tagFamilies, entityLocator, nil, 8)
	require.NoError(t, err)
	assert.Equal(t, wantShard, gotShard)
	require.Equal(t, len(wantValues), len(gotValues))
}
