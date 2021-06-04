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

package logical_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/logical"
	"github.com/apache/skywalking-banyandb/pkg/types"
)

func TestExpr_KeyRef_Stringer(t *testing.T) {
	keyRef := logical.NewFieldRef("duration")
	assert.Equal(t, keyRef.String(), "#duration")
}

func TestExpr_KeyRef_ToField(t *testing.T) {
	ctrl := gomock.NewController(t)

	keyRef := logical.NewFieldRef("duration")
	plan := logical.NewMockPlan(ctrl)
	schema := types.NewMockSchema(ctrl)
	plan.EXPECT().Schema().Return(schema, nil)
	schema.EXPECT().GetFields().Return([]types.Field{
		types.NewField("duration", types.INT64),
		types.NewField("serviceName", types.STRING),
		types.NewField("spanID", types.STRING),
	})
	refField, err := keyRef.ToField(plan)
	assert.NoError(t, err)
	assert.Equal(t, refField, types.NewField("duration", types.INT64))
}

func TestExpr_KeyRef_ToField_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)

	keyRef := logical.NewFieldRef("traceID")
	plan := logical.NewMockPlan(ctrl)
	schema := types.NewMockSchema(ctrl)
	plan.EXPECT().Schema().Return(schema, nil)
	schema.EXPECT().GetFields().Return([]types.Field{
		types.NewField("duration", types.INT64),
		types.NewField("serviceName", types.STRING),
		types.NewField("spanID", types.STRING),
	})
	_, err := keyRef.ToField(plan)
	assert.ErrorIs(t, err, logical.NoSuchField)
}
