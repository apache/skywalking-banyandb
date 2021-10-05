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

package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

func Test_IndexRule(t *testing.T) {
	ast := assert.New(t)
	is := require.New(t)
	ir, err := NewIndexRule()
	is.NoError(err)
	r, err := ir.GetIndexRule(context.Background(), &commonv1.Metadata{Name: "duration", Group: "default"})
	is.NoError(err)
	ast.NotNil(r)
	ast.Equal("duration", r.Metadata.Name)
	is.NoError(err)
	rules, err := ir.ListIndexRule(context.Background(), ListOpt{})
	is.NoError(err)
	ast.NotEmpty(rules)
}

func Test_Stream(t *testing.T) {
	ast := assert.New(t)
	is := require.New(t)
	s, err := NewStream()
	is.NoError(err)
	r, err := s.GetStream(context.Background(), &commonv1.Metadata{Name: "sw", Group: "default"})
	is.NoError(err)
	ast.NotNil(r)
	ast.Equal("sw", r.Metadata.Name)
	is.NoError(err)
	rules, err := s.ListStream(context.Background(), ListOpt{})
	is.NoError(err)
	ast.NotEmpty(rules)
}

func Test_IndexRuleBinding(t *testing.T) {
	ast := assert.New(t)
	is := require.New(t)
	irb, err := NewIndexRuleBinding()
	is.NoError(err)
	r, err := irb.GetIndexRuleBinding(context.Background(), &commonv1.Metadata{Name: "sw-index-rule-binding", Group: "default"})
	is.NoError(err)
	ast.NotNil(r)
	ast.Equal("sw-index-rule-binding", r.Metadata.Name)
	is.NoError(err)
	rules, err := irb.ListIndexRuleBinding(context.Background(), ListOpt{})
	is.NoError(err)
	ast.NotEmpty(rules)
}
