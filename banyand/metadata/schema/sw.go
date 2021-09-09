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
	"bytes"
	"context"
	"embed"

	"github.com/golang/protobuf/jsonpb"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
)

var (
	_ Stream    = (*streamRepo)(nil)
	_ IndexRule = (*indexRuleRepo)(nil)

	//go:embed data/index_rules/*.json
	indexRuleStore embed.FS
	//go:embed data/index_rule_binding.json
	indexRuleBindingJSON string
	//go:embed data/stream.json
	streamJSON string
)

type streamRepo struct {
	data *databasev2.Stream
}

func NewStream() (Stream, error) {
	stream := &databasev2.Stream{}
	if err := jsonpb.UnmarshalString(streamJSON, stream); err != nil {
		return nil, err
	}
	return &streamRepo{
		data: stream,
	}, nil
}

func (l *streamRepo) Get(_ context.Context, _ *commonv2.Metadata) (*databasev2.Stream, error) {
	return l.data, nil
}

func (l *streamRepo) List(ctx context.Context, _ ListOpt) ([]*databasev2.Stream, error) {
	s, err := l.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	return []*databasev2.Stream{s}, nil
}

type indexRuleRepo struct {
	store embed.FS
}

func NewIndexRule() (IndexRule, error) {
	return &indexRuleRepo{
		store: indexRuleStore,
	}, nil
}

func (i *indexRuleRepo) Get(_ context.Context, metadata *commonv2.Metadata) (*databasev2.IndexRule, error) {
	bb, err := i.store.ReadFile("data/index_rules/" + metadata.Name + ".json")
	if err != nil {
		return nil, err
	}
	indexRule := &databasev2.IndexRule{}
	err = jsonpb.Unmarshal(bytes.NewReader(bb), indexRule)
	if err != nil {
		return nil, err
	}
	return indexRule, nil
}

func (i *indexRuleRepo) List(ctx context.Context, _ ListOpt) ([]*databasev2.IndexRule, error) {
	t, err := i.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	return []*databasev2.IndexRule{t}, nil
}

type indexRuleBindingRepo struct {
	data *databasev2.IndexRuleBinding
}

func NewIndexRuleBinding() (IndexRuleBinding, error) {
	indexRuleBinding := &databasev2.IndexRuleBinding{}
	if err := jsonpb.UnmarshalString(indexRuleBindingJSON, indexRuleBinding); err != nil {
		return nil, err
	}
	return &indexRuleBindingRepo{
		data: indexRuleBinding,
	}, nil
}

func (i *indexRuleBindingRepo) Get(_ context.Context, _ *commonv2.Metadata) (*databasev2.IndexRuleBinding, error) {
	return i.data, nil
}

func (i *indexRuleBindingRepo) List(ctx context.Context, _ ListOpt) ([]*databasev2.IndexRuleBinding, error) {
	t, err := i.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	return []*databasev2.IndexRuleBinding{t}, nil
}
