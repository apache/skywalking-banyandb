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
	"errors"
	"fmt"
	"os"
	"path/filepath"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var (
	_ Stream           = (*EtcdSchemaRegistry)(nil)
	_ IndexRuleBinding = (*EtcdSchemaRegistry)(nil)
	_ IndexRule        = (*EtcdSchemaRegistry)(nil)

	ErrEntityNotFound             = errors.New("entity is not found")
	ErrUnexpectedNumberOfEntities = errors.New("unexpected number of entities")

	StreamKeyPrefix           = "/stream/"
	IndexRuleBindingKeyPrefix = "/index-rule-binding/"
	IndexRuleKeyPrefix        = "/index-rule/"
)

type EtcdSchemaRegistry struct {
	server *embed.Etcd
	kv     clientv3.KV
}

func (e *EtcdSchemaRegistry) GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error) {
	var streamEntity databasev1.Stream
	if err := e.get(ctx, formatSteamKey(metadata), &streamEntity); err != nil {
		return nil, err
	}
	return &streamEntity, nil
}

func (e *EtcdSchemaRegistry) ListStream(ctx context.Context, opt ListOpt) ([]*databasev1.Stream, error) {
	keyPrefix := StreamKeyPrefix
	if opt.Group != "" {
		keyPrefix += opt.Group + "/"
	}
	messages, err := e.listWithPrefix(ctx, keyPrefix, func() proto.Message {
		return &databasev1.Stream{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Stream, len(messages))
	for i, message := range messages {
		entities[i] = message.(*databasev1.Stream)
	}
	return entities, nil
}

func (e *EtcdSchemaRegistry) UpdateStream(ctx context.Context, stream *databasev1.Stream) error {
	return e.update(ctx, formatSteamKey(stream.GetMetadata()), stream)
}

func (e *EtcdSchemaRegistry) DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, formatSteamKey(metadata))
}

func (e *EtcdSchemaRegistry) GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error) {
	var indexRuleBinding databasev1.IndexRuleBinding
	if err := e.get(ctx, formatIndexRuleBindingKey(metadata), &indexRuleBinding); err != nil {
		return nil, err
	}
	return &indexRuleBinding, nil
}

func (e *EtcdSchemaRegistry) ListIndexRuleBinding(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	keyPrefix := IndexRuleBindingKeyPrefix
	if opt.Group != "" {
		keyPrefix += opt.Group + "/"
	}
	messages, err := e.listWithPrefix(ctx, keyPrefix, func() proto.Message {
		return &databasev1.IndexRuleBinding{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.IndexRuleBinding, len(messages))
	for i, message := range messages {
		entities[i] = message.(*databasev1.IndexRuleBinding)
	}
	return entities, nil
}

func (e *EtcdSchemaRegistry) UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	return e.update(ctx, formatIndexRuleBindingKey(indexRuleBinding.GetMetadata()), indexRuleBinding)
}

func (e *EtcdSchemaRegistry) DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, formatIndexRuleBindingKey(metadata))
}

func (e *EtcdSchemaRegistry) GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error) {
	var entity databasev1.IndexRule
	if err := e.get(ctx, formatIndexRuleKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *EtcdSchemaRegistry) ListIndexRule(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRule, error) {
	keyPrefix := IndexRuleKeyPrefix
	if opt.Group != "" {
		keyPrefix += opt.Group + "/"
	}
	messages, err := e.listWithPrefix(ctx, keyPrefix, func() proto.Message {
		return &databasev1.IndexRule{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.IndexRule, len(messages))
	for i, message := range messages {
		entities[i] = message.(*databasev1.IndexRule)
	}
	return entities, nil
}

func (e *EtcdSchemaRegistry) UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	return e.update(ctx, formatIndexRuleKey(indexRule.GetMetadata()), indexRule)
}

func (e *EtcdSchemaRegistry) DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, formatIndexRuleKey(metadata))
}

func (e *EtcdSchemaRegistry) preload() error {
	s := &databasev1.Stream{}
	if err := protojson.Unmarshal([]byte(streamJSON), s); err != nil {
		return err
	}
	err := e.UpdateStream(context.Background(), s)
	if err != nil {
		return err
	}

	indexRuleBinding := &databasev1.IndexRuleBinding{}
	if err := protojson.Unmarshal([]byte(indexRuleBindingJSON), indexRuleBinding); err != nil {
		return err
	}
	err = e.UpdateIndexRuleBinding(context.Background(), indexRuleBinding)
	if err != nil {
		return err
	}

	entries, err := indexRuleStore.ReadDir(indexRuleDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := indexRuleStore.ReadFile(indexRuleDir + "/" + entry.Name())
		if err != nil {
			return err
		}
		var idxRule databasev1.IndexRule
		err = protojson.Unmarshal(data, &idxRule)
		if err != nil {
			return err
		}
		err = e.UpdateIndexRule(context.Background(), &idxRule)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *EtcdSchemaRegistry) Close() error {
	e.server.Close()
	return nil
}

func NewEtcdSchemaRegistry() (*EtcdSchemaRegistry, error) {
	// TODO: allow use cluster setting
	embedConfig := newStandaloneEtcdConfig()
	e, err := embed.StartEtcd(embedConfig)
	if err != nil {
		return nil, err
	}
	if e != nil {
		<-e.Server.ReadyNotify() // wait for e.Server to join the cluster
	}
	client, err := clientv3.NewFromURL(e.Config().ACUrls[0].String())
	if err != nil {
		return nil, err
	}
	kvClient := clientv3.NewKV(client)
	return &EtcdSchemaRegistry{
		server: e,
		kv:     kvClient,
	}, nil
}

func (e *EtcdSchemaRegistry) get(ctx context.Context, key string, message proto.Message) error {
	resp, err := e.kv.Get(ctx, key)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return ErrEntityNotFound
	}
	if resp.Count > 1 {
		return ErrUnexpectedNumberOfEntities
	}
	if err := proto.Unmarshal(resp.Kvs[0].Value, message); err != nil {
		return err
	}
	return nil
}

func (e *EtcdSchemaRegistry) update(ctx context.Context, key string, message proto.Message) error {
	val, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	_, err = e.kv.Put(ctx, key, string(val))
	if err != nil {
		return err
	}
	return nil
}

func (e *EtcdSchemaRegistry) listWithPrefix(ctx context.Context, prefix string, factory func() proto.Message) ([]proto.Message, error) {
	resp, err := e.kv.Get(ctx, prefix, clientv3.WithFromKey(), clientv3.WithRange(incrementLastByte(prefix)))
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, ErrEntityNotFound
	}
	entities := make([]proto.Message, resp.Count)
	for i := int64(0); i < resp.Count; i++ {
		message := factory()
		if err := proto.Unmarshal(resp.Kvs[0].Value, message); err != nil {
			return nil, err
		}
		entities[i] = message
	}
	return entities, nil
}

func (e *EtcdSchemaRegistry) delete(ctx context.Context, key string) (bool, error) {
	resp, err := e.kv.Delete(ctx, key)
	if err != nil {
		return false, err
	}
	return resp.Deleted > 0, nil
}

func formatIndexRuleKey(metadata *commonv1.Metadata) string {
	return formatKey(IndexRuleKeyPrefix, metadata)
}

func formatIndexRuleBindingKey(metadata *commonv1.Metadata) string {
	return formatKey(IndexRuleBindingKeyPrefix, metadata)
}

func formatSteamKey(metadata *commonv1.Metadata) string {
	return formatKey(StreamKeyPrefix, metadata)
}

func formatKey(prefix string, metadata *commonv1.Metadata) string {
	return prefix + metadata.GetGroup() + "/" + metadata.GetName()
}

func incrementLastByte(key string) string {
	bb := []byte(key)
	bb[len(bb)-1] += 1
	return string(bb)
}

func newStandaloneEtcdConfig() *embed.Config {
	cfg := embed.NewConfig()
	//TODO: allow user to set path
	cfg.Dir = filepath.Join(os.TempDir(), fmt.Sprintf("embed-etcd"))
	return cfg
}
