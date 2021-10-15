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
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"

	googleUUID "github.com/google/uuid"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var (
	_ Stream           = (*etcdSchemaRegistry)(nil)
	_ IndexRuleBinding = (*etcdSchemaRegistry)(nil)
	_ IndexRule        = (*etcdSchemaRegistry)(nil)
	_ Measure          = (*etcdSchemaRegistry)(nil)
	_ Group            = (*etcdSchemaRegistry)(nil)

	ErrEntityNotFound             = errors.New("entity is not found")
	ErrUnexpectedNumberOfEntities = errors.New("unexpected number of entities")
	ErrGroupAlreadyDefined        = errors.New("group is already defined")
	ErrGroupNotDefined            = errors.New("group is not defined or has already been deleted")

	StreamKeyPrefix           = "/stream/"
	IndexRuleBindingKeyPrefix = "/index-rule-binding/"
	IndexRuleKeyPrefix        = "/index-rule/"
	MeasureKeyPrefix          = "/measure/"
	GroupsKeyPrefix           = "/groups/"
)

type RegistryOption func(*etcdSchemaRegistryConfig)

func PreloadSchema() RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.preload = true
	}
}

func UseRandomTempDir() RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.rootDir = RandomTempDir()
	}
}

func RandomTempDir() string {
	return path.Join(os.TempDir(), fmt.Sprintf("banyandb-embed-etcd-%s", googleUUID.New().String()))
}

func RootDir(rootDir string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.rootDir = rootDir
	}
}

func UseListener(client, peer string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.listenerClientURL = client
		config.listenerPeerURL = peer
	}
}

func UseUnixDomain() RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.listenerClientURL, config.listenerPeerURL = RandomUnixDomainListener()
	}
}

func RandomUnixDomainListener() (string, string) {
	i := rand.Uint64()
	return fmt.Sprintf("%s://localhost:%d%06d", "unix", os.Getpid(), i), fmt.Sprintf("%s://localhost:%d%06d", "unix", os.Getpid(), i+1)
}

type etcdSchemaRegistry struct {
	server *embed.Etcd
	kv     clientv3.KV
}

type etcdSchemaRegistryConfig struct {
	// preload internal schema
	preload bool
	// rootDir is the root directory for etcd storage
	rootDir string
	// listenerClientURL is the listener for client
	listenerClientURL string
	// listenerPeerURL is the listener for peer
	listenerPeerURL string
}

func (e *etcdSchemaRegistry) ExistGroup(ctx context.Context, group string) (bool, error) {
	var entity commonv1.Group
	err := e.get(ctx, formatGroupKey(group), &entity)
	if err != nil && !errors.Is(err, ErrEntityNotFound) {
		return false, err
	}
	return entity.GetName() != "" && !entity.Deleted, nil
}

func (e *etcdSchemaRegistry) ListGroup(ctx context.Context) ([]string, error) {
	messages, err := e.listWithPrefix(ctx, GroupsKeyPrefix, func() proto.Message {
		return &commonv1.Group{}
	})
	if err != nil {
		return nil, err
	}
	groups := make([]string, len(messages))
	for i, message := range messages {
		groups[i] = message.(*commonv1.Group).GetName()
	}
	return groups, nil
}

func (e *etcdSchemaRegistry) DeleteGroup(ctx context.Context, group string) (bool, error) {
	exist, err := e.ExistGroup(ctx, group)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, errors.Wrap(ErrGroupNotDefined, group)
	}
	if err := e.update(ctx, formatGroupKey(group), &commonv1.Group{
		Name:    group,
		Deleted: true,
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (e *etcdSchemaRegistry) CreateGroup(ctx context.Context, group string) error {
	exist, err := e.ExistGroup(ctx, group)
	if err != nil {
		return err
	}
	if exist {
		return errors.Wrap(ErrGroupAlreadyDefined, group)
	}
	return e.update(ctx, formatGroupKey(group), &commonv1.Group{
		Name:    group,
		Deleted: false,
	})
}

func (e *etcdSchemaRegistry) GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error) {
	var entity databasev1.Measure
	if err := e.get(ctx, formatMeasureKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListMeasure(ctx context.Context, opt ListOpt) ([]*databasev1.Measure, error) {
	keyPrefix := MeasureKeyPrefix
	if opt.Group != "" {
		keyPrefix += opt.Group + "/"
	}
	messages, err := e.listWithPrefix(ctx, keyPrefix, func() proto.Message {
		return &databasev1.Measure{}
	})
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Measure, len(messages))
	for i, message := range messages {
		entities[i] = message.(*databasev1.Measure)
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) UpdateMeasure(ctx context.Context, measure *databasev1.Measure) error {
	groupExist, err := e.ExistGroup(ctx, measure.GetMetadata().GetGroup())
	if err != nil {
		return err
	}
	if !groupExist {
		return errors.Wrap(ErrGroupNotDefined, measure.GetMetadata().GetGroup())
	}
	return e.update(ctx, formatMeasureKey(measure.GetMetadata()), measure)
}

func (e *etcdSchemaRegistry) DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, formatMeasureKey(metadata))
}

func (e *etcdSchemaRegistry) GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error) {
	var entity databasev1.Stream
	if err := e.get(ctx, formatSteamKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListStream(ctx context.Context, opt ListOpt) ([]*databasev1.Stream, error) {
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

func (e *etcdSchemaRegistry) UpdateStream(ctx context.Context, stream *databasev1.Stream) error {
	groupExist, err := e.ExistGroup(ctx, stream.GetMetadata().GetGroup())
	if err != nil {
		return err
	}
	if !groupExist {
		return errors.Wrap(ErrGroupNotDefined, stream.GetMetadata().GetGroup())
	}
	return e.update(ctx, formatSteamKey(stream.GetMetadata()), stream)
}

func (e *etcdSchemaRegistry) DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, formatSteamKey(metadata))
}

func (e *etcdSchemaRegistry) GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error) {
	var indexRuleBinding databasev1.IndexRuleBinding
	if err := e.get(ctx, formatIndexRuleBindingKey(metadata), &indexRuleBinding); err != nil {
		return nil, err
	}
	return &indexRuleBinding, nil
}

func (e *etcdSchemaRegistry) ListIndexRuleBinding(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRuleBinding, error) {
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

func (e *etcdSchemaRegistry) UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	groupExist, err := e.ExistGroup(ctx, indexRuleBinding.GetMetadata().GetGroup())
	if err != nil {
		return err
	}
	if !groupExist {
		return errors.Wrap(ErrGroupNotDefined, indexRuleBinding.GetMetadata().GetGroup())
	}
	return e.update(ctx, formatIndexRuleBindingKey(indexRuleBinding.GetMetadata()), indexRuleBinding)
}

func (e *etcdSchemaRegistry) DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, formatIndexRuleBindingKey(metadata))
}

func (e *etcdSchemaRegistry) GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error) {
	var entity databasev1.IndexRule
	if err := e.get(ctx, formatIndexRuleKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListIndexRule(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRule, error) {
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

func (e *etcdSchemaRegistry) UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	groupExist, err := e.ExistGroup(ctx, indexRule.GetMetadata().GetGroup())
	if err != nil {
		return err
	}
	if !groupExist {
		return errors.Wrap(ErrGroupNotDefined, indexRule.GetMetadata().GetGroup())
	}
	return e.update(ctx, formatIndexRuleKey(indexRule.GetMetadata()), indexRule)
}

func (e *etcdSchemaRegistry) DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, formatIndexRuleKey(metadata))
}

func (e *etcdSchemaRegistry) preload() error {
	if err := e.CreateGroup(context.TODO(), "default"); err != nil {
		return err
	}

	s := &databasev1.Stream{}
	if err := protojson.Unmarshal([]byte(streamJSON), s); err != nil {
		return err
	}
	err := e.UpdateStream(context.Background(), s)
	if err != nil {
		return err
	}

	indexRuleBinding := &databasev1.IndexRuleBinding{}
	if err = protojson.Unmarshal([]byte(indexRuleBindingJSON), indexRuleBinding); err != nil {
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

func (e *etcdSchemaRegistry) Close() error {
	e.server.Close()
	return nil
}

func NewEtcdSchemaRegistry(options ...RegistryOption) (Registry, error) {
	registryConfig := &etcdSchemaRegistryConfig{
		rootDir:           os.TempDir(),
		listenerClientURL: embed.DefaultListenClientURLs,
		listenerPeerURL:   embed.DefaultListenPeerURLs,
	}
	for _, opt := range options {
		opt(registryConfig)
	}
	// TODO: allow use cluster setting
	embedConfig := newStandaloneEtcdConfig(registryConfig)
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
	reg := &etcdSchemaRegistry{
		server: e,
		kv:     kvClient,
	}
	if registryConfig.preload {
		err := reg.preload()
		if err != nil {
			return nil, err
		}
	}
	return reg, nil
}

func (e *etcdSchemaRegistry) get(ctx context.Context, key string, message proto.Message) error {
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

func (e *etcdSchemaRegistry) update(ctx context.Context, key string, message proto.Message) error {
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

func (e *etcdSchemaRegistry) listWithPrefix(ctx context.Context, prefix string, factory func() proto.Message) ([]proto.Message, error) {
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

func (e *etcdSchemaRegistry) delete(ctx context.Context, key string) (bool, error) {
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

func formatMeasureKey(metadata *commonv1.Metadata) string {
	return formatKey(MeasureKeyPrefix, metadata)
}

func formatKey(prefix string, metadata *commonv1.Metadata) string {
	return prefix + metadata.GetGroup() + "/" + metadata.GetName()
}

func formatGroupKey(group string) string {
	return GroupsKeyPrefix + group
}

func incrementLastByte(key string) string {
	bb := []byte(key)
	bb[len(bb)-1]++
	return string(bb)
}

func newStandaloneEtcdConfig(config *etcdSchemaRegistryConfig) *embed.Config {
	cfg := embed.NewConfig()
	// TODO: allow user to set path
	cfg.Dir = filepath.Join(config.rootDir, "embed-etcd")
	cURL, _ := url.Parse(config.listenerClientURL)
	pURL, _ := url.Parse(config.listenerPeerURL)

	cfg.ClusterState = "new"
	cfg.LCUrls, cfg.ACUrls = []url.URL{*cURL}, []url.URL{*cURL}
	cfg.LPUrls, cfg.APUrls = []url.URL{*pURL}, []url.URL{*pURL}
	cfg.InitialCluster = ",default=" + pURL.String()
	return cfg
}
