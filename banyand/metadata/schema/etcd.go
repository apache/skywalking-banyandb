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
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	unixDomainSockScheme = "unix"

	GroupsKeyPrefix           = "/groups/"
	GroupMetadataKey          = "/__meta_group__"
	StreamKeyPrefix           = "/streams/"
	IndexRuleBindingKeyPrefix = "/index-rule-bindings/"
	IndexRuleKeyPrefix        = "/index-rules/"
	MeasureKeyPrefix          = "/measures/"
)

type HasMetadata interface {
	GetMetadata() *commonv1.Metadata
	proto.Message
}

type RegistryOption func(*etcdSchemaRegistryConfig)

func RootDir(rootDir string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.rootDir = rootDir
	}
}

func randomUnixDomainListener() (string, string) {
	i := rand.Uint64()
	return fmt.Sprintf("%s://localhost:%d%06d", unixDomainSockScheme, os.Getpid(), i),
		fmt.Sprintf("%s://localhost:%d%06d", unixDomainSockScheme, os.Getpid(), i+1)
}

func UseRandomListener() RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		lc, lp := randomUnixDomainListener()
		config.listenerClientURL = lc
		config.listenerPeerURL = lp
	}
}

type eventHandler struct {
	interestKeys Kind
	handler      EventHandler
}

func (eh *eventHandler) InterestOf(kind Kind) bool {
	return KindMask&kind&eh.interestKeys != 0
}

type etcdSchemaRegistry struct {
	server   *embed.Etcd
	kv       clientv3.KV
	handlers []*eventHandler
}

type etcdSchemaRegistryConfig struct {
	// rootDir is the root directory for etcd storage
	rootDir string
	// listenerClientURL is the listener for client
	listenerClientURL string
	// listenerPeerURL is the listener for peer
	listenerPeerURL string
}

func (e *etcdSchemaRegistry) RegisterHandler(kind Kind, handler EventHandler) {
	e.handlers = append(e.handlers, &eventHandler{
		interestKeys: kind,
		handler:      handler,
	})
}

func (e *etcdSchemaRegistry) notifyUpdate(metadata Metadata) {
	for _, h := range e.handlers {
		if h.InterestOf(metadata.Kind) {
			h.handler.OnAddOrUpdate(metadata)
		}
	}
}

func (e *etcdSchemaRegistry) notifyDelete(metadata Metadata) {
	for _, h := range e.handlers {
		if h.InterestOf(metadata.Kind) {
			h.handler.OnDelete(metadata)
		}
	}
}

func (e *etcdSchemaRegistry) GetGroup(ctx context.Context, group string) (*commonv1.Group, error) {
	var entity commonv1.Group
	err := e.get(ctx, formatGroupKey(group), &entity)
	if err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListGroup(ctx context.Context) ([]string, error) {
	messages, err := e.kv.Get(ctx, GroupsKeyPrefix, clientv3.WithFromKey(), clientv3.WithRange(incrementLastByte(GroupsKeyPrefix)))
	if err != nil {
		return nil, err
	}

	if messages.Count == 0 {
		return []string{}, nil
	}

	var groups []string
	for _, kv := range messages.Kvs {
		// kv.Key = "/groups/" + {group} + "/__meta_info__"
		groupWithSuffix := strings.TrimPrefix(string(kv.Key), GroupsKeyPrefix)
		if strings.HasSuffix(groupWithSuffix, GroupMetadataKey) {
			groups = append(groups, strings.TrimSuffix(groupWithSuffix, GroupMetadataKey))
		}
	}

	return groups, nil
}

func (e *etcdSchemaRegistry) DeleteGroup(ctx context.Context, group string) (bool, error) {
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return false, errors.Wrap(err, group)
	}
	keyPrefix := GroupsKeyPrefix + g.GetName() + "/"
	_, err = e.kv.Delete(ctx, keyPrefix, clientv3.WithRange(incrementLastByte(keyPrefix)))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (e *etcdSchemaRegistry) CreateGroup(ctx context.Context, group string) error {
	_, err := e.GetGroup(ctx, group)
	if err != nil && !errors.Is(err, ErrEntityNotFound) {
		return errors.Wrap(err, group)
	}
	return e.touchGroup(ctx, &commonv1.Group{
		Name: group,
	})
}

func (e *etcdSchemaRegistry) touchGroup(ctx context.Context, g *commonv1.Group) error {
	groupBytes, err := proto.Marshal(g)
	if err != nil {
		return err
	}
	g.UpdatedAt = timestamppb.Now()
	_, err = e.kv.Put(ctx, formatGroupKey(g.GetName()), string(groupBytes))
	return err
}

func (e *etcdSchemaRegistry) GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error) {
	var entity databasev1.Measure
	if err := e.get(ctx, formatMeasureKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListMeasure(ctx context.Context, opt ListOpt) ([]*databasev1.Measure, error) {
	keyPrefixes, err := e.listPrefixesForEntity(ctx, opt, MeasureKeyPrefix)
	if err != nil {
		return nil, err
	}

	var entities []*databasev1.Measure
	for _, keyPrefix := range keyPrefixes {
		messages, err := e.listWithPrefix(ctx, keyPrefix, func() proto.Message {
			return &databasev1.Measure{}
		})
		if err != nil {
			return nil, err
		}
		for _, message := range messages {
			entities = append(entities, message.(*databasev1.Measure))
		}
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) UpdateMeasure(ctx context.Context, measure *databasev1.Measure) error {
	g, err := e.GetGroup(ctx, measure.GetMetadata().GetGroup())
	if err != nil {
		return errors.Wrap(err, measure.GetMetadata().GetGroup())
	}
	return e.update(ctx, g, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindMeasure,
			Group: g.GetName(),
			Name:  measure.GetMetadata().GetName(),
		},
		Spec: measure,
	})
}

func (e *etcdSchemaRegistry) DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	g, err := e.GetGroup(ctx, metadata.GetGroup())
	if err != nil {
		return false, errors.Wrap(err, metadata.GetGroup())
	}
	return e.delete(ctx, g, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindMeasure,
			Group: g.GetName(),
			Name:  metadata.GetName(),
		},
	})
}

func (e *etcdSchemaRegistry) GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error) {
	var entity databasev1.Stream
	if err := e.get(ctx, formatStreamKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListStream(ctx context.Context, opt ListOpt) ([]*databasev1.Stream, error) {
	keyPrefixes, err := e.listPrefixesForEntity(ctx, opt, StreamKeyPrefix)
	if err != nil {
		return nil, err
	}

	var entities []*databasev1.Stream
	for _, keyPrefix := range keyPrefixes {
		messages, err := e.listWithPrefix(ctx, keyPrefix, func() proto.Message {
			return &databasev1.Stream{}
		})
		if err != nil {
			return nil, err
		}

		for _, message := range messages {
			entities = append(entities, message.(*databasev1.Stream))
		}
	}

	return entities, nil
}

func (e *etcdSchemaRegistry) UpdateStream(ctx context.Context, stream *databasev1.Stream) error {
	g, err := e.GetGroup(ctx, stream.GetMetadata().GetGroup())
	if err != nil {
		return errors.Wrap(err, stream.GetMetadata().GetGroup())
	}
	return e.update(ctx, g, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindStream,
			Group: g.GetName(),
			Name:  stream.GetMetadata().GetName(),
		},
		Spec: stream,
	})
}

func (e *etcdSchemaRegistry) DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	g, err := e.GetGroup(ctx, metadata.GetGroup())
	if err != nil {
		return false, errors.Wrap(err, metadata.GetGroup())
	}
	return e.delete(ctx, g, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindStream,
			Group: g.GetName(),
			Name:  metadata.GetName(),
		},
	})
}

func (e *etcdSchemaRegistry) GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error) {
	var indexRuleBinding databasev1.IndexRuleBinding
	if err := e.get(ctx, formatIndexRuleBindingKey(metadata), &indexRuleBinding); err != nil {
		return nil, err
	}
	return &indexRuleBinding, nil
}

func (e *etcdSchemaRegistry) ListIndexRuleBinding(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	keyPrefixes, err := e.listPrefixesForEntity(ctx, opt, IndexRuleBindingKeyPrefix)
	if err != nil {
		return nil, err
	}

	var entities []*databasev1.IndexRuleBinding
	for _, keyPrefix := range keyPrefixes {
		messages, err := e.listWithPrefix(ctx, keyPrefix, func() proto.Message {
			return &databasev1.IndexRuleBinding{}
		})
		if err != nil {
			return nil, err
		}
		for _, message := range messages {
			entities = append(entities, message.(*databasev1.IndexRuleBinding))
		}
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	g, err := e.GetGroup(ctx, indexRuleBinding.GetMetadata().GetGroup())
	if err != nil {
		return errors.Wrap(err, indexRuleBinding.GetMetadata().GetGroup())
	}
	return e.update(ctx, g, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRuleBinding,
			Name:  indexRuleBinding.GetMetadata().GetName(),
			Group: g.GetName(),
		},
		Spec: indexRuleBinding,
	})
}

func (e *etcdSchemaRegistry) DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	g, err := e.GetGroup(ctx, metadata.GetGroup())
	if err != nil {
		return false, errors.Wrap(err, metadata.GetGroup())
	}
	return e.delete(ctx, g, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRuleBinding,
			Name:  metadata.GetName(),
			Group: g.GetName(),
		},
	})
}

func (e *etcdSchemaRegistry) GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error) {
	var entity databasev1.IndexRule
	if err := e.get(ctx, formatIndexRuleKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListIndexRule(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRule, error) {
	keyPrefixes, err := e.listPrefixesForEntity(ctx, opt, IndexRuleKeyPrefix)
	if err != nil {
		return nil, err
	}

	var entities []*databasev1.IndexRule
	for _, keyPrefix := range keyPrefixes {
		messages, err := e.listWithPrefix(ctx, keyPrefix, func() proto.Message {
			return &databasev1.IndexRule{}
		})
		if err != nil {
			return nil, err
		}
		for _, message := range messages {
			entities = append(entities, message.(*databasev1.IndexRule))
		}
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	g, err := e.GetGroup(ctx, indexRule.GetMetadata().GetGroup())
	if err != nil {
		return errors.Wrap(err, indexRule.GetMetadata().GetGroup())
	}
	return e.update(ctx, g, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRule,
			Name:  indexRule.GetMetadata().GetName(),
			Group: g.GetName(),
		},
		Spec: indexRule,
	})
}

func (e *etcdSchemaRegistry) DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	g, err := e.GetGroup(ctx, metadata.GetGroup())
	if err != nil {
		return false, errors.Wrap(err, metadata.GetGroup())
	}
	return e.delete(ctx, g, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindIndexRule,
			Name:  metadata.GetName(),
			Group: g.GetName(),
		},
	})
}

func (e *etcdSchemaRegistry) ReadyNotify() <-chan struct{} {
	return e.server.Server.ReadyNotify()
}

func (e *etcdSchemaRegistry) StopNotify() <-chan struct{} {
	return e.server.Server.StopNotify()
}

func (e *etcdSchemaRegistry) StoppingNotify() <-chan struct{} {
	return e.server.Server.StoppingNotify()
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
	if err = proto.Unmarshal(resp.Kvs[0].Value, message); err != nil {
		return err
	}
	if messageWithMetadata, ok := message.(HasMetadata); ok {
		// Assign readonly fields
		messageWithMetadata.GetMetadata().CreateRevision = resp.Kvs[0].CreateRevision
		messageWithMetadata.GetMetadata().ModRevision = resp.Kvs[0].ModRevision
	}
	return nil
}

func (e *etcdSchemaRegistry) update(ctx context.Context, group *commonv1.Group, metadata Metadata) error {
	val, err := proto.Marshal(metadata.Spec.(proto.Message))
	if err != nil {
		return err
	}
	_, err = e.kv.Put(ctx, metadata.Key(), string(val))
	if err != nil {
		return err
	}
	e.notifyUpdate(metadata)
	return e.touchGroup(ctx, group)
}

func (e *etcdSchemaRegistry) listWithPrefix(ctx context.Context, prefix string, factory func() proto.Message) ([]proto.Message, error) {
	resp, err := e.kv.Get(ctx, prefix, clientv3.WithFromKey(), clientv3.WithRange(incrementLastByte(prefix)))
	if err != nil {
		return nil, err
	}
	entities := make([]proto.Message, resp.Count)
	for i := int64(0); i < resp.Count; i++ {
		message := factory()
		if innerErr := proto.Unmarshal(resp.Kvs[i].Value, message); innerErr != nil {
			return nil, innerErr
		}
		entities[i] = message
		if messageWithMetadata, ok := message.(HasMetadata); ok {
			// Assign readonly fields
			messageWithMetadata.GetMetadata().CreateRevision = resp.Kvs[0].CreateRevision
			messageWithMetadata.GetMetadata().ModRevision = resp.Kvs[0].ModRevision
		}
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) listPrefixesForEntity(ctx context.Context, opt ListOpt, entityPrefix string) ([]string, error) {
	var keyPrefixes []string

	if opt.Group != "" {
		keyPrefixes = append(keyPrefixes, GroupsKeyPrefix+opt.Group+entityPrefix)
	} else {
		groups, err := e.ListGroup(ctx)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			keyPrefixes = append(keyPrefixes, GroupsKeyPrefix+g+entityPrefix)
		}
	}

	return keyPrefixes, nil
}

func (e *etcdSchemaRegistry) delete(ctx context.Context, g *commonv1.Group, metadata Metadata) (bool, error) {
	resp, err := e.kv.Delete(ctx, metadata.Key(), clientv3.WithPrevKV())
	if err != nil {
		return false, err
	}
	if resp.Deleted == 1 {
		var message proto.Message
		switch metadata.Kind {
		case KindMeasure:
			message = &databasev1.Measure{}
		case KindStream:
			message = &databasev1.Stream{}
		case KindIndexRuleBinding:
			message = &databasev1.IndexRuleBinding{}
		case KindIndexRule:
			message = &databasev1.IndexRule{}
		}
		if unmarshalErr := proto.Unmarshal(resp.PrevKvs[0].Value, message); unmarshalErr == nil {
			e.notifyDelete(Metadata{
				TypeMeta: TypeMeta{
					Kind:  metadata.Kind,
					Name:  metadata.Name,
					Group: g.GetName(),
				},
				Spec: message,
			})
		}
		return true, e.touchGroup(ctx, g)
	}
	return false, nil
}

func formatIndexRuleKey(metadata *commonv1.Metadata) string {
	return formatKey(IndexRuleKeyPrefix, metadata)
}

func formatIndexRuleBindingKey(metadata *commonv1.Metadata) string {
	return formatKey(IndexRuleBindingKeyPrefix, metadata)
}

func formatStreamKey(metadata *commonv1.Metadata) string {
	return formatKey(StreamKeyPrefix, metadata)
}

func formatMeasureKey(metadata *commonv1.Metadata) string {
	return formatKey(MeasureKeyPrefix, metadata)
}

func formatKey(entityPrefix string, metadata *commonv1.Metadata) string {
	return GroupsKeyPrefix + metadata.GetGroup() + entityPrefix + metadata.GetName()
}

func formatGroupKey(group string) string {
	return GroupsKeyPrefix + group + GroupMetadataKey
}

func incrementLastByte(key string) string {
	bb := []byte(key)
	bb[len(bb)-1]++
	return string(bb)
}

func newStandaloneEtcdConfig(config *etcdSchemaRegistryConfig) *embed.Config {
	cfg := embed.NewConfig()
	// TODO: allow user to set path
	cfg.Dir = filepath.Join(config.rootDir, "metadata")
	cURL, _ := url.Parse(config.listenerClientURL)
	pURL, _ := url.Parse(config.listenerPeerURL)

	cfg.ClusterState = "new"
	cfg.LCUrls, cfg.ACUrls = []url.URL{*cURL}, []url.URL{*cURL}
	cfg.LPUrls, cfg.APUrls = []url.URL{*pURL}, []url.URL{*pURL}
	cfg.InitialCluster = ",default=" + pURL.String()
	return cfg
}
