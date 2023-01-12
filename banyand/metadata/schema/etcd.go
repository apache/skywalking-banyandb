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
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ Stream           = (*etcdSchemaRegistry)(nil)
	_ IndexRuleBinding = (*etcdSchemaRegistry)(nil)
	_ IndexRule        = (*etcdSchemaRegistry)(nil)
	_ Measure          = (*etcdSchemaRegistry)(nil)
	_ Group            = (*etcdSchemaRegistry)(nil)
	_ Property         = (*etcdSchemaRegistry)(nil)

	errUnexpectedNumberOfEntities = errors.New("unexpected number of entities")
	errConcurrentModification     = errors.New("concurrent modification of entities")
)

// HasMetadata allows getting Metadata.
type HasMetadata interface {
	GetMetadata() *commonv1.Metadata
	proto.Message
}

// RegistryOption is the option to create Registry.
type RegistryOption func(*etcdSchemaRegistryConfig)

// RootDir sets the root directory of Registry.
func RootDir(rootDir string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.rootDir = rootDir
	}
}

// ConfigureListener sets client and peer urls of listeners.
func ConfigureListener(lc, lp string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.listenerClientURL = lc
		config.listenerPeerURL = lp
	}
}

type eventHandler struct {
	handler      EventHandler
	interestKeys Kind
}

func (eh *eventHandler) interestOf(kind Kind) bool {
	return KindMask&kind&eh.interestKeys != 0
}

type etcdSchemaRegistry struct {
	server   *embed.Etcd
	client   *clientv3.Client
	closer   *run.Closer
	handlers []*eventHandler
	mux      sync.RWMutex
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
	e.mux.Lock()
	defer e.mux.Unlock()
	e.handlers = append(e.handlers, &eventHandler{
		interestKeys: kind,
		handler:      handler,
	})
}

func (e *etcdSchemaRegistry) notifyUpdate(metadata Metadata) {
	e.mux.RLock()
	hh := e.handlers
	e.mux.RUnlock()
	for _, h := range hh {
		if h.interestOf(metadata.Kind) {
			h.handler.OnAddOrUpdate(metadata)
		}
	}
}

func (e *etcdSchemaRegistry) notifyDelete(metadata Metadata) {
	e.mux.RLock()
	hh := e.handlers
	e.mux.RUnlock()
	for _, h := range hh {
		if h.interestOf(metadata.Kind) {
			h.handler.OnDelete(metadata)
		}
	}
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
	e.closer.CloseThenWait()
	_ = e.client.Close()
	e.server.Close()
	return nil
}

// NewEtcdSchemaRegistry returns a Registry powered by Etcd.
func NewEtcdSchemaRegistry(options ...RegistryOption) (Registry, error) {
	registryConfig := &etcdSchemaRegistryConfig{
		rootDir:           os.TempDir(),
		listenerClientURL: embed.DefaultListenClientURLs,
		listenerPeerURL:   embed.DefaultListenPeerURLs,
	}
	for _, opt := range options {
		opt(registryConfig)
	}
	zapCfg := logger.GetLogger("etcd").ToZapConfig()

	var l *zap.Logger
	var err error
	if l, err = zapCfg.Build(); err != nil {
		return nil, err
	}
	// TODO: allow use cluster setting
	embedConfig := newStandaloneEtcdConfig(registryConfig, l)
	e, err := embed.StartEtcd(embedConfig)
	if err != nil {
		return nil, err
	}
	if e != nil {
		<-e.Server.ReadyNotify() // wait for e.Server to join the cluster
	}

	config := clientv3.Config{
		Endpoints:            []string{e.Config().ACUrls[0].String()},
		DialTimeout:          5 * time.Second,
		DialKeepAliveTime:    30 * time.Second,
		DialKeepAliveTimeout: 10 * time.Second,
		DialOptions:          []grpc.DialOption{grpc.WithBlock()},
		Logger:               l,
	}
	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	reg := &etcdSchemaRegistry{
		server: e,
		client: client,
		closer: run.NewCloser(0),
	}
	return reg, nil
}

func (e *etcdSchemaRegistry) get(ctx context.Context, key string, message proto.Message) error {
	if !e.closer.AddRunning() {
		return errClosed
	}
	defer e.closer.Done()
	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return ErrGRPCResourceNotFound
	}
	if resp.Count > 1 {
		return errUnexpectedNumberOfEntities
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

// update will first ensure the existence of the entity with the metadata,
// and overwrite the existing value if so.
// Otherwise, it will return ErrGRPCResourceNotFound.
func (e *etcdSchemaRegistry) update(ctx context.Context, metadata Metadata) error {
	if !e.closer.AddRunning() {
		return errClosed
	}
	defer e.closer.Done()
	key, err := metadata.key()
	if err != nil {
		return err
	}
	getResp, err := e.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if getResp.Count > 1 {
		return errUnexpectedNumberOfEntities
	}
	val, err := proto.Marshal(metadata.Spec.(proto.Message))
	if err != nil {
		return err
	}
	replace := getResp.Count > 0
	if replace {
		existingVal, innerErr := metadata.Unmarshal(getResp.Kvs[0].Value)
		if innerErr != nil {
			return innerErr
		}
		// directly return if we have the same entity
		if metadata.equal(existingVal) {
			return nil
		}

		modRevision := getResp.Kvs[0].ModRevision
		txnResp, txnErr := e.client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", modRevision)).
			Then(clientv3.OpPut(key, string(val))).
			Commit()
		if txnErr != nil {
			return txnErr
		}
		if !txnResp.Succeeded {
			return errConcurrentModification
		}
	} else {
		return ErrGRPCResourceNotFound
	}
	e.notifyUpdate(metadata)
	return nil
}

// create will first check existence of the entity with the metadata,
// and put the value if it does not exist.
// Otherwise, it will return ErrGRPCAlreadyExists.
func (e *etcdSchemaRegistry) create(ctx context.Context, metadata Metadata) error {
	if !e.closer.AddRunning() {
		return errClosed
	}
	defer e.closer.Done()
	key, err := metadata.key()
	if err != nil {
		return err
	}
	getResp, err := e.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if getResp.Count > 1 {
		return errUnexpectedNumberOfEntities
	}
	val, err := proto.Marshal(metadata.Spec.(proto.Message))
	if err != nil {
		return err
	}
	replace := getResp.Count > 0
	if replace {
		return errGRPCAlreadyExists
	}
	_, err = e.client.Put(ctx, key, string(val))
	if err != nil {
		return err
	}

	e.notifyUpdate(metadata)
	return nil
}

func (e *etcdSchemaRegistry) listWithPrefix(ctx context.Context, prefix string, factory func() proto.Message) ([]proto.Message, error) {
	if !e.closer.AddRunning() {
		return nil, errClosed
	}
	defer e.closer.Done()
	resp, err := e.client.Get(ctx, prefix, clientv3.WithFromKey(), clientv3.WithRange(incrementLastByte(prefix)))
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
			messageWithMetadata.GetMetadata().CreateRevision = resp.Kvs[i].CreateRevision
			messageWithMetadata.GetMetadata().ModRevision = resp.Kvs[i].ModRevision
		}
	}
	return entities, nil
}

func listPrefixesForEntity(group, entityPrefix string) string {
	return groupsKeyPrefix + group + entityPrefix
}

func (e *etcdSchemaRegistry) delete(ctx context.Context, metadata Metadata) (bool, error) {
	if !e.closer.AddRunning() {
		return false, errClosed
	}
	defer e.closer.Done()
	key, err := metadata.key()
	if err != nil {
		return false, err
	}
	resp, err := e.client.Delete(ctx, key, clientv3.WithPrevKV())
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
		case KindProperty:
			message = &propertyv1.Property{}
		case KindTopNAggregation:
			message = &databasev1.TopNAggregation{}
		default:
			return false, nil
		}
		if unmarshalErr := proto.Unmarshal(resp.PrevKvs[0].Value, message); unmarshalErr == nil {
			e.notifyDelete(Metadata{
				TypeMeta: TypeMeta{
					Kind:  metadata.Kind,
					Name:  metadata.Name,
					Group: metadata.Group,
				},
				Spec: message,
			})
		}
		return true, nil
	}
	return false, nil
}

func formatKey(entityPrefix string, metadata *commonv1.Metadata) string {
	return groupsKeyPrefix + metadata.GetGroup() + entityPrefix + metadata.GetName()
}

func incrementLastByte(key string) string {
	bb := []byte(key)
	bb[len(bb)-1]++
	return string(bb)
}

func newStandaloneEtcdConfig(config *etcdSchemaRegistryConfig, logger *zap.Logger) *embed.Config {
	cfg := embed.NewConfig()
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger)
	cfg.Dir = filepath.Join(config.rootDir, "metadata")
	cURL, _ := url.Parse(config.listenerClientURL)
	pURL, _ := url.Parse(config.listenerPeerURL)

	cfg.ClusterState = "new"
	cfg.LCUrls, cfg.ACUrls = []url.URL{*cURL}, []url.URL{*cURL}
	cfg.LPUrls, cfg.APUrls = []url.URL{*pURL}, []url.URL{*pURL}
	cfg.InitialCluster = ",default=" + pURL.String()
	return cfg
}
