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
	"crypto/tls"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
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

// Namespace sets the namespace of the registry.
func Namespace(namespace string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.namespace = namespace
	}
}

// ConfigureServerEndpoints sets a list of the server urls.
func ConfigureServerEndpoints(url []string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.serverEndpoints = url
	}
}

// ConfigureEtcdUser sets a username & password of the etcd.
func ConfigureEtcdUser(username string, password string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		if username != "" && password != "" {
			config.username = username
			config.password = password
		}
	}
}

// ConfigureEtcdTLSCAFile sets a trusted ca file of the etcd tls config.
func ConfigureEtcdTLSCAFile(file string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		config.tlsCAFile = file
	}
}

// ConfigureEtcdTLSCertAndKey sets a cert & key of the etcd tls config.
func ConfigureEtcdTLSCertAndKey(certFile string, keyFile string) RegistryOption {
	return func(config *etcdSchemaRegistryConfig) {
		if certFile != "" && keyFile != "" {
			config.tlsCertFile = certFile
			config.tlsKeyFile = keyFile
		}
	}
}

type etcdSchemaRegistry struct {
	namespace string
	client    *clientv3.Client
	closer    *run.Closer
	l         *logger.Logger
	watchers  []*watcher
	mux       sync.RWMutex
}

type etcdSchemaRegistryConfig struct {
	namespace       string
	username        string
	password        string
	tlsCAFile       string
	tlsCertFile     string
	tlsKeyFile      string
	serverEndpoints []string
}

func (e *etcdSchemaRegistry) RegisterHandler(name string, kind Kind, handler EventHandler) {
	// Validate kind
	if kind&KindMask != kind {
		panic(fmt.Sprintf("invalid kind %d", kind))
	}
	e.mux.Lock()
	defer e.mux.Unlock()
	for i := 0; i < KindSize; i++ {
		ki := Kind(1 << i)
		if kind&ki > 0 {
			e.l.Info().Str("name", name).Stringer("kind", ki).Msg("registering watcher")
			w := e.newWatcher(name, ki, handler)
			e.watchers = append(e.watchers, w)
		}
	}
}

func (e *etcdSchemaRegistry) Close() error {
	e.closer.Done()
	e.closer.CloseThenWait()
	for i, w := range e.watchers {
		_ = w.Close()
		e.watchers[i] = nil
	}
	return e.client.Close()
}

// NewEtcdSchemaRegistry returns a Registry powered by Etcd.
func NewEtcdSchemaRegistry(options ...RegistryOption) (Registry, error) {
	registryConfig := &etcdSchemaRegistryConfig{}
	for _, opt := range options {
		opt(registryConfig)
	}
	if registryConfig.serverEndpoints == nil {
		return nil, errors.New("server address is not set")
	}
	log := logger.GetLogger("etcd")
	zapCfg := log.ToZapConfig()

	var l *zap.Logger
	var err error
	if l, err = zapCfg.Build(); err != nil {
		return nil, err
	}

	config := clientv3.Config{
		Endpoints:            registryConfig.serverEndpoints,
		DialTimeout:          5 * time.Second,
		DialKeepAliveTime:    30 * time.Second,
		DialKeepAliveTimeout: 10 * time.Second,
		AutoSyncInterval:     5 * time.Minute,
		Logger:               l,
		Username:             registryConfig.username,
		Password:             registryConfig.password,
		TLS:                  extractTLSConfig(registryConfig),
	}
	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	reg := &etcdSchemaRegistry{
		namespace: registryConfig.namespace,
		client:    client,
		closer:    run.NewCloser(1),
		l:         log,
	}
	return reg, nil
}

func (e *etcdSchemaRegistry) prependNamespace(key string) string {
	if e.namespace == "" {
		return key
	}
	return path.Join("/", e.namespace, key)
}

func (e *etcdSchemaRegistry) get(ctx context.Context, key string, message proto.Message) error {
	if !e.closer.AddRunning() {
		return ErrClosed
	}
	defer e.closer.Done()
	key = e.prependNamespace(key)
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
func (e *etcdSchemaRegistry) update(ctx context.Context, metadata Metadata) (int64, error) {
	if !e.closer.AddRunning() {
		return 0, ErrClosed
	}
	defer e.closer.Done()
	key, err := metadata.key()
	if err != nil {
		return 0, err
	}
	key = e.prependNamespace(key)
	getResp, err := e.client.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if getResp.Count > 1 {
		return 0, errUnexpectedNumberOfEntities
	}
	val, err := proto.Marshal(metadata.Spec.(proto.Message))
	if err != nil {
		return 0, err
	}
	replace := getResp.Count > 0
	if !replace {
		return 0, ErrGRPCResourceNotFound
	}
	existingVal, innerErr := metadata.Kind.Unmarshal(getResp.Kvs[0])
	if innerErr != nil {
		return 0, innerErr
	}
	// directly return if we have the same entity
	if metadata.equal(existingVal) {
		return 0, nil
	}

	modRevision := metadata.ModRevision
	if modRevision == 0 {
		modRevision = getResp.Kvs[0].ModRevision
	}
	txnResp, txnErr := e.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", modRevision)).
		Then(clientv3.OpPut(key, string(val))).
		Commit()
	if txnErr != nil {
		return 0, txnErr
	}
	if !txnResp.Succeeded {
		return 0, errConcurrentModification
	}

	return txnResp.Responses[0].GetResponsePut().Header.Revision, nil
}

// create will first check existence of the entity with the metadata,
// and put the value if it does not exist.
// Otherwise, it will return ErrGRPCAlreadyExists.
func (e *etcdSchemaRegistry) create(ctx context.Context, metadata Metadata) (int64, error) {
	if !e.closer.AddRunning() {
		return 0, ErrClosed
	}
	defer e.closer.Done()
	key, err := metadata.key()
	if err != nil {
		return 0, err
	}
	key = e.prependNamespace(key)
	getResp, err := e.client.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if getResp.Count > 1 {
		return 0, errUnexpectedNumberOfEntities
	}
	val, err := proto.Marshal(metadata.Spec.(proto.Message))
	if err != nil {
		return 0, err
	}
	replace := getResp.Count > 0
	if replace {
		return 0, errGRPCAlreadyExists
	}
	putResp, err := e.client.Put(ctx, key, string(val))
	if err != nil {
		return 0, err
	}

	return putResp.Header.Revision, nil
}

func (e *etcdSchemaRegistry) listWithPrefix(ctx context.Context, prefix string, kind Kind) ([]proto.Message, error) {
	if !e.closer.AddRunning() {
		return nil, ErrClosed
	}
	defer e.closer.Done()
	prefix = e.prependNamespace(prefix)
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	entities := make([]proto.Message, resp.Count)
	for i := int64(0); i < resp.Count; i++ {
		md, err := kind.Unmarshal(resp.Kvs[i])
		if err != nil {
			return nil, err
		}
		entities[i] = md.Spec.(proto.Message)
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) delete(ctx context.Context, metadata Metadata) (bool, error) {
	if !e.closer.AddRunning() {
		return false, ErrClosed
	}
	defer e.closer.Done()
	key, err := metadata.key()
	if err != nil {
		return false, err
	}
	key = e.prependNamespace(key)
	resp, err := e.client.Delete(ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return false, err
	}
	if resp.Deleted == 1 {
		return true, nil
	}
	return false, nil
}

func (e *etcdSchemaRegistry) register(ctx context.Context, metadata Metadata, forced bool) error {
	if !e.closer.AddRunning() {
		return ErrClosed
	}
	defer e.closer.Done()
	key, err := metadata.key()
	if err != nil {
		return err
	}
	key = e.prependNamespace(key)
	val, err := proto.Marshal(metadata.Spec.(proto.Message))
	if err != nil {
		return err
	}
	// Create a lease with a short TTL
	lease, err := e.client.Grant(ctx, 5) // 5 seconds
	if err != nil {
		return err
	}
	if forced {
		if _, err = e.client.Put(ctx, key, string(val), clientv3.WithLease(lease.ID)); err != nil {
			return err
		}
	} else {
		var ops []clientv3.Cmp
		ops = append(ops, clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
		txn := e.client.Txn(ctx).If(ops...)
		txn = txn.Then(clientv3.OpPut(key, string(val), clientv3.WithLease(lease.ID)))
		txn = txn.Else(clientv3.OpGet(key))
		response, errCommit := txn.Commit()
		if errCommit != nil {
			return errCommit
		}
		if !response.Succeeded {
			return errGRPCAlreadyExists
		}
	}

	// Keep the lease alive
	// nolint:contextcheck
	keepAliveChan, err := e.client.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		return err
	}
	go func() {
		if !e.closer.AddRunning() {
			return
		}
		defer func() {
			_, _ = e.client.Lease.Revoke(context.Background(), lease.ID)
			e.closer.Done()
		}()
		for {
			select {
			case <-e.closer.CloseNotify():
				return
			case keepAliveResp := <-keepAliveChan:
				if keepAliveResp == nil {
					// The channel has been closed
					return
				}
			}
		}
	}()
	return nil
}

func (e *etcdSchemaRegistry) newWatcher(name string, kind Kind, handler EventHandler) *watcher {
	return newWatcher(e.client, watcherConfig{
		key:     e.prependNamespace(kind.key()),
		kind:    kind,
		handler: handler,
	}, e.l.Named(fmt.Sprintf("watcher-%s[%s]", name, kind.String())))
}

func listPrefixesForEntity(group, entityPrefix string) string {
	return path.Join(entityPrefix, group)
}

func formatKey(entityPrefix string, metadata *commonv1.Metadata) string {
	return path.Join(
		listPrefixesForEntity(metadata.GetGroup(), entityPrefix),
		metadata.GetName())
}

func extractTLSConfig(cfg *etcdSchemaRegistryConfig) *tls.Config {
	if cfg.tlsCAFile == "" && cfg.tlsCertFile == "" && cfg.tlsKeyFile == "" {
		return nil
	}
	tlsInfo := transport.TLSInfo{
		TrustedCAFile: cfg.tlsCAFile,
		CertFile:      cfg.tlsCertFile,
		KeyFile:       cfg.tlsKeyFile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil
	}
	return tlsConfig
}
