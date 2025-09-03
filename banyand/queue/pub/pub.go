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

// Package pub implements the queue client.
package pub

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// ChunkedSyncClientConfig configures chunked sync client behavior.
type ChunkedSyncClientConfig struct {
	ChunkSize        uint32        // Size of each chunk in bytes
	EnableRetryOnOOO bool          // Enable retry on out-of-order errors
	MaxOOORetries    int           // Maximum retries for out-of-order chunks
	OOORetryDelay    time.Duration // Delay between retries
}

var (
	_ run.PreRunner = (*pub)(nil)
	_ run.Service   = (*pub)(nil)
	_ run.Config    = (*pub)(nil)
)

type pub struct {
	schema.UnimplementedOnInitHandler
	metadata        metadata.Repo
	writableProbe   map[string]map[string]struct{}
	cbStates        map[string]*circuitState
	registered      map[string]*databasev1.Node
	active          map[string]*client
	evictable       map[string]evictNode
	closer          *run.Closer
	handlers        map[bus.Topic]schema.EventHandler
	log             *logger.Logger
	caCertPath      string
	prefix          string
	allowedRoles    []databasev1.Role
	mu              sync.RWMutex
	cbMu            sync.RWMutex
	writableProbeMu sync.Mutex
	tlsEnabled      bool
}

func (p *pub) FlagSet() *run.FlagSet {
	prefixFlag := func(name string) string {
		if p.prefix == "" {
			return name
		}
		return p.prefix + "-" + name
	}
	fs := run.NewFlagSet("queue-client")
	fs.BoolVar(&p.tlsEnabled, prefixFlag("client-tls"), false, fmt.Sprintf("enable client TLS for %s", p.prefix))
	fs.StringVar(&p.caCertPath, prefixFlag("client-ca-cert"), "", fmt.Sprintf("CA certificate file to verify the %s server", p.prefix))
	return fs
}

func (p *pub) Validate() error {
	// simple sanity‑check: if TLS is on, a CA bundle must be provided
	if p.tlsEnabled && p.caCertPath == "" {
		return fmt.Errorf("TLS is enabled (--internal-tls), but no CA certificate file was provided (--internal-ca-cert is required)")
	}
	return nil
}

func (p *pub) Register(topic bus.Topic, handler schema.EventHandler) {
	p.handlers[topic] = handler
}

func (p *pub) GracefulStop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.evictable {
		close(p.evictable[i].c)
	}
	p.evictable = nil
	p.closer.Done()
	p.closer.CloseThenWait()
	for _, c := range p.active {
		_ = c.conn.Close()
	}
	p.active = nil
}

// Serve implements run.Service.
func (p *pub) Serve() run.StopNotify {
	return p.closer.CloseNotify()
}

var bypassMatches = []MatchFunc{bypassMatch}

func bypassMatch(_ map[string]string) bool { return true }

func (p *pub) Broadcast(timeout time.Duration, topic bus.Topic, messages bus.Message) ([]bus.Future, error) {
	var nodes []*databasev1.Node
	p.mu.RLock()
	for k := range p.active {
		if n := p.registered[k]; n != nil {
			nodes = append(nodes, n)
		}
	}
	p.mu.RUnlock()
	if len(nodes) == 0 {
		return nil, errors.New("no active nodes")
	}
	names := make(map[string]struct{})
	if len(messages.NodeSelectors()) == 0 {
		for _, n := range nodes {
			names[n.Metadata.GetName()] = struct{}{}
		}
	} else {
		for _, sel := range messages.NodeSelectors() {
			var matches []MatchFunc
			if sel == nil {
				matches = bypassMatches
			} else {
				for _, s := range sel {
					selector, err := ParseLabelSelector(s)
					if err != nil {
						return nil, fmt.Errorf("failed to parse node selector: %w", err)
					}
					matches = append(matches, selector.Matches)
				}
			}
			for _, n := range nodes {
				for _, m := range matches {
					if m(n.Labels) {
						names[n.Metadata.Name] = struct{}{}
						break
					}
				}
			}
		}
	}

	if l := p.log.Debug(); l.Enabled() {
		l.Msgf("broadcasting message to %s nodes", names)
	}

	if len(names) == 0 {
		return nil, fmt.Errorf("no nodes match the selector %v", messages.NodeSelectors())
	}

	futureCh := make(chan publishResult, len(names))
	var wg sync.WaitGroup
	for n := range names {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			f, err := p.publish(timeout, topic, bus.NewMessageWithNode(messages.ID(), n, messages.Data()))
			futureCh <- publishResult{n: n, f: f, e: err}
		}(n)
	}
	go func() {
		wg.Wait()
		close(futureCh)
	}()
	var futures []bus.Future
	var errs error
	for f := range futureCh {
		if f.e != nil {
			errs = multierr.Append(errs, errors.Wrapf(f.e, "failed to publish message to %s", f.n))
			if isFailoverError(f.e) {
				if p.closer.AddRunning() {
					go func() {
						defer p.closer.Done()
						p.failover(f.n, common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, f.e.Error()), topic)
					}()
				}
			}
			continue
		}
		futures = append(futures, f.f)
	}

	if errs != nil {
		return futures, fmt.Errorf("broadcast errors: %w", errs)
	}
	return futures, nil
}

type publishResult struct {
	f bus.Future
	e error
	n string
}

func (p *pub) publish(timeout time.Duration, topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	var err error
	f := &future{}
	handleMessage := func(m bus.Message, err error) error {
		r, errSend := messageToRequest(topic, m)
		if errSend != nil {
			return multierr.Append(err, fmt.Errorf("failed to marshal message[%d]: %w", m.ID(), errSend))
		}
		node := m.Node()
		p.mu.RLock()
		client, ok := p.active[node]
		p.mu.RUnlock()
		if !ok {
			return multierr.Append(err, fmt.Errorf("failed to get client for node %s", node))
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		f.cancelFn = append(f.cancelFn, cancel)
		stream, errCreateStream := client.client.Send(ctx)
		if errCreateStream != nil {
			return multierr.Append(err, fmt.Errorf("failed to get stream for node %s: %w", node, errCreateStream))
		}
		errSend = stream.Send(r)
		if errSend != nil {
			return multierr.Append(err, fmt.Errorf("failed to send message to node %s: %w", node, errSend))
		}
		f.clients = append(f.clients, stream)
		f.topics = append(f.topics, topic)
		f.nodes = append(f.nodes, node)
		return err
	}
	for _, m := range messages {
		err = handleMessage(m, err)
	}
	return f, err
}

func (p *pub) Publish(_ context.Context, topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	// nolint: contextcheck
	return p.publish(15*time.Second, topic, messages...)
}

// New returns a new queue client targeting the given node roles.
// If no roles are passed, it defaults to databasev1.Role_ROLE_DATA.
func New(metadata metadata.Repo, roles ...databasev1.Role) queue.Client {
	if len(roles) == 0 {
		roles = []databasev1.Role{databasev1.Role_ROLE_DATA}
	}
	var strBuilder strings.Builder
	for _, role := range roles {
		switch role {
		case databasev1.Role_ROLE_DATA:
			strBuilder.WriteString("data")
		case databasev1.Role_ROLE_LIAISON:
			strBuilder.WriteString("liaison")
		default:
			logger.Panicf("unknown role %s", role)
		}
	}
	p := &pub{
		metadata:      metadata,
		active:        make(map[string]*client),
		evictable:     make(map[string]evictNode),
		registered:    make(map[string]*databasev1.Node),
		handlers:      make(map[bus.Topic]schema.EventHandler),
		closer:        run.NewCloser(1),
		allowedRoles:  roles,
		prefix:        strBuilder.String(),
		writableProbe: make(map[string]map[string]struct{}),
		cbStates:      make(map[string]*circuitState),
	}
	return p
}

// NewWithoutMetadata returns a new queue client without metadata, defaulting to data nodes.
func NewWithoutMetadata() queue.Client {
	p := New(nil, databasev1.Role_ROLE_DATA)
	p.(*pub).log = logger.GetLogger("queue-client")
	return p
}

func (p *pub) Name() string {
	return "queue-client-" + p.prefix
}

func (p *pub) PreRun(context.Context) error {
	if p.metadata != nil {
		p.metadata.RegisterHandler("queue-client", schema.KindNode, p)
	}

	p.log = logger.GetLogger("server-queue-pub-" + p.prefix)
	return nil
}

func messageToRequest(topic bus.Topic, m bus.Message) (*clusterv1.SendRequest, error) {
	r := &clusterv1.SendRequest{
		Topic:     topic.String(),
		MessageId: uint64(m.ID()),
		BatchMod:  m.BatchModeEnabled(),
	}

	switch data := m.Data().(type) {
	case proto.Message:
		messageData, err := proto.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message %T: %w", m, err)
		}
		r.Body = messageData
	case []byte:
		r.Body = data
	default:
		return nil, fmt.Errorf("invalid message type %T", m.Data())
	}

	return r, nil
}

type future struct {
	clients  []clusterv1.Service_SendClient
	cancelFn []func()
	topics   []bus.Topic
	nodes    []string
}

func (l *future) Get() (bus.Message, error) {
	if len(l.clients) < 1 {
		return bus.Message{}, io.EOF
	}
	c := l.clients[0]
	t := l.topics[0]
	n := l.nodes[0]
	defer func() {
		l.clients = l.clients[1:]
		l.topics = l.topics[1:]
		l.cancelFn[0]()
		l.cancelFn = l.cancelFn[1:]
		l.nodes = l.nodes[1:]
	}()
	resp, err := c.Recv()
	if err != nil {
		return bus.Message{}, err
	}
	if resp.Error != "" {
		return bus.Message{}, errors.New(resp.Error)
	}
	if resp.Body == nil {
		return bus.NewMessageWithNode(bus.MessageID(resp.MessageId), n, nil), nil
	}
	if messageSupplier, ok := data.TopicResponseMap[t]; ok {
		m := messageSupplier()
		err = proto.Unmarshal(resp.Body, m)
		if err != nil {
			return bus.Message{}, err
		}
		return bus.NewMessageWithNode(
			bus.MessageID(resp.MessageId),
			n,
			m,
		), nil
	}
	return bus.Message{}, fmt.Errorf("invalid topic %s", t)
}

func (l *future) GetAll() ([]bus.Message, error) {
	var globalErr error
	ret := make([]bus.Message, 0, len(l.clients))
	for {
		m, err := l.Get()
		if errors.Is(err, io.EOF) {
			return ret, globalErr
		}
		if err != nil {
			globalErr = multierr.Append(globalErr, err)
			continue
		}
		ret = append(ret, m)
	}
}

func isFailoverError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return s.Code() == codes.Unavailable || s.Code() == codes.DeadlineExceeded
}

func (p *pub) getClientTransportCredentials() ([]grpc.DialOption, error) {
	opts, err := grpchelper.SecureOptions(nil, p.tlsEnabled, false, p.caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS config: %w", err)
	}
	return opts, nil
}

// NewChunkedSyncClient implements queue.Client.
func (p *pub) NewChunkedSyncClient(node string, chunkSize uint32) (queue.ChunkedSyncClient, error) {
	return p.NewChunkedSyncClientWithConfig(node, &ChunkedSyncClientConfig{
		ChunkSize:        chunkSize,
		EnableRetryOnOOO: true,
		MaxOOORetries:    3,
		OOORetryDelay:    100 * time.Millisecond,
	})
}

// NewChunkedSyncClientWithConfig creates a chunked sync client with advanced configuration.
func (p *pub) NewChunkedSyncClientWithConfig(node string, config *ChunkedSyncClientConfig) (queue.ChunkedSyncClient, error) {
	p.mu.RLock()
	client, ok := p.active[node]
	p.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no active client for node %s", node)
	}

	if config.ChunkSize == 0 {
		config.ChunkSize = defaultChunkSize
	}

	return &chunkedSyncClient{
		client:    client.client,
		conn:      client.conn,
		node:      node,
		log:       p.log,
		chunkSize: config.ChunkSize,
		config:    config,
	}, nil
}
