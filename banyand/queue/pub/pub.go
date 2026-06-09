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
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	pkgerrors "github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	apidata "github.com/apache/skywalking-banyandb/api/data"
	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

var queuePubScope = observability.RootScope.SubScope("queue_pub")

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

	_ grpchelper.ConnectionHandler[*client] = (*pub)(nil)
)

type pub struct {
	schema.UnimplementedOnInitHandler
	metadata         metadata.Repo
	handlers         map[bus.Topic]schema.EventHandler
	log              *logger.Logger
	metrics          *pubMetrics
	migrationMetrics *pubMigrationMetrics
	connMgr          *grpchelper.ConnManager[*client]
	closer           *run.Closer
	writableProbe    map[string]map[string]struct{}
	nodeCache        map[string]nodeInfo
	caCertPath       string
	caCertReloader   *pkgtls.Reloader
	prefix           string
	retryPolicy      string
	selfNode         string
	selfRole         string
	selfTier         string
	allowedRoles     []databasev1.Role
	writableProbeMu  sync.Mutex
	nodeCacheMu      sync.RWMutex
	tlsEnabled       bool
}

// nodeInfo caches the resolved role and tier for a remote node.
type nodeInfo struct {
	role string
	tier string
}

type pubMetrics struct {
	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalLatency  meter.Histogram
	totalErr      meter.Counter
	sentBytes     meter.Counter
}

func newPubMetrics(factory observability.Factory) *pubMetrics {
	labels := []string{"operation", "group", "remote_node", "remote_role", "remote_tier"}
	return &pubMetrics{
		totalStarted:  factory.NewCounter("total_started", labels...),
		totalFinished: factory.NewCounter("total_finished", labels...),
		totalLatency:  factory.NewHistogram("total_latency", meter.DefBuckets, labels...),
		totalErr:      factory.NewCounter("total_err", append(labels, "error_type")...),
		sentBytes:     factory.NewCounter("sent_bytes", labels...),
	}
}

// AddressOf implements grpchelper.ConnectionHandler.
func (p *pub) AddressOf(node *databasev1.Node) string {
	return node.GrpcAddress
}

// GetDialOptions implements grpchelper.ConnectionHandler.
func (p *pub) GetDialOptions() ([]grpc.DialOption, error) {
	return p.getClientTransportCredentials()
}

// NewClient implements grpchelper.ConnectionHandler.
func (p *pub) NewClient(conn *grpc.ClientConn, node *databasev1.Node) (*client, error) {
	md := schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Name: node.Metadata.GetName(),
			Kind: schema.KindNode,
		},
		Spec: node,
	}
	return &client{
		client: clusterv1.NewServiceClient(conn),
		conn:   conn,
		md:     md,
	}, nil
}

// OnActive implements grpchelper.ConnectionHandler.
func (p *pub) OnActive(name string, c *client) {
	for _, h := range p.handlers {
		h.OnAddOrUpdate(c.md)
	}
	if node, ok := c.md.Spec.(*databasev1.Node); ok {
		info := resolveNodeInfo(node)
		p.nodeCacheMu.Lock()
		p.nodeCache[name] = info
		p.nodeCacheMu.Unlock()
	}
}

// OnInactive implements grpchelper.ConnectionHandler.
func (p *pub) OnInactive(name string, c *client) {
	for _, h := range p.handlers {
		h.OnDelete(c.md)
	}
	p.writableProbeMu.Lock()
	delete(p.writableProbe, name)
	p.writableProbeMu.Unlock()
	p.nodeCacheMu.Lock()
	delete(p.nodeCache, name)
	p.nodeCacheMu.Unlock()
}

// resolveNodeInfo extracts role and tier from a Node spec.
func resolveNodeInfo(node *databasev1.Node) nodeInfo {
	info := nodeInfo{}
	for _, r := range node.GetRoles() {
		switch r {
		case databasev1.Role_ROLE_DATA:
			info.role = "data"
		case databasev1.Role_ROLE_LIAISON:
			info.role = "liaison"
		default:
			// ROLE_UNSPECIFIED, ROLE_META and any future roles are not queue peers.
		}
		if info.role != "" {
			break
		}
	}
	if node.GetLabels() != nil {
		info.tier = node.GetLabels()["type"]
	}
	return info
}

// getNodeInfo returns the cached nodeInfo for the named node.
func (p *pub) getNodeInfo(name string) nodeInfo {
	if p == nil {
		return nodeInfo{}
	}
	p.nodeCacheMu.RLock()
	info := p.nodeCache[name]
	p.nodeCacheMu.RUnlock()
	return info
}

// SetSelfNode sets the publisher's own node identity for sender stamping.
func (p *pub) SetSelfNode(name, role, tier string) {
	p.selfNode = name
	p.selfRole = role
	p.selfTier = tier
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
		return fmt.Errorf("TLS is enabled (--data-client-tls), but no CA certificate file was provided (--data-client-ca-cert is required)")
	}
	return nil
}

func (p *pub) Register(topic bus.Topic, handler schema.EventHandler) {
	p.handlers[topic] = handler
}

func (p *pub) GracefulStop() {
	if p.caCertReloader != nil {
		p.caCertReloader.Stop()
	}
	p.closer.Done()
	p.closer.CloseThenWait()
	p.connMgr.GracefulStop()
}

// Serve implements run.Service.
func (p *pub) Serve() run.StopNotify {
	// Start CA certificate reloader if enabled
	if p.caCertReloader != nil {
		if err := p.caCertReloader.Start(context.Background()); err != nil {
			p.log.Error().Err(err).Msg("Failed to start CA certificate reloader")
			stopCh := p.closer.CloseNotify()
			return stopCh
		}
		p.log.Info().Str("caCertPath", p.caCertPath).Msg("Started CA certificate file monitoring")

		// Listen for certificate update events
		certUpdateCh := p.caCertReloader.GetUpdateChannel()
		stopCh := p.closer.CloseNotify()
		if p.closer.AddRunning() {
			run.Go(context.Background(), "pub-cert-watcher", p.log, func(_ context.Context) {
				defer p.closer.Done()
				for {
					select {
					case <-certUpdateCh:
						p.log.Info().Msg("CA certificate updated, reconnecting clients")
						p.connMgr.ReconnectAll()
					case <-stopCh:
						return
					}
				}
			})
		}
		return stopCh
	}

	return p.closer.CloseNotify()
}

var bypassMatches = []MatchFunc{bypassMatch}

func bypassMatch(_ map[string]string) bool { return true }

func (p *pub) Broadcast(timeout time.Duration, topic bus.Topic, messages bus.Message) ([]bus.Future, error) {
	nodes := p.connMgr.ActiveRegisteredNodes()
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
			f, err := p.publish(timeout, topic, bus.NewMessageWithNodeAndGroup(messages.ID(), n, messages.Group(), messages.Data()))
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
			errs = multierr.Append(errs, pkgerrors.Wrapf(f.e, "failed to publish message to %s", f.n))
			if grpchelper.IsFailoverError(f.e) {
				if p.closer.AddRunning() {
					run.Go(context.Background(), "pub-failover", p.log, func(ctx context.Context) {
						defer p.closer.Done()
						p.failover(ctx, f.n, common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, f.e.Error()), topic)
					})
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
	f := &future{
		log:     p.log,
		metrics: p.metrics,
	}
	handleMessage := func(m bus.Message, err error) error {
		r, errSend := messageToRequest(topic, m)
		if errSend != nil {
			return multierr.Append(err, fmt.Errorf("failed to marshal message[%d]: %w", m.ID(), errSend))
		}
		// Stamp sender identity so the receiver can label query/control queue
		// metrics by sender (topology). publish opens one stream per message, so
		// every request is the first frame of its stream — the sub captures it there.
		r.SenderNode = p.selfNode
		r.SenderRole = p.selfRole
		r.SenderTier = p.selfTier
		node := m.Node()
		group := m.Group()
		info := p.getNodeInfo(node)
		execErr := p.connMgr.Execute(node, func(c *client) error {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			f.cancelFn = append(f.cancelFn, cancel)
			stream, errCreateStream := c.client.Send(ctx)
			if errCreateStream != nil {
				// Record failure for circuit breaker (only for transient/internal errors)
				if p.metrics != nil {
					p.metrics.totalErr.Inc(1, apidata.OperationOf(topic), group, node, info.role, info.tier, sendErrReasonSendError)
				}
				return fmt.Errorf("failed to get stream for node %s: %w", node, errCreateStream)
			}
			if sendErr := stream.Send(r); sendErr != nil {
				if p.metrics != nil {
					p.metrics.totalErr.Inc(1, apidata.OperationOf(topic), group, node, info.role, info.tier, sendErrReasonSendError)
				}
				return fmt.Errorf("failed to send message to node %s: %w", node, sendErr)
			}
			f.clients = append(f.clients, stream)
			f.topics = append(f.topics, topic)
			f.nodes = append(f.nodes, node)
			f.groups = append(f.groups, group)
			f.roles = append(f.roles, info.role)
			f.tiers = append(f.tiers, info.tier)
			f.starts = append(f.starts, time.Now())
			// Started is recorded here (one stream per message); the matching
			// finished/latency is observed when the response is read in Get.
			if p.metrics != nil {
				p.metrics.totalStarted.Inc(1, apidata.OperationOf(topic), group, node, info.role, info.tier)
			}
			return nil
		})
		if execErr != nil {
			err = multierr.Append(err, execErr)
		}
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

// GetRouteTable implements RouteTableProvider interface.
// Returns a RouteTable with all registered nodes and their health states.
func (p *pub) GetRouteTable() *databasev1.RouteTable {
	return p.connMgr.GetRouteTable()
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
		handlers:      make(map[bus.Topic]schema.EventHandler),
		closer:        run.NewCloser(1),
		allowedRoles:  roles,
		prefix:        strBuilder.String(),
		writableProbe: make(map[string]map[string]struct{}),
		nodeCache:     make(map[string]nodeInfo),
		retryPolicy:   retryPolicy,
	}
	return p
}

// NewWithoutMetadata returns a new queue client without metadata, defaulting to data nodes.
// sender_* fields are left empty for this lifecycle-tool publisher.
//
// If omr is non-nil, a parallel banyandb_lifecycle_migration_* metric family is
// registered on it. The regular banyandb_queue_pub_* family stays disabled because
// metadata is nil (PreRun gates it on metadata != nil). Pass nil to leave the
// migration metrics disabled (e.g. tests, or non-migration clients).
func NewWithoutMetadata(omr observability.MetricsRegistry) queue.Client {
	p := New(nil, databasev1.Role_ROLE_DATA)
	pp := p.(*pub)
	pp.log = logger.GetLogger("queue-client")
	pp.connMgr = grpchelper.NewConnManager(grpchelper.ConnManagerConfig[*client]{
		Handler:        pp,
		Logger:         pp.log,
		RetryPolicy:    pp.retryPolicy,
		MaxRecvMsgSize: maxReceiveMessageSize,
	})
	if omr != nil {
		pp.migrationMetrics = newPubMigrationMetrics(omr.With(lifecycleMigrationScope))
	}
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

	if p.metrics == nil && p.metadata != nil {
		if svc, ok := p.metadata.(metadata.Service); ok {
			if omr := svc.MetricsRegistry(); omr != nil {
				p.metrics = newPubMetrics(omr.With(queuePubScope))
			} else {
				p.log.Warn().Msg("queue_pub metrics disabled: MetricsRegistry returned nil")
			}
		} else {
			p.log.Warn().Msg("queue_pub metrics disabled: metadata does not implement metadata.Service")
		}
	}

	// Initialize connection manager with the pub as the handler
	p.connMgr = grpchelper.NewConnManager(grpchelper.ConnManagerConfig[*client]{ //nolint:contextcheck // health check runs in background goroutine
		Handler:        p,
		Logger:         p.log,
		RetryPolicy:    p.retryPolicy,
		MaxRecvMsgSize: maxReceiveMessageSize,
	})

	// Initialize CA certificate reloader if TLS is enabled and CA cert path is provided
	if p.tlsEnabled && p.caCertPath != "" {
		var err error
		p.caCertReloader, err = pkgtls.NewClientCertReloader(p.caCertPath, p.log)
		if err != nil {
			return pkgerrors.Wrapf(err, "failed to initialize CA certificate reloader for %s", p.prefix)
		}
		p.log.Info().Str("caCertPath", p.caCertPath).Msg("Initialized CA certificate reloader")
	}

	return nil
}

func messageToRequest(topic bus.Topic, m bus.Message) (*clusterv1.SendRequest, error) {
	r := &clusterv1.SendRequest{
		Topic:     topic.String(),
		MessageId: uint64(m.ID()),
		BatchMod:  m.BatchModeEnabled(),
		VersionInfo: &clusterv1.VersionInfo{
			ApiVersion:                  apiversion.Version,
			FileFormatVersion:           storage.GetCurrentVersion(),
			CompatibleFileFormatVersion: storage.GetCompatibleVersions(),
		},
	}

	switch msgData := m.Data().(type) {
	case proto.Message:
		messageData, err := proto.Marshal(msgData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message %T: %w", m, err)
		}
		r.Body = messageData
		r.Group = apidata.GroupFromMessageData(topic, msgData)
	case []byte:
		r.Body = msgData
		r.Group = m.Group()
	default:
		return nil, fmt.Errorf("invalid message type %T", m.Data())
	}

	return r, nil
}

type future struct {
	log      *logger.Logger
	metrics  *pubMetrics
	clients  []clusterv1.Service_SendClient
	cancelFn []func()
	topics   []bus.Topic
	nodes    []string
	groups   []string
	roles    []string
	tiers    []string
	starts   []time.Time
}

func (l *future) Get() (msg bus.Message, err error) {
	if len(l.clients) < 1 {
		return bus.Message{}, io.EOF
	}

	c := l.clients[0]
	t := l.topics[0]
	n := l.nodes[0]
	group := l.groups[0]
	role := l.roles[0]
	tier := l.tiers[0]
	start := l.starts[0]

	errReason := sendErrReasonRecvError
	defer func() {
		if closeErr := c.CloseSend(); closeErr != nil {
			l.log.Error().Err(closeErr).Msg("failed to close send stream")
		}

		l.clients = l.clients[1:]
		l.topics = l.topics[1:]
		l.cancelFn[0]()
		l.cancelFn = l.cancelFn[1:]
		l.nodes = l.nodes[1:]
		l.groups = l.groups[1:]
		l.roles = l.roles[1:]
		l.tiers = l.tiers[1:]
		l.starts = l.starts[1:]

		// The non-batched publish path (query/control) records its finished and
		// latency here, when the response is read; errors are split by reason.
		if l.metrics != nil {
			operation := apidata.OperationOf(t)
			if err != nil {
				l.metrics.totalErr.Inc(1, operation, group, n, role, tier, errReason)
			} else {
				l.metrics.totalFinished.Inc(1, operation, group, n, role, tier)
			}
			l.metrics.totalLatency.Observe(time.Since(start).Seconds(), operation, group, n, role, tier)
		}
	}()
	resp, err := c.Recv()
	if err != nil {
		return bus.Message{}, err
	}
	if resp.Error != "" {
		errReason = sendErrReasonServerRejected
		return bus.Message{}, errors.New(resp.Error)
	}
	if resp.Body == nil {
		return bus.NewMessageWithNode(bus.MessageID(resp.MessageId), n, nil), nil
	}
	if codec, ok := apidata.TopicResponseMap[t]; ok {
		m, decodeErr := codec.Unmarshal(resp.Body)
		if decodeErr != nil {
			errReason = sendErrReasonDecodeError
			return bus.Message{}, decodeErr
		}
		return bus.NewMessageWithNode(
			bus.MessageID(resp.MessageId),
			n,
			m,
		), nil
	}
	errReason = sendErrReasonInvalidTopic
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

func (p *pub) getClientTransportCredentials() ([]grpc.DialOption, error) {
	if !p.tlsEnabled {
		return grpchelper.SecureOptions(nil, false, false, "")
	}

	// Use reloader if available (for dynamic reloading)
	if p.caCertReloader != nil {
		// Extract server name from the connection (we'll use a default for now)
		// The actual server name will be validated by the TLS handshake
		tlsConfig, err := p.caCertReloader.GetClientTLSConfig("")
		if err != nil {
			return nil, fmt.Errorf("failed to get TLS config from reloader: %w", err)
		}
		creds := credentials.NewTLS(tlsConfig)
		return []grpc.DialOption{grpc.WithTransportCredentials(creds)}, nil
	}

	// Fallback to static file reading if reloader is not available
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
	c, ok := p.connMgr.GetClient(node)
	if !ok {
		return nil, fmt.Errorf("no active client for node %s", node)
	}

	if config.ChunkSize == 0 {
		config.ChunkSize = defaultChunkSize
	}
	// Resolve the target node's role/tier so file-sync metrics carry the remote
	// service-level topology labels (the connMgr client already holds the Node).
	var info nodeInfo
	if n, ok := c.md.Spec.(*databasev1.Node); ok {
		info = resolveNodeInfo(n)
	}
	return &chunkedSyncClient{
		client:           c.client,
		conn:             c.conn,
		node:             node,
		log:              p.log,
		metrics:          p.metrics,
		migrationMetrics: p.migrationMetrics,
		selfNode:         p.selfNode,
		selfRole:         p.selfRole,
		selfTier:         p.selfTier,
		remoteRole:       info.role,
		remoteTier:       info.tier,
		chunkSize:        config.ChunkSize,
		config:           config,
	}, nil
}

// HealthyNodes returns a list of node names that are currently healthy and connected.
func (p *pub) HealthyNodes() []string {
	return p.connMgr.ActiveNames()
}

// NewNodeSchemaStatusClient borrows the underlying *grpc.ClientConn from the
// connection pool and wraps it as a clusterv1.NodeSchemaStatusServiceClient
// so the cluster-barrier fan-out (Step 2.2) can probe peer caches without
// opening parallel connections. The returned client shares the conn's HTTP/2
// streams with normal queue traffic; per-RPC contexts carry their own
// deadlines.
func (p *pub) NewNodeSchemaStatusClient(node string) (clusterv1.NodeSchemaStatusServiceClient, error) {
	c, ok := p.connMgr.GetClient(node)
	if !ok {
		return nil, fmt.Errorf("no active client for node %s", node)
	}
	return clusterv1.NewNodeSchemaStatusServiceClient(c.conn), nil
}
