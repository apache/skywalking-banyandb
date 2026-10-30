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

package logging

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	// flushInterval is the longest a buffered event waits before it is written.
	flushInterval = time.Second
	// flushSize is the number of buffered events that triggers a write ahead of
	// the interval.
	flushSize = 100
	// writeTimeout is handed to every publish. The local pipeline does not
	// honor it, so the publisher's drain deadline is what bounds shutdown.
	writeTimeout = 5 * time.Second
	// drainTimeout bounds the final drain at shutdown, and with it
	// GracefulStop: a local write runs synchronously on the delivery goroutine
	// and does not honor its context, so the publisher stops waiting for a
	// stuck one at this deadline.
	drainTimeout = 5 * time.Second
	// schemaTimeout bounds one schema pass against metadata. It is separate
	// from writeTimeout: a slow destination is not a reason to wait longer on
	// the metadata service.
	schemaTimeout = 5 * time.Second
	// schemaRetryInterval is how often a failed schema creation is retried.
	schemaRetryInterval = 10 * time.Second
	// destinationRetries bounds how many ticks a batch is held while its
	// destination cannot take it: the local write topic has no subscriber, or a
	// liaison has no data node it reached. At the one-second interval that
	// covers ten seconds, the data client's default health check, so a batch
	// outlives the eviction of an unreachable node; past it the batch is
	// dropped and counted.
	destinationRetries = 10
)

// errSchemaUnavailable is the delivery failure while the schema has never
// been established.
var errSchemaUnavailable = errors.New("the native log schema is not established yet")

// errLocateFailed is the delivery failure of a liaison that located no data
// node for the log group.
var errLocateFailed = errors.New("no data node located for the native log group")

// NodeLocator locates the node that stores a shard of a group, as a liaison's
// node registry does.
type NodeLocator interface {
	Locate(group, name string, shardID, replicaID uint32) (string, error)
}

// timing holds the internal intervals. They are constants in production; a
// field lets a test shorten them.
type timing struct {
	reportInterval time.Duration
	schemaRetry    time.Duration
	writeTimeout   time.Duration
}

var defaultTiming = timing{reportInterval: flushInterval, schemaRetry: schemaRetryInterval, writeTimeout: writeTimeout}

// The states of the schema, as the last schema pass left it.
const (
	schemaUnready int32 = iota
	schemaReady
	schemaIncompatible
)

// Service establishes the schema, starts the sink's publisher with a delivery
// to the local write path, and reports the sink's counters.
//
// It is a run.Unit: everything that can fail against metadata happens in Serve
// and never in PreRun, because PreRun runs before the services it depends on.
type Service struct {
	metadata metadata.Repo
	omr      observability.MetricsRegistry
	metrics  *metrics
	pipeline queue.Client
	// nodes is set on a liaison, which has no storage engine: each batch goes
	// to the data node it locates. Elsewhere it is nil and batches are written
	// through the local pipeline.
	nodes  NodeLocator
	sink   *Sink
	closer *run.Closer
	// stopMaintain cancels the loop's context, so a schema pass in progress
	// does not hold GracefulStop for its whole timeout.
	stopMaintain context.CancelFunc
	l            *logger.Logger
	pm           protector.Memory
	cfg          *logger.NativeLogging
	node         NodeInfo
	timing       timing
	// schema is schemaReady, schemaUnready or schemaIncompatible. It is
	// written by the schema pass and read at every delivery. An incompatible
	// stream keeps the retry in place -- an operator can drop and recreate it
	// without restarting the node -- but until then the drops are counted
	// under their own reason, because waiting will not fix this one.
	schema    atomic.Int32
	warnedTTL bool
}

// NewService returns the service that drains sink. The sink is constructed
// separately and attached to the logger right after Init, so that the buffer
// exists for the lines emitted while the process is still starting.
func NewService(sink *Sink, cfg *logger.NativeLogging, md metadata.Repo,
	pipeline queue.Client, pm protector.Memory, omr observability.MetricsRegistry,
) *Service {
	return &Service{
		sink:     sink,
		omr:      omr,
		cfg:      cfg,
		metadata: md,
		pipeline: pipeline,
		pm:       pm,
		closer:   run.NewCloser(1),
		timing:   defaultTiming,
	}
}

// NewLiaisonService returns the service of a liaison. A liaison has no storage
// engine, so it sends each batch over client, its tier-2 client, to the data
// node nodes locates for shard 0 of the log group.
func NewLiaisonService(sink *Sink, cfg *logger.NativeLogging, md metadata.Repo,
	client queue.Client, nodes NodeLocator, pm protector.Memory, omr observability.MetricsRegistry,
) *Service {
	s := NewService(sink, cfg, md, client, pm, omr)
	s.nodes = nodes
	return s
}

// Name implements run.Unit.
func (s *Service) Name() string { return logger.NativeWriteModule }

// PreRun implements run.PreRunner. It takes the node identity out of the
// context and touches nothing else: no metadata call, no publish.
func (s *Service) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(logger.NativeWriteModule)
	if val := ctx.Value(common.ContextNodeKey); val != nil {
		if node, ok := val.(common.Node); ok {
			// Only the runtime identity: the role was set at construction
			// and replacing the whole struct would blank it, leaving every
			// stored row without the node_type the schema separates roles by.
			s.node.NodeID = node.NodeID
			s.node.GRPCAddress = node.GrpcAddress
			s.node.HTTPAddress = node.HTTPAddress
		}
	}
	return nil
}

// SetNodeType records which role these logs belong to.
func (s *Service) SetNodeType(t string) { s.node.NodeType = t }

// Serve creates the schema, connects the sink to the memory protector and
// starts delivery. It returns immediately: delivery runs until GracefulStop.
func (s *Service) Serve() run.StopNotify {
	if !s.cfg.Enabled {
		s.closer.Done()
		return s.closer.CloseNotify()
	}
	s.sink.SetNode(s.node)
	s.bindPressure()
	s.metrics = newMetrics(s.omr)

	ctx, cancel := context.WithTimeout(context.Background(), schemaTimeout)
	s.applySchema(createSchema(ctx, s.metadata, s.cfg.TTLDays))
	cancel()

	s.sink.pub.Start(s.delivery())
	// run.Go rather than a raw goroutine, so a panic in the loop is recovered
	// instead of taking the process down.
	ctx, s.stopMaintain = context.WithCancel(context.Background())
	run.Go(ctx, s.Name(), s.l, func(runCtx context.Context) {
		s.maintain(runCtx)
	})
	return s.closer.CloseNotify()
}

// delivery is what the publisher delivers each batch through.
func (s *Service) delivery() queue.Delivery {
	dest := queue.NewLocalDelivery(s.pipeline, data.TopicStreamWrite, s.timing.writeTimeout)
	if s.nodes != nil {
		dest = queue.NewRemoteDelivery(s.pipeline, data.TopicStreamWrite, s.locate, s.timing.writeTimeout)
	}
	return &delivery{svc: s, dest: dest}
}

// locate returns the data node that owns shard 0 of the log group, the shard
// every event is written to. It runs for every attempt, so a retried batch
// goes to the node that owns the shard by then.
func (s *Service) locate() (string, error) {
	node, err := s.nodes.Locate(GroupName, StreamName, shardID, 0)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errLocateFailed, err)
	}
	if node == "" {
		return "", errLocateFailed
	}
	return node, nil
}

// bindPressure makes the sink drop while the memory protector reports high
// pressure. State is the protector's nonblocking check: it reads counters and
// never waits, so it is safe on every goroutine that logs. Where no protector
// is registered it reports low pressure, and the buffer's own bounds hold.
func (s *Service) bindPressure() {
	if s.pm == nil {
		return
	}
	pm := s.pm
	s.sink.SetPressure(func() bool { return pm.State() == protector.StateHigh })
}

// maintain reports the sink's counters and re-runs the schema pass, until the
// closer is notified.
func (s *Service) maintain(ctx context.Context) {
	defer s.closer.Done()
	ticker := time.NewTicker(s.timing.reportInterval)
	defer ticker.Stop()
	retry := time.NewTicker(s.timing.schemaRetry)
	defer retry.Stop()
	stop := s.closer.CloseNotify()
	for {
		select {
		case <-stop:
			s.report()
			return
		case <-retry.C:
			s.retrySchema(ctx)
		case <-ticker.C:
			s.report()
		}
	}
}

// report publishes the sink's own counters. They travel the same transport as
// the events they count, so under native-only observability an outage loses
// both; keeping Prometheus enabled alongside is what makes the loss visible.
func (s *Service) report() {
	if s.metrics == nil {
		return
	}
	s.metrics.observe(s.sink)
}

// retrySchema re-runs the schema pass on every tick, including when the last
// one succeeded. Latching on the first success would mean the schema is checked
// exactly once per process: a group dropped at runtime would never be noticed,
// and because a refused local write is not reported per event, every later
// batch would be published into nothing and counted as written. createSchema is
// idempotent, so the healthy path costs two AlreadyExists replies and a read.
func (s *Service) retrySchema(ctx context.Context) {
	retryCtx, cancel := context.WithTimeout(ctx, schemaTimeout)
	defer cancel()
	s.applySchema(createSchema(retryCtx, s.metadata, s.cfg.TTLDays))
}

// applySchema records the outcome of one schema attempt. It is the only writer
// of ready and incompatible, so they can never disagree: a later
// success clears the incompatible flag an earlier attempt set, which is what
// lets an operator fix the stream without restarting the node.
//
// A transient metadata failure does not clear ready. Once the schema is known
// to exist, an unreachable metadata service is a reason to keep publishing on
// what was already established, not a reason to start dropping batches that
// would have been written.
func (s *Service) applySchema(state schemaState, err error) {
	switch {
	case err == nil:
		s.schema.Store(schemaReady)
		s.warnOnce(&s.warnedTTL, state.ttlMismatch)
	case errors.Is(err, errSchemaIncompatible):
		// Logged when the stream becomes incompatible, not on every pass: the
		// pass repeats every schemaRetryInterval, and an error line every ten
		// seconds for as long as the stream is wrong would bury everything
		// else. A later success clears the state, so a recurrence is logged too.
		if s.schema.Swap(schemaIncompatible) != schemaIncompatible {
			s.l.Error().Err(err).Msgf(
				"the %q stream in %q cannot store this version's log events; "+
					"drop and recreate it to enable native logging",
				StreamName, GroupName)
		}
	case s.schema.Load() != schemaReady:
		// Never established. Not fatal: the buffer keeps accepting and the
		// pass retries, so a late metadata service costs nothing permanent.
		s.l.Error().Err(err).Msg("failed to create the native log schema; will retry")
	default:
		// Established before and metadata is momentarily unreachable. Keep
		// publishing; a real rejection is reported by the publisher.
		s.l.Debug().Err(err).Msg("could not re-check the native log schema; keeping the last known state")
	}
}

// warnOnce reports a configuration that is being ignored, once per process.
// The schema pass now runs every tick, so an unguarded warning here would be
// the noisiest line the node emits.
func (s *Service) warnOnce(done *bool, msg string) {
	if msg == "" || *done {
		return
	}
	*done = true
	s.l.Warn().Msg(msg)
}

// delivery prepares each batch for its destination: it checks the schema,
// stamps the identity known by now, and marks the context so the write path's
// own logging about these events is never collected.
type delivery struct {
	svc   *Service
	dest  queue.Delivery
	batch []any
}

func (d *delivery) Deliver(ctx context.Context, batch []any) error {
	switch d.svc.schema.Load() {
	case schemaIncompatible:
		return errSchemaIncompatible
	case schemaUnready:
		return errSchemaUnavailable
	}
	d.batch = d.batch[:0]
	for _, item := range batch {
		r, ok := item.(*request)
		if !ok {
			continue
		}
		r.encodeFailed.Store(false)
		// The identity is known now even when it was not at admission.
		d.svc.sink.stamp(r)
		iwr, err := internalRequest(r.WriteRequest)
		if err != nil {
			// Counted by the request's outcome, which reads this flag.
			r.encodeFailed.Store(true)
			continue
		}
		d.batch = append(d.batch, iwr)
	}
	if len(d.batch) == 0 {
		return nil
	}
	// Marked here and not on the publisher's context: the publisher is built
	// before logging is initialized, and the mark is a logger.
	err := d.dest.Deliver(logger.WithNativeWrite(ctx), d.batch)
	clear(d.batch)
	return err
}

// internalRequest adds what the internal write path needs. The shard is always
// shard 0, as for native metrics: a standalone or data node writes to its own
// local pipeline, so its logs stay on the node that produced them, and a
// liaison sends to the data node that owns shard 0.
func internalRequest(req *streamv1.WriteRequest) (*streamv1.InternalWriteRequest, error) {
	entity := entityOf(req)
	if _, err := entity.ToEntity(); err != nil {
		return nil, err
	}
	return &streamv1.InternalWriteRequest{
		Request:      req,
		ShardId:      shardID,
		EntityValues: entity.Encode(),
	}, nil
}

// entityOf picks the entity tags out of the searchable family, in the order
// the schema declares them.
func entityOf(req *streamv1.WriteRequest) pbv1.EntityValues {
	tags := req.GetElement().GetTagFamilies()[0].GetTags()
	values := make(pbv1.EntityValues, 0, len(entityTags))
	for _, name := range entityTags {
		for i, declared := range searchableTags {
			if declared == name && i < len(tags) {
				values = append(values, tags[i])
			}
		}
	}
	return values
}

// GracefulStop stops admission before draining. Draining first would leave a
// window the length of one publish in which lines are admitted and then
// stranded in a buffer nobody will read again. The publisher's Close also
// refuses a line from a writer that loaded the sink before Detach.
//
// It returns within drainTimeout even when a write is stuck: a local write
// does not honor its context, so it cannot be interrupted, only left behind.
// Its events are counted as shutdown_deadline.
func (s *Service) GracefulStop() {
	s.sink.Detach()
	s.sink.pub.Close()
	if charged, _ := s.sink.pub.Bytes(); charged > 0 {
		reportf("native log delivery did not finish within %s; a write is stuck, and the %d bytes it holds are counted as lost",
			drainTimeout, charged)
	}
	if s.stopMaintain != nil {
		s.stopMaintain()
	}
	s.closer.CloseThenWait()
}

// reportf writes the sink's own failures straight to stderr. Routing them
// through the logging path would feed the buffer that just failed.
func reportf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
}
