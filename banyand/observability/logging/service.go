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
	"fmt"
	"os"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	// writeTimeout bounds one batch publish. The consumer absorbs a slow
	// destination in the buffer rather than in the caller.
	writeTimeout = 5 * time.Second
	// drainTimeout bounds the final drain, so teardown is never held up by a
	// destination that has already stopped answering.
	drainTimeout = 5 * time.Second
	// schemaRetryInterval is how often a failed schema creation is retried.
	schemaRetryInterval = 10 * time.Second
)

// Service owns the consumer that drains the sink and writes what it finds.
//
// It is a run.Unit: everything that can fail against metadata happens in Serve
// and never in PreRun, because PreRun runs before the services it depends on.
type Service struct {
	metadata metadata.Repo
	pipeline queue.Client
	sink     *Sink
	closer   *run.Closer
	l        *logger.Logger
	pm       protector.Memory
	node     NodeInfo
	cfg      logger.NativeLogging
	ready    bool
}

// NewService returns the service that drains sink. The sink is constructed
// separately and installed on the logger before Init, so that the buffer
// exists for the lines emitted while the process is still starting.
func NewService(sink *Sink, cfg logger.NativeLogging, md metadata.Repo,
	pipeline queue.Client, pm protector.Memory,
) *Service {
	return &Service{
		sink:     sink,
		cfg:      cfg,
		metadata: md,
		pipeline: pipeline,
		pm:       pm,
		closer:   run.NewCloser(1),
	}
}

// Name implements run.Unit.
func (s *Service) Name() string { return "native-log" }

// PreRun implements run.PreRunner. It takes the node identity out of the
// context and touches nothing else: no metadata call, no publish.
func (s *Service) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	if val := ctx.Value(common.ContextNodeKey); val != nil {
		if node, ok := val.(common.Node); ok {
			s.node = NodeInfo{
				NodeID:      node.NodeID,
				GRPCAddress: node.GrpcAddress,
				HTTPAddress: node.HTTPAddress,
			}
		}
	}
	return nil
}

// SetNodeType records which role these logs belong to.
func (s *Service) SetNodeType(t string) { s.node.NodeType = t }

// Serve creates the schema, binds the budget to the memory protector and
// starts the consumer. It returns immediately: the consumer runs until the
// closer is notified.
func (s *Service) Serve() run.StopNotify {
	if !s.cfg.Enabled {
		s.closer.Done()
		return s.closer.CloseNotify()
	}
	s.sink.SetNode(s.node)
	s.bindBudget()

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	if err := createSchema(ctx, s.metadata, s.cfg.ShardNum, s.cfg.TTLDays); err != nil {
		// A failure here is not fatal: the buffer keeps accepting and the
		// consumer retries, so a late metadata service costs nothing permanent.
		s.l.Error().Err(err).Msg("failed to create the native log schema; will retry")
	} else {
		s.ready = true
	}
	cancel()

	go s.consume()
	return s.closer.CloseNotify()
}

// bindBudget replaces the configured cap with the adaptive budget wherever a
// memory protector is actually running. Where availability is unknown the cap
// is the only term, which is the steady state on any role whose protector is
// not registered.
func (s *Service) bindBudget() {
	if s.pm == nil {
		return
	}
	const (
		fraction = 0.02
		reserve  = int64(64 << 20)
	)
	cap := s.cfg.MaxBytes
	s.sink.SetBudget(func() int64 {
		available := s.pm.AvailableBytes()
		if available < 0 {
			// Unknown is not unlimited.
			return cap
		}
		headroom := available - reserve
		if headroom < 0 {
			headroom = 0
		}
		adaptive := int64(float64(headroom) * fraction)
		if adaptive < cap {
			return adaptive
		}
		return cap
	})
}

// consume is the single goroutine that drains the buffer. It selects over the
// closer, the flush ticker and the buffer itself, so both the interval and the
// size trigger can fire; a scheduled callback could only serve the first.
func (s *Service) consume() {
	defer s.closer.Done()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()
	retry := time.NewTicker(schemaRetryInterval)
	defer retry.Stop()

	batch := make([]*streamv1.WriteRequest, 0, s.cfg.FlushSize)
	stop := s.closer.CloseNotify()
	for {
		select {
		case <-stop:
			s.drain(batch)
			return
		case <-retry.C:
			s.retrySchema()
		case <-ticker.C:
			if len(batch) > 0 {
				s.flush(batch)
				batch = batch[:0]
			}
		case req := <-s.sink.queue:
			batch = append(batch, req)
			if len(batch) >= s.cfg.FlushSize {
				s.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (s *Service) retrySchema() {
	if s.ready {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	if err := createSchema(ctx, s.metadata, s.cfg.ShardNum, s.cfg.TTLDays); err == nil {
		s.ready = true
	}
}

// drain publishes whatever is still held at shutdown, bounded so that teardown
// is never blocked. Admission has already stopped by this point, so the set it
// publishes is exactly the set admitted before the cutoff.
func (s *Service) drain(batch []*streamv1.WriteRequest) {
	deadline := time.Now().Add(drainTimeout)
	for {
		select {
		case req := <-s.sink.queue:
			batch = append(batch, req)
			if len(batch) < s.cfg.FlushSize && time.Now().Before(deadline) {
				continue
			}
		default:
		}
		break
	}
	if len(batch) > 0 {
		s.flush(batch)
	}
	// Anything still queued past the deadline is lost, and counted rather than
	// discarded silently.
	for {
		select {
		case req := <-s.sink.queue:
			s.sink.queued.Add(-sizeOf(req))
			s.sink.release(req)
			s.sink.drop(reasonShutdown)
		default:
			return
		}
	}
}

// flush turns one batch into write requests and publishes it. Every path
// releases the requests back to the pool and settles the byte accounting, so a
// failure costs the batch and nothing more.
func (s *Service) flush(batch []*streamv1.WriteRequest) {
	var size int64
	for _, req := range batch {
		size += sizeOf(req)
	}
	defer func() {
		s.sink.queued.Add(-size)
		s.sink.inFlight.Add(-size)
		for _, req := range batch {
			s.sink.release(req)
		}
	}()
	s.sink.inFlight.Add(size)

	if !s.ready {
		s.sink.dropN(reasonSchemaMissing, len(batch))
		return
	}

	messages := make([]bus.Message, 0, len(batch))
	for _, req := range batch {
		iwr, err := s.internalRequest(req)
		if err != nil {
			s.sink.drop(reasonEncodeFailed)
			continue
		}
		messages = append(messages, bus.NewBatchMessageWithNode(
			bus.MessageID(time.Now().UnixNano()), "", iwr))
	}
	if len(messages) == 0 {
		return
	}

	publisher := s.pipeline.NewBatchPublisher(writeTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	_, err := publisher.Publish(ctx, data.TopicStreamWrite, messages...)
	if _, closeErr := publisher.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		// The sink never logs through the logging path it publishes on, so a
		// failure here goes straight to stderr rather than back into itself.
		s.sink.dropN(reasonPublishFailed, len(messages))
		reportf("native log publish failed: %v", err)
		return
	}
	s.sink.written.Add(uint64(len(messages)))
}

// internalRequest adds the routing the internal write path needs. The shard is
// computed from the entity the way the normal write path computes it, rather
// than the fixed shard zero the metric collector writes with.
func (s *Service) internalRequest(req *streamv1.WriteRequest) (*streamv1.InternalWriteRequest, error) {
	entity := entityOf(req)
	key, err := entity.ToEntity()
	if err != nil {
		return nil, err
	}
	shardID, err := partition.ShardID(key.Marshal(), s.cfg.ShardNum)
	if err != nil {
		return nil, err
	}
	return &streamv1.InternalWriteRequest{
		Request:      req,
		ShardId:      uint32(shardID),
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
// stranded in a buffer nobody will read again.
func (s *Service) GracefulStop() {
	logger.StopNative()
	s.closer.CloseThenWait()
}

// reportf writes the sink's own failures straight to stderr. Routing them
// through the logging path would feed the buffer that just failed.
func reportf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
}
