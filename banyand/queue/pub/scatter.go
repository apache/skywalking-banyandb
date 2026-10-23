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

package pub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	apidata "github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var _ queue.NodeStream = (*nodeStream)(nil)

// Scatter implements queue.Client. It opens one Scan stream per selected node under ctx.
func (p *pub) Scatter(ctx context.Context, topic bus.Topic, message bus.Message, opts queue.ScatterOptions) ([]queue.NodeStream, error) {
	names, err := p.selectNodes(message)
	if err != nil {
		return nil, err
	}
	nodes := make([]string, 0, len(names))
	for n := range names {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	if opts.Prefetch <= 0 {
		opts.Prefetch = queue.DefaultScanPrefetch
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = p.scanIdleTimeout
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = defaultScanIdleTimeout
	}
	streams := make([]queue.NodeStream, 0, len(nodes))
	for _, n := range nodes {
		ns, openErr := p.openScan(ctx, n, topic, message, opts)
		if openErr != nil {
			for _, opened := range streams {
				opened.Close()
			}
			return nil, fmt.Errorf("failed to open a scan to %s: %w", n, openErr)
		}
		streams = append(streams, ns)
	}
	return streams, nil
}

func (p *pub) openScan(ctx context.Context, node string, topic bus.Topic, message bus.Message, opts queue.ScatterOptions) (*nodeStream, error) {
	req, err := messageToRequest(topic, message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message[%d]: %w", message.ID(), err)
	}
	group := message.Group()
	if req.Group != "" {
		group = req.Group
	}
	scanReq := &clusterv1.ScanRequest{
		Topic:         req.Topic,
		MessageId:     req.MessageId,
		Body:          req.Body,
		VersionInfo:   req.VersionInfo,
		Group:         group,
		SenderNode:    p.selfNode,
		SenderRole:    p.selfRole,
		SenderTier:    p.selfTier,
		MaxChunkBytes: opts.MaxChunkBytes,
		MaxChunkRows:  opts.MaxChunkRows,
	}
	info := p.getNodeInfo(node)
	operation := apidata.OperationOf(topic)
	ns := &nodeStream{
		node:        node,
		topic:       topic,
		prefetch:    opts.Prefetch,
		idleTimeout: opts.IdleTimeout,
		ch:          make(chan bus.Message, opts.Prefetch),
		done:        make(chan struct{}),
		closed:      make(chan struct{}),
		metrics:     p.metrics,
		labels:      [4]string{operation, group, node, info.role},
		tier:        info.tier,
		start:       time.Now(),
	}
	execErr := p.connMgr.Execute(node, func(c *client) error {
		streamCtx, cancel := context.WithCancel(ctx)
		// The client's default is wait-for-ready, so a node that never becomes ready would
		// block the open for as long as ctx lives; bound it by the idle timeout.
		openGuard := time.AfterFunc(opts.IdleTimeout, cancel)
		stream, openErr := c.client.Scan(streamCtx, scanReq)
		openGuard.Stop()
		if openErr == nil {
			openErr = streamCtx.Err()
		}
		if openErr != nil {
			cancel()
			if p.metrics != nil {
				p.metrics.totalErr.Inc(1, operation, group, node, info.role, info.tier, sendErrReasonSendError)
			}
			return fmt.Errorf("failed to open the scan stream: %w", openErr)
		}
		ns.stream = stream
		ns.cancel = cancel
		ns.streamCtx = streamCtx
		return nil
	})
	if execErr != nil {
		if grpchelper.IsFailoverError(execErr) && p.closer.AddRunning() {
			run.Go(context.Background(), "pub-failover", p.log, func(fctx context.Context) { //nolint:contextcheck // detached from the scan: the failover outlives it
				defer p.closer.Done()
				p.failover(fctx, node, common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, execErr.Error()), topic)
			})
		}
		return nil, execErr
	}
	if p.metrics != nil {
		p.metrics.totalStarted.Inc(1, operation, group, node, info.role, info.tier)
	}
	// The receive goroutine is tracked by the closer so that GracefulStop waits for it.
	if !p.closer.AddRunning() {
		ns.cancel()
		return nil, errors.New("the queue client is closed")
	}
	run.Go(context.Background(), "pub-scan-recv", p.log, func(_ context.Context) { //nolint:contextcheck // the receive loop is bound to the stream context
		defer p.closer.Done()
		ns.receive()
	})
	return ns, nil
}

// nodeStream is one node's Scan stream. A single goroutine receives from the wire,
// decodes and fills ch, so at most prefetch decoded chunks wait for the consumer.
type nodeStream struct {
	streamCtx context.Context
	// termErr is the terminal error; it is written before ch is closed.
	termErr     error
	stream      clusterv1.Service_ScanClient
	metrics     *pubMetrics
	cancel      context.CancelFunc
	ch          chan bus.Message
	done        chan struct{}
	closed      chan struct{}
	start       time.Time
	labels      [4]string
	node        string
	tier        string
	topic       bus.Topic
	prefetch    int
	idleTimeout time.Duration
	closeOnce   sync.Once
	idleFired   atomic.Bool
}

// Node implements queue.NodeStream.
func (n *nodeStream) Node() string { return n.node }

// Next implements queue.NodeStream.
func (n *nodeStream) Next(ctx context.Context) (bus.Message, error) {
	select {
	case <-n.closed:
		return bus.Message{}, queue.ErrStreamClosed
	default:
	}
	select {
	case m, ok := <-n.ch:
		if !ok {
			return bus.Message{}, n.termErr
		}
		return m, nil
	case <-ctx.Done():
		return bus.Message{}, ctx.Err()
	case <-n.closed:
		return bus.Message{}, queue.ErrStreamClosed
	}
}

// Close implements queue.NodeStream. It cancels the scan and waits for the receive goroutine.
func (n *nodeStream) Close() {
	n.closeOnce.Do(func() {
		close(n.closed)
		n.cancel()
		<-n.done
	})
}

// receive is the receive loop. It owns the terminal error and closes ch on exit.
func (n *nodeStream) receive() {
	defer close(n.done)
	idle := time.AfterFunc(n.idleTimeout, func() {
		n.idleFired.Store(true)
		n.cancel()
	})
	defer idle.Stop()
	errReason := ""
	defer func() {
		n.finish(errReason)
	}()
	for {
		resp, err := n.stream.Recv()
		if err != nil {
			errReason, n.termErr = n.classifyRecvError(err)
			close(n.ch)
			return
		}
		idle.Reset(n.idleTimeout)
		if resp.Error != "" {
			n.termErr = fmt.Errorf("scan on node %s failed: %w", n.node, remoteError(resp.Status, resp.Error))
			errReason = sendErrReasonServerRejected
			close(n.ch)
			return
		}
		if len(resp.Body) > 0 {
			msg, decodeErr := n.decode(resp)
			if decodeErr != nil {
				n.termErr, errReason = decodeErr, sendErrReasonDecodeError
				n.cancel()
				close(n.ch)
				return
			}
			// While the consumer lags, the node's silence says nothing about its health:
			// the heartbeat cannot be read either, so the idle clock is paused.
			idle.Stop()
			select {
			case n.ch <- msg:
			case <-n.streamCtx.Done():
				errReason, n.termErr = n.classifyRecvError(n.streamCtx.Err())
				close(n.ch)
				return
			}
			idle.Reset(n.idleTimeout)
		}
		if resp.Last {
			n.termErr = io.EOF
			n.cancel()
			close(n.ch)
			return
		}
	}
}

func (n *nodeStream) decode(resp *clusterv1.ScanResponse) (bus.Message, error) {
	codec, ok := apidata.TopicResponseMap[n.topic]
	if !ok {
		return bus.Message{}, fmt.Errorf("invalid topic %s", n.topic)
	}
	m, err := codec.Unmarshal(resp.Body)
	if err != nil {
		return bus.Message{}, fmt.Errorf("failed to decode a chunk from node %s: %w", n.node, err)
	}
	return bus.NewMessageWithNode(bus.MessageID(resp.MessageId), n.node, m), nil
}

// classifyRecvError turns a receive failure into the terminal error. It never returns io.EOF.
func (n *nodeStream) classifyRecvError(err error) (string, error) {
	switch {
	case n.idleFired.Load():
		return sendErrReasonIdleTimeout, fmt.Errorf("node %s sent nothing for %s: %w", n.node, n.idleTimeout, queue.ErrScanIdleTimeout)
	case isClosed(n.closed):
		return sendErrReasonStreamCanceled, queue.ErrStreamClosed
	case errors.Is(err, io.EOF):
		return sendErrReasonRecvError, fmt.Errorf("node %s ended the scan stream without a last chunk: %w", n.node, io.ErrUnexpectedEOF)
	case n.streamCtx.Err() != nil:
		return sendErrReasonCanceled, fmt.Errorf("scan on node %s canceled: %w", n.node, n.streamCtx.Err())
	default:
		return sendErrReasonRecvError, fmt.Errorf("scan on node %s failed: %w", n.node, err)
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (n *nodeStream) finish(errReason string) {
	if n.metrics == nil {
		return
	}
	op, group, node, role := n.labels[0], n.labels[1], n.labels[2], n.labels[3]
	if errReason != "" {
		n.metrics.totalErr.Inc(1, op, group, node, role, n.tier, errReason)
	} else {
		n.metrics.totalFinished.Inc(1, op, group, node, role, n.tier)
	}
	n.metrics.totalLatency.Observe(time.Since(n.start).Seconds(), op, group, node, role, n.tier)
}
