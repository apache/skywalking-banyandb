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
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	rpcTimeout               = 2 * time.Second
	defaultJitterFactor      = 0.2
	defaultMaxRetries        = 3
	defaultCBThreshold       = 5
	defaultCBResetTimeout    = 60 * time.Second
	defaultPerRequestTimeout = 2 * time.Second
	defaultBackoffBase       = 500 * time.Millisecond
	defaultBackoffMax        = 30 * time.Second
)

var (
	// Retry policy for health check.
	initBackoff = time.Second
	maxBackoff  = 20 * time.Second

	// The timeout is set by each RPC.
	retryPolicy = `{
	"methodConfig": [
	  {
	    "name": [{"service": "banyandb.cluster.v1.Service"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".5s",
	        "MaxBackoff": "10s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.cluster.v1.ChunkedSyncService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 3,
	        "InitialBackoff": ".5s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.trace.v1.TraceService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".2s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.stream.v1.StreamService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".2s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.measure.v1.MeasureService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".2s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  },
	  {
	    "name": [{"service": "banyandb.property.v1.PropertyService"}],
	    "waitForReady": true,
	    "retryPolicy": {
	        "MaxAttempts": 4,
	        "InitialBackoff": ".2s",
	        "MaxBackoff": "5s",
	        "BackoffMultiplier": 2.0,
	        "RetryableStatusCodes": [ "UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED" ]
	    }
	  }
	]}`

	// Retryable gRPC status codes for streaming send retries.
	retryableCodes = map[codes.Code]bool{
		codes.OK:                 false,
		codes.Canceled:           false,
		codes.Unknown:            false,
		codes.InvalidArgument:    false,
		codes.DeadlineExceeded:   true, // Retryable - operation exceeded deadline
		codes.NotFound:           false,
		codes.AlreadyExists:      false,
		codes.PermissionDenied:   false,
		codes.ResourceExhausted:  true, // Retryable - server resource limits exceeded
		codes.FailedPrecondition: false,
		codes.Aborted:            false,
		codes.OutOfRange:         false,
		codes.Unimplemented:      false,
		codes.Internal:           true, // Retryable - internal server error should participate in circuit breaker
		codes.Unavailable:        true, // Retryable - service temporarily unavailable
		codes.DataLoss:           false,
		codes.Unauthenticated:    false,
	}
)

// CircuitState defines the circuit breaker states.
type CircuitState int

// CircuitState defines the circuit breaker states.
const (
	StateClosed   CircuitState = iota // Normal operation
	StateOpen                         // Reject requests until cooldown expires
	StateHalfOpen                     // Allow a single probe
)

// circuitState holds circuit breaker metadata; it does NOT duplicate gRPC clients/conns.
type circuitState struct {
	lastFailureTime       time.Time
	openTime              time.Time
	state                 CircuitState
	consecutiveFailures   int
	halfOpenProbeInFlight bool
}

type client struct {
	client clusterv1.ServiceClient
	conn   *grpc.ClientConn
	md     schema.Metadata
}

func (p *pub) OnAddOrUpdate(md schema.Metadata) {
	if md.Kind != schema.KindNode {
		return
	}
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		p.log.Warn().Msg("failed to cast node spec")
		return
	}
	var okRole bool
	for _, r := range node.Roles {
		for _, allowed := range p.allowedRoles {
			if r == allowed {
				okRole = true
				break
			}
		}
		if okRole {
			break
		}
	}
	if !okRole {
		return
	}

	address := node.GrpcAddress
	if address == "" {
		p.log.Warn().Stringer("node", node).Msg("grpc address is empty")
		return
	}
	name := node.Metadata.GetName()
	if name == "" {
		p.log.Warn().Stringer("node", node).Msg("node name is empty")
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.registerNode(node)

	if _, ok := p.active[name]; ok {
		return
	}
	if _, ok := p.evictable[name]; ok {
		return
	}
	credOpts, err := p.getClientTransportCredentials()
	if err != nil {
		p.log.Error().Err(err).Msg("failed to load client TLS credentials")
		return
	}
	conn, err := grpc.NewClient(address, append(credOpts, grpc.WithDefaultServiceConfig(retryPolicy))...)
	if err != nil {
		p.log.Error().Err(err).Msg("failed to connect to grpc server")
		return
	}

	if !p.checkClientHealthAndReconnect(conn, md) {
		p.log.Info().Str("status", p.dump()).Stringer("node", node).Msg("node is unhealthy in the register flow, move it to evict queue")
		return
	}

	c := clusterv1.NewServiceClient(conn)
	p.active[name] = &client{conn: conn, client: c, md: md}
	p.addClient(md)
	// Initialize or reset circuit breaker state to closed
	p.recordSuccess(name)
	p.log.Info().Str("status", p.dump()).Stringer("node", node).Msg("new node is healthy, add it to active queue")
}

func (p *pub) registerNode(node *databasev1.Node) {
	name := node.Metadata.GetName()
	defer func() {
		p.registered[name] = node
	}()

	n, ok := p.registered[name]
	if !ok {
		return
	}
	if n.GrpcAddress == node.GrpcAddress {
		return
	}
	if en, ok := p.evictable[name]; ok {
		close(en.c)
		delete(p.evictable, name)
		p.log.Info().Str("node", name).Str("status", p.dump()).Msg("node is removed from evict queue by the new gRPC address updated event")
	}
	if client, ok := p.active[name]; ok {
		_ = client.conn.Close()
		delete(p.active, name)
		p.deleteClient(client.md)
		p.log.Info().Str("status", p.dump()).Str("node", name).Msg("node is removed from active queue by the new gRPC address updated event")
	}
}

func (p *pub) OnDelete(md schema.Metadata) {
	if md.Kind != schema.KindNode {
		return
	}
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		p.log.Warn().Msg("failed to cast node spec")
		return
	}
	name := node.Metadata.GetName()
	if name == "" {
		p.log.Warn().Stringer("node", node).Msg("node name is empty")
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.registered, name)
	if en, ok := p.evictable[name]; ok {
		close(en.c)
		delete(p.evictable, name)
		p.log.Info().Str("status", p.dump()).Stringer("node", node).Msg("node is removed from evict queue by delete event")
		return
	}

	if client, ok := p.active[name]; ok {
		if p.removeNodeIfUnhealthy(md, node, client) {
			p.log.Info().Str("status", p.dump()).Stringer("node", node).Msg("remove node from active queue by delete event")
			return
		}
		if !p.closer.AddRunning() {
			return
		}
		go func() {
			defer p.closer.Done()
			var elapsed time.Duration
			attempt := 0
			for {
				backoff := jitteredBackoff(initBackoff, maxBackoff, attempt, defaultJitterFactor)
				select {
				case <-time.After(backoff):
					if func() bool {
						elapsed += backoff
						p.mu.Lock()
						defer p.mu.Unlock()
						if _, ok := p.registered[name]; ok {
							// The client has been added back to registered clients map, just return
							return true
						}
						if p.removeNodeIfUnhealthy(md, node, client) {
							p.log.Info().Str("status", p.dump()).Stringer("node", node).Dur("after", elapsed).Msg("remove node from active queue by delete event")
							return true
						}
						return false
					}() {
						return
					}
				case <-p.closer.CloseNotify():
					return
				}
				attempt++
			}
		}()
	}
}

func (p *pub) removeNodeIfUnhealthy(md schema.Metadata, node *databasev1.Node, client *client) bool {
	if p.healthCheck(node.String(), client.conn) {
		return false
	}
	_ = client.conn.Close()
	name := node.Metadata.GetName()
	delete(p.active, name)
	p.deleteClient(md)
	return true
}

// secureRandFloat64 generates a cryptographically secure random float64 in [0, 1).
func secureRandFloat64() float64 {
	// Generate a random uint64
	maxVal := big.NewInt(1 << 53) // Use 53 bits for precision similar to math/rand
	n, err := rand.Int(rand.Reader, maxVal)
	if err != nil {
		// Fallback to a reasonable value if crypto/rand fails
		return 0.5
	}
	return float64(n.Uint64()) / float64(1<<53)
}

// jitteredBackoff calculates backoff duration with jitter to avoid thundering herds.
// Uses bounded symmetric jitter: backoff * (1 + jitter * (rand() - 0.5) * 2).
func jitteredBackoff(baseBackoff, maxBackoff time.Duration, attempt int, jitterFactor float64) time.Duration {
	if jitterFactor < 0 {
		jitterFactor = 0
	}
	if jitterFactor > 1 {
		jitterFactor = 1
	}

	// Exponential backoff: base * 2^attempt
	backoff := baseBackoff
	for i := 0; i < attempt; i++ {
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
			break
		}
	}

	// Apply jitter: backoff * (1 + jitter * (rand() - 0.5) * 2)
	// This gives us a range of [backoff * (1-jitter), backoff * (1+jitter)]
	jitterRange := float64(backoff) * jitterFactor
	randomFloat := secureRandFloat64()
	randomOffset := (randomFloat - 0.5) * 2 * jitterRange

	jitteredDuration := time.Duration(float64(backoff) + randomOffset)
	if jitteredDuration < 0 {
		jitteredDuration = baseBackoff / 10 // Minimum backoff
	}
	if jitteredDuration > maxBackoff {
		jitteredDuration = maxBackoff
	}

	return jitteredDuration
}

// isTransientError checks if the error is considered transient and retryable.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	if s, ok := status.FromError(err); ok {
		return retryableCodes[s.Code()]
	}

	return false
}

// isInternalError checks if the error is an internal server error.
func isInternalError(err error) bool {
	if err == nil {
		return false
	}

	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Internal
	}

	return false
}

func (p *pub) checkClientHealthAndReconnect(conn *grpc.ClientConn, md schema.Metadata) bool {
	node, ok := md.Spec.(*databasev1.Node)
	if !ok {
		logger.Panicf("failed to cast node spec")
		return false
	}
	if p.healthCheck(node.String(), conn) {
		return true
	}
	_ = conn.Close()
	if !p.closer.AddRunning() {
		return false
	}
	name := node.Metadata.Name
	p.evictable[name] = evictNode{n: node, c: make(chan struct{})}
	p.deleteClient(md)
	go func(p *pub, name string, en evictNode, md schema.Metadata) {
		defer p.closer.Done()
		attempt := 0
		for {
			backoff := jitteredBackoff(initBackoff, maxBackoff, attempt, defaultJitterFactor)
			select {
			case <-time.After(backoff):
				credOpts, errEvict := p.getClientTransportCredentials()
				if errEvict != nil {
					p.log.Error().Err(errEvict).Msg("failed to load client TLS credentials (evict)")
					return
				}
				connEvict, errEvict := grpc.NewClient(node.GrpcAddress, append(credOpts, grpc.WithDefaultServiceConfig(retryPolicy))...)
				if errEvict == nil && p.healthCheck(en.n.String(), connEvict) {
					func() {
						p.mu.Lock()
						defer p.mu.Unlock()
						if _, ok := p.evictable[name]; !ok {
							// The client has been removed from evict clients map, just return
							return
						}
						c := clusterv1.NewServiceClient(connEvict)
						p.active[name] = &client{conn: connEvict, client: c, md: md}
						p.addClient(md)
						delete(p.evictable, name)
						// Reset circuit breaker state to closed
						p.recordSuccess(name)
						p.log.Info().Str("status", p.dump()).Stringer("node", en.n).Msg("node is healthy, move it back to active queue")
					}()
					return
				}
				_ = connEvict.Close()
				if _, ok := p.registered[name]; !ok {
					return
				}
				p.log.Error().Err(errEvict).Msgf("failed to re-connect to grpc server %s after waiting for %s", node.GrpcAddress, backoff)
			case <-en.c:
				return
			case <-p.closer.CloseNotify():
				return
			}
			attempt++
		}
	}(p, name, p.evictable[name], md)
	return false
}

func (p *pub) healthCheck(node string, conn *grpc.ClientConn) bool {
	var resp *grpc_health_v1.HealthCheckResponse
	if err := grpchelper.Request(context.Background(), rpcTimeout, func(rpcCtx context.Context) (err error) {
		resp, err = grpc_health_v1.NewHealthClient(conn).Check(rpcCtx,
			&grpc_health_v1.HealthCheckRequest{
				Service: "",
			})
		return err
	}); err != nil {
		if e := p.log.Debug(); e.Enabled() {
			e.Err(err).Str("node", node).Msg("service unhealthy")
		}
		return false
	}
	if resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
		return true
	}
	return false
}

func (p *pub) checkServiceHealth(svc string, conn *grpc.ClientConn) *common.Error {
	client := clusterv1.NewServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := client.HealthCheck(ctx, &clusterv1.HealthCheckRequest{
		ServiceName: svc,
	})
	if err != nil {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, err.Error())
	}
	if resp.Status == modelv1.Status_STATUS_SUCCEED {
		return nil
	}
	return common.NewErrorWithStatus(resp.Status, resp.Error)
}

func (p *pub) failover(node string, ce *common.Error, topic bus.Topic) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ce.Status() != modelv1.Status_STATUS_INTERNAL_ERROR {
		_, _ = p.checkWritable(node, topic)
		return
	}
	if en, evictable := p.evictable[node]; evictable {
		if _, registered := p.registered[node]; !registered {
			close(en.c)
			delete(p.evictable, node)
			p.log.Info().Str("node", node).Str("status", p.dump()).Msg("node is removed from evict queue by wire event")
		}
		return
	}

	if client, ok := p.active[node]; ok && !p.checkClientHealthAndReconnect(client.conn, client.md) {
		_ = client.conn.Close()
		delete(p.active, node)
		p.deleteClient(client.md)
		p.log.Info().Str("status", p.dump()).Str("node", node).Msg("node is unhealthy in the failover flow, move it to evict queue")
	}
}

func (p *pub) checkWritable(n string, topic bus.Topic) (bool, *common.Error) {
	h, ok := p.handlers[topic]
	if !ok {
		return false, nil
	}
	node, ok := p.active[n]
	if !ok {
		return false, nil
	}
	topicStr := topic.String()
	err := p.checkServiceHealth(topicStr, node.conn)
	if err == nil {
		return true, nil
	}
	h.OnDelete(node.md)
	if !p.closer.AddRunning() {
		return false, err
	}
	// Deduplicate concurrent probes for the same node/topic.
	p.writableProbeMu.Lock()
	if _, ok := p.writableProbe[n]; !ok {
		p.writableProbe[n] = make(map[string]struct{})
	}
	if _, exists := p.writableProbe[n][topicStr]; exists {
		p.writableProbeMu.Unlock()
		return false, err
	}
	p.writableProbe[n][topicStr] = struct{}{}
	p.writableProbeMu.Unlock()

	go func(nodeName, t string) {
		defer p.closer.Done()
		defer func() {
			p.writableProbeMu.Lock()
			if topics, ok := p.writableProbe[nodeName]; ok {
				delete(topics, t)
				if len(topics) == 0 {
					delete(p.writableProbe, nodeName)
				}
			}
			p.writableProbeMu.Unlock()
		}()
		attempt := 0
		for {
			backoff := jitteredBackoff(initBackoff, maxBackoff, attempt, defaultJitterFactor)
			select {
			case <-time.After(backoff):
				if errInternal := p.checkServiceHealth(t, node.conn); errInternal == nil {
					func() {
						p.mu.Lock()
						defer p.mu.Unlock()
						nodeCur, okCur := p.active[nodeName]
						if !okCur {
							return
						}
						// Record success for circuit breaker
						p.recordSuccess(nodeName)
						h.OnAddOrUpdate(nodeCur.md)
					}()
					return
				}
				p.log.Warn().Str("topic", t).Err(err).Str("node", nodeName).Dur("backoff", backoff).Msg("data node can not ingest data")
			case <-p.closer.CloseNotify():
				return
			}
			attempt++
		}
	}(n, topicStr)
	return false, err
}

func (p *pub) deleteClient(md schema.Metadata) {
	if len(p.handlers) > 0 {
		for _, h := range p.handlers {
			h.OnDelete(md)
		}
	}
}

func (p *pub) addClient(md schema.Metadata) {
	if len(p.handlers) > 0 {
		for _, h := range p.handlers {
			h.OnAddOrUpdate(md)
		}
	}
}

func (p *pub) dump() string {
	keysRegistered := make([]string, 0, len(p.registered))
	for k := range p.registered {
		keysRegistered = append(keysRegistered, k)
	}
	keysActive := make([]string, 0, len(p.active))
	for k := range p.active {
		keysActive = append(keysActive, k)
	}
	keysEvictable := make([]string, 0, len(p.evictable))
	for k := range p.evictable {
		keysEvictable = append(keysEvictable, k)
	}
	return fmt.Sprintf("registered: %v, active :%v, evictable :%v", keysRegistered, keysActive, keysEvictable)
}

type evictNode struct {
	n *databasev1.Node
	c chan struct{}
}

// isRequestAllowed checks if a request to the given node is allowed based on circuit breaker state.
// It also handles state transitions from Open to Half-Open when cooldown expires.
func (p *pub) isRequestAllowed(node string) bool {
	p.cbMu.Lock()
	defer p.cbMu.Unlock()

	cb, exists := p.cbStates[node]
	if !exists {
		return true // No circuit breaker state, allow request
	}

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if cooldown period has expired
		if time.Since(cb.openTime) >= defaultCBResetTimeout {
			// Transition to Half-Open to allow a single probe request
			cb.state = StateHalfOpen
			cb.halfOpenProbeInFlight = true // Set token for the probe
			p.log.Info().Str("node", node).Msg("circuit breaker transitioned to half-open")
			return true
		}
		return false // Still in cooldown period
	case StateHalfOpen:
		// In half-open state, deny requests if probe is already in flight
		if cb.halfOpenProbeInFlight {
			return false // Probe already in progress, deny additional requests
		}
		// This case should not normally happen since we set the token on transition,
		// but handle it defensively by allowing the request and setting the token
		cb.halfOpenProbeInFlight = true
		return true
	default:
		return true
	}
}

// recordSuccess resets the circuit breaker state to Closed on successful operation.
// This handles Half-Open -> Closed transitions.
func (p *pub) recordSuccess(node string) {
	p.cbMu.Lock()
	defer p.cbMu.Unlock()

	cb, exists := p.cbStates[node]
	if !exists {
		// Initialize circuit breaker state
		p.cbStates[node] = &circuitState{
			state:               StateClosed,
			consecutiveFailures: 0,
		}
		return
	}

	// Reset to closed state
	cb.state = StateClosed
	cb.consecutiveFailures = 0
	cb.lastFailureTime = time.Time{}
	cb.openTime = time.Time{}
	cb.halfOpenProbeInFlight = false // Clear probe token
}

// recordFailure updates the circuit breaker state on failed operation.
// Only records failures for transient/internal errors that should count toward opening the circuit.
func (p *pub) recordFailure(node string, err error) {
	// Only record failure if the error is transient or internal
	if !isTransientError(err) && !isInternalError(err) {
		return
	}
	p.cbMu.Lock()
	defer p.cbMu.Unlock()

	cb, exists := p.cbStates[node]
	if !exists {
		// Initialize circuit breaker state
		cb = &circuitState{
			state:               StateClosed,
			consecutiveFailures: 1,
			lastFailureTime:     time.Now(),
		}
		p.cbStates[node] = cb
	} else {
		cb.consecutiveFailures++
		cb.lastFailureTime = time.Now()
	}

	// Check if we should open the circuit
	threshold := defaultCBThreshold
	if cb.consecutiveFailures >= threshold && cb.state == StateClosed {
		cb.state = StateOpen
		cb.openTime = time.Now()
		p.log.Warn().Str("node", node).Int("failures", cb.consecutiveFailures).Msg("circuit breaker opened")
	} else if cb.state == StateHalfOpen {
		// Failed during half-open, go back to open
		cb.state = StateOpen
		cb.openTime = time.Now()
		cb.halfOpenProbeInFlight = false // Clear probe token
		p.log.Warn().Str("node", node).Msg("circuit breaker reopened after half-open failure")
	}
}
