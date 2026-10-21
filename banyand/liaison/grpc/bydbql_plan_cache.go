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

package grpc

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type planCacheConfig struct {
	Budget      *protector.QueryBudget
	MaxEntries  int
	MaxBytes    int64
	MaxCompiles int
	MaxInFlight int
}

type compiledPlanEntry struct {
	template     *bydbql.MeasureTemplate
	element      *list.Element
	reservation  *planCompileReservation
	dependencies []measurePlanDependency
	cost         int64
	references   int
	cached       bool
}

type compiledPlanFlight struct {
	statement *bydbql.PreparedStatement
	entry     *compiledPlanEntry
	done      chan struct{}
	cancel    context.CancelFunc
	err       error
	key       string
	cost      int64
	waiters   int
	complete  bool
}

type compiledPlanHandle struct {
	cache    *compiledPlanCache
	template *bydbql.MeasureTemplate
	entry    *compiledPlanEntry
	once     sync.Once
}

func (h *compiledPlanHandle) Release() {
	if h == nil {
		return
	}
	h.once.Do(func() {
		h.cache.mu.Lock()
		defer h.cache.mu.Unlock()
		h.cache.releaseEntry(h.entry)
		h.cache.publishStats()
	})
}

func (h *compiledPlanHandle) valid() bool {
	return h != nil && h.cache.validate(h.entry.dependencies)
}

type compiledPlanCache struct {
	source         *entityRepoSchemaSource
	transformer    *bydbql.Transformer
	metrics        *metrics
	budget         *protector.QueryBudget
	entries        map[string]*compiledPlanEntry
	flights        map[string]*compiledPlanFlight
	slots          chan struct{}
	stop           context.Context
	cancel         context.CancelFunc
	order          list.List
	cfg            planCacheConfig
	workers        sync.WaitGroup
	mu             sync.Mutex
	stats          PlanCacheStats
	reported       PlanCacheStats
	flightBytes    int64
	retainedCount  int
	runningFlights int
	closed         bool
}

func newCompiledPlanCache(cfg planCacheConfig, source *entityRepoSchemaSource, transformer *bydbql.Transformer, m *metrics) *compiledPlanCache {
	if cfg.MaxCompiles <= 0 {
		cfg.MaxCompiles = 4
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 64
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 64 << 20
	}
	budget := cfg.Budget
	if budget == nil {
		budget = protector.QueryBudgetFor(nil)
	}
	stop, cancel := context.WithCancel(context.Background())
	cache := &compiledPlanCache{
		cfg: cfg, source: source, transformer: transformer, metrics: m, budget: budget, stop: stop, cancel: cancel,
		entries: make(map[string]*compiledPlanEntry), flights: make(map[string]*compiledPlanFlight), slots: make(chan struct{}, cfg.MaxCompiles),
	}
	registerCompiledPlanCache(cache)
	return cache
}

func (c *compiledPlanCache) snapshot() PlanCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *compiledPlanCache) publishStats() {
	c.stats.Entries = int64(len(c.entries))
	c.stats.InFlight = int64(c.runningFlights)
	c.stats.RetiredBytes = c.stats.RetainedBytes - c.stats.EntryBytes
	recordPlanCacheStats(c.reported, c.stats)
	c.emitMetrics()
	c.reported = c.stats
}

func (c *compiledPlanCache) acquire(ctx context.Context, key string, statement *bydbql.PreparedStatement) (*compiledPlanHandle, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if len(statement.MeasureSources()) == 0 {
		return nil, "bypass", nil
	}
	hooks := bydbQLPlanCacheHooks()
	if c.cfg.MaxEntries <= 0 || statement.NumPlaceholders() == 0 || hooks.ForceBypass || c.source == nil {
		return c.uncached(ctx, statement)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, "", context.Canceled
	}
	if entry := c.entries[key]; entry != nil {
		if c.validate(entry.dependencies) {
			c.order.MoveToFront(entry.element)
			entry.references++
			c.stats.Hits++
			handle := &compiledPlanHandle{cache: c, template: entry.template, entry: entry}
			c.publishStats()
			c.mu.Unlock()
			return handle, "hit", nil
		}
		c.stats.Stale++
		c.removeEntry(entry)
	}
	c.stats.Misses++
	if flight := c.flights[key]; flight != nil {
		flight.waiters++
		c.stats.Waiters++
		c.stats.Coalesced++
		c.publishStats()
		c.mu.Unlock()
		return c.await(ctx, flight)
	}
	cost := planFlightCost(statement, key)
	if c.runningFlights >= c.cfg.MaxInFlight || cost > c.cfg.MaxBytes-c.flightBytes {
		c.publishStats()
		c.mu.Unlock()
		return c.uncached(ctx, statement)
	}
	// Compile lifetime belongs to all waiters, with no request-owned context values.
	workCtx, cancel := context.WithTimeout(c.stop, 30*time.Second)
	flight := &compiledPlanFlight{statement: statement, key: key, done: make(chan struct{}), cancel: cancel, waiters: 1, cost: cost}
	c.flights[key] = flight
	c.flightBytes += cost
	c.runningFlights++
	c.stats.Waiters++
	c.workers.Add(1)
	c.publishStats()
	c.mu.Unlock()
	//nolint:contextcheck // Shared work deliberately has no caller's values or cancellation.
	run.Go(workCtx, "bydbql-plan-compile", logger.GetLogger("bydbql-plan-cache"), func(taskCtx context.Context) {
		defer c.workers.Done()
		c.compile(taskCtx, flight)
	})
	return c.await(ctx, flight)
}

func (c *compiledPlanCache) await(ctx context.Context, flight *compiledPlanFlight) (*compiledPlanHandle, string, error) {
	select {
	case <-ctx.Done():
	case <-flight.done:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	flight.waiters--
	c.stats.Waiters--
	var handle *compiledPlanHandle
	err := ctx.Err()
	if err == nil && flight.complete {
		err = flight.err
		if err == nil && flight.entry != nil {
			flight.entry.references++
			handle = &compiledPlanHandle{cache: c, template: flight.entry.template, entry: flight.entry}
		}
	}
	if flight.waiters == 0 {
		if !flight.complete {
			flight.cancel()
			// A new caller must not join abandoned work. The detached flight remains
			// charged against both bounds until its worker finishes.
			if c.flights[flight.key] == flight {
				delete(c.flights, flight.key)
			}
		} else if flight.entry != nil {
			c.releaseEntry(flight.entry)
		}
	}
	c.publishStats()
	return handle, "miss", err
}

func (c *compiledPlanCache) compile(ctx context.Context, flight *compiledPlanFlight) {
	var entry *compiledPlanEntry
	var reservation *planCompileReservation
	compileErr := errors.New("measure plan compilation interrupted")
	defer func() {
		reservation.release(c)
		c.finish(flight, entry, compileErr)
	}()
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		compileErr = ctx.Err()
		return
	}
	for attempt := 0; attempt < 2; attempt++ {
		dependencies, schemas, coherent := c.source.captureMeasureDependencies(flight.statement.MeasureSources())
		if !coherent {
			compileErr = nil
			return
		}
		bound := compiledPlanWorkBound(flight.statement, schemas)
		var reserveErr error
		reservation, reserveErr = c.reserveWork(ctx, bound)
		if reserveErr != nil {
			compileErr = reserveErr
			return
		}
		c.mu.Lock()
		c.stats.Compiles++
		c.publishStats()
		c.mu.Unlock()
		if hook := bydbQLPlanCacheHooks().BeforeCompile; hook != nil {
			hook(ctx, flight.key)
		}
		if compileErr = ctx.Err(); compileErr != nil {
			return
		}
		var template *bydbql.MeasureTemplate
		started := time.Now()
		template, compileErr = c.transformer.CompileMeasureWithSchemas(ctx, flight.statement, schemas)
		c.observeDuration("compile", started)
		if compileErr != nil {
			return
		}
		if !c.validate(dependencies) {
			reservation.release(c)
			reservation = nil
			c.mu.Lock()
			c.stats.Stale++
			c.publishStats()
			c.mu.Unlock()
			continue
		}
		cost := planCacheSize(template.EstimatedSize(), uint64(len(flight.key)), 256, uint64(len(dependencies))*96)
		entry = &compiledPlanEntry{template: template, dependencies: dependencies, cost: cost, references: 1}
		c.mu.Lock()
		if ctx.Err() != nil || c.closed || flight.waiters == 0 {
			c.mu.Unlock()
			entry = nil
			compileErr = context.Canceled
			return
		}
		if !c.validate(dependencies) {
			entry = nil
			c.stats.Stale++
			c.publishStats()
			c.mu.Unlock()
			reservation.release(c)
			reservation = nil
			continue
		}
		if c.makeRoom(cost) {
			entry.cached = true
			entry.element = c.order.PushFront(flight.key)
			c.entries[flight.key] = entry
			c.stats.EntryBytes += cost
			c.stats.RetainedBytes += cost
			c.retainedCount++
		} else {
			c.stats.Oversize++
			entry.reservation = reservation
			reservation = nil
			c.stats.UncachedBytes += cost
		}
		c.publishStats()
		c.mu.Unlock()
		compileErr = nil
		return
	}
	// The local schema keeps changing; execute uncached under normal schema gates.
	compileErr = nil
}

func compiledPlanWorkBound(statement *bydbql.PreparedStatement, schemas map[bydbql.MeasureSource]*databasev1.Measure) uint64 {
	size := planCacheSize(statement.EstimatedSize(), uint64(len(statement.QueryText())), 8192)
	for _, schema := range schemas {
		size = planCacheSize(int(size), uint64(proto.Size(schema)))
	}
	// Include snapshot clones, typed maps/recipes and transient builder capacity.
	if uint64(size) > (uint64(1<<63-1)-(64<<10))/32 {
		return uint64(1<<63 - 1)
	}
	return uint64(size)*32 + (64 << 10)
}

type planCompileReservation struct {
	releaseBudget func()
	bytes         int64
}

func (r *planCompileReservation) release(c *compiledPlanCache) {
	if r == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r.releaseLocked(c)
	c.publishStats()
}

func (r *planCompileReservation) releaseLocked(c *compiledPlanCache) {
	if r.releaseBudget == nil {
		return
	}
	r.releaseBudget()
	r.releaseBudget = nil
	c.stats.CompileReservedBytes -= r.bytes
}

func (c *compiledPlanCache) reserveWork(ctx context.Context, bound uint64) (*planCompileReservation, error) {
	release, err := c.budget.Reserve(ctx, bound)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.stats.CompileReservedBytes += int64(bound)
	c.publishStats()
	c.mu.Unlock()
	return &planCompileReservation{releaseBudget: release, bytes: int64(bound)}, nil
}

func (c *compiledPlanCache) uncached(ctx context.Context, statement *bydbql.PreparedStatement) (*compiledPlanHandle, string, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, "bypass", context.Canceled
	}
	c.workers.Add(1)
	c.mu.Unlock()
	defer c.workers.Done()
	workCtx, cancel := context.WithCancel(ctx)
	//nolint:contextcheck // Shutdown must cancel work independently of the caller's context.
	stop := context.AfterFunc(c.stop, cancel)
	defer func() { stop(); cancel() }()
	ctx = workCtx
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return nil, "bypass", ctx.Err()
	}
	// Missing local schemas use the established registry fallback, never publication.
	release, err := c.reserveWork(ctx, compiledPlanWorkBound(statement, nil))
	if err != nil {
		return nil, "bypass", err
	}
	defer func() { release.release(c) }()
	c.mu.Lock()
	c.stats.Bypass++
	c.stats.Compiles++
	c.publishStats()
	c.mu.Unlock()
	started := time.Now()
	template, err := c.transformer.CompileMeasure(ctx, statement)
	c.observeDuration("compile", started)
	if err != nil {
		release.release(c)
		return nil, "bypass", err
	}
	if template == nil {
		release.release(c)
		return nil, "bypass", nil
	}
	cost := int64(template.EstimatedSize())
	entry := &compiledPlanEntry{template: template, references: 1, cost: cost, reservation: release}
	release = nil
	c.mu.Lock()
	c.stats.UncachedBytes += cost
	c.publishStats()
	c.mu.Unlock()
	return &compiledPlanHandle{cache: c, template: template, entry: entry}, "bypass", nil
}

func (c *compiledPlanCache) finish(flight *compiledPlanFlight, entry *compiledPlanEntry, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	flight.cancel()
	flight.entry, flight.err, flight.complete = entry, err, true
	if c.flights[flight.key] == flight {
		delete(c.flights, flight.key)
	}
	c.flightBytes -= flight.cost
	c.runningFlights--
	if err != nil {
		c.stats.Errors++
		if errors.Is(err, context.Canceled) {
			c.stats.CanceledCompiles++
		}
	}
	if flight.waiters == 0 && entry != nil {
		c.releaseEntry(entry)
	}
	close(flight.done)
	c.publishStats()
}

func (c *compiledPlanCache) makeRoom(cost int64) bool {
	entries, bytes := c.cfg.MaxEntries, c.cfg.MaxBytes
	hooks := bydbQLPlanCacheHooks()
	if hooks.MaxEntries > 0 {
		entries = hooks.MaxEntries
	}
	if hooks.MaxBytes > 0 {
		bytes = hooks.MaxBytes
	}
	if entries <= 0 || cost > bytes {
		return false
	}
	for (c.retainedCount >= entries || cost > bytes-c.stats.RetainedBytes) && c.order.Len() > 0 {
		key := c.order.Back().Value.(string)
		c.removeEntry(c.entries[key])
	}
	return entries > 0 && c.retainedCount < entries && cost <= bytes-c.stats.RetainedBytes
}

func (c *compiledPlanCache) removeEntry(entry *compiledPlanEntry) {
	key := entry.element.Value.(string)
	delete(c.entries, key)
	c.order.Remove(entry.element)
	entry.element = nil
	entry.cached = false
	c.stats.EntryBytes -= entry.cost
	c.stats.Evictions++
	if entry.references == 0 {
		c.stats.RetainedBytes -= entry.cost
		c.retainedCount--
		entry.template = nil
		entry.dependencies = nil
	}
}

func (c *compiledPlanCache) releaseEntry(entry *compiledPlanEntry) {
	entry.references--
	if entry.references > 0 || entry.cached {
		return
	}
	if entry.reservation != nil {
		entry.reservation.releaseLocked(c)
		entry.reservation = nil
		c.stats.UncachedBytes -= entry.cost
	} else {
		c.stats.RetainedBytes -= entry.cost
		c.retainedCount--
	}
	entry.template = nil
	entry.dependencies = nil
}

func (c *compiledPlanCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.order.Len() > 0 {
		key := c.order.Back().Value.(string)
		c.removeEntry(c.entries[key])
	}
	c.publishStats()
}

func (c *compiledPlanCache) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.cancel()
	c.mu.Unlock()
	c.workers.Wait()
	c.clear()
	unregisterCompiledPlanCache(c)
}
