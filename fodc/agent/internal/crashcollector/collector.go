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

// Package crashcollector polls diagnosis collections from BanyanDB and stores them in the FODC agent.
package crashcollector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

const (
	defaultPollInterval        = 5 * time.Second
	defaultDiagnosisBufferSize = 128
	estimatedRecordBytes       = 16 * 1024
)

// Config configures the crash collector.
type Config struct {
	SourceEndpoints   []string
	PollInterval      time.Duration
	BufferSize        int
	CapacitySizeBytes int64
}

// CollectionRecord stores diagnosis data fetched from a BanyanDB endpoint.
type CollectionRecord struct {
	FetchedAt      time.Time            `json:"fetchedAt"`
	SourceEndpoint string               `json:"sourceEndpoint"`
	Collection     panicdiag.Collection `json:"collection"`
}

// Collector polls BanyanDB diagnosis endpoints and stores the latest fetched collections.
type Collector struct {
	log          *logger.Logger
	client       *http.Client
	endpoints    []string
	pollInterval time.Duration

	mu       sync.RWMutex
	records  *flightrecorder.RingBuffer[CollectionRecord]
	seenKeys map[string]struct{}
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// New returns a new Collector.
func New(log *logger.Logger, cfg Config) *Collector {
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	bufferSize := cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = defaultDiagnosisBufferSize
	}
	if cfg.CapacitySizeBytes > 0 {
		computedCapacity := computeCapacity(cfg.CapacitySizeBytes)
		if computedCapacity < bufferSize {
			bufferSize = computedCapacity
		}
	}
	recordBuffer := flightrecorder.NewRingBuffer[CollectionRecord]()
	recordBuffer.SetCapacity(bufferSize)
	return &Collector{
		log:          log,
		client:       &http.Client{Timeout: 5 * time.Second},
		endpoints:    append([]string(nil), cfg.SourceEndpoints...),
		pollInterval: interval,
		records:      recordBuffer,
		seenKeys:     make(map[string]struct{}),
	}
}

// Start starts the collector loop and returns a stop function.
func (c *Collector) Start(ctx context.Context) func() {
	if c == nil || len(c.endpoints) == 0 {
		return nil
	}
	collectorCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.loop(collectorCtx)
	}()
	return func() {
		cancel()
		c.wg.Wait()
	}
}

func (c *Collector) loop(ctx context.Context) {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	if err := c.PollOnce(ctx); err != nil {
		c.log.Warn().Err(err).Msg("Initial diagnosis collection poll failed")
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.PollOnce(ctx); err != nil {
				c.log.Warn().Err(err).Msg("Diagnosis collection poll failed")
			}
		}
	}
}

// PollOnce fetches diagnosis collections from all configured BanyanDB endpoints.
func (c *Collector) PollOnce(ctx context.Context) error {
	if c == nil || len(c.endpoints) == 0 {
		return nil
	}
	for _, endpoint := range c.endpoints {
		collections, err := c.fetchCollections(ctx, endpoint)
		if err != nil {
			return err
		}
		c.storeCollections(endpoint, collections)
	}
	return nil
}

// ListCollections returns the latest fetched diagnosis collections.
func (c *Collector) ListCollections() []CollectionRecord {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := c.records.GetAllValues()
	return result
}

// MarshalCollections marshals the latest fetched diagnosis collections as JSON.
func (c *Collector) MarshalCollections() ([]byte, error) {
	data, err := json.Marshal(c.ListCollections())
	if err != nil {
		return nil, fmt.Errorf("marshal collections: %w", err)
	}
	return data, nil
}

func (c *Collector) fetchCollections(ctx context.Context, metricsEndpoint string) ([]panicdiag.Collection, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, panicdiagDiagnosisEndpoint(metricsEndpoint), nil)
	if err != nil {
		return nil, fmt.Errorf("create diagnosis request: %w", err)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch diagnosis collections: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("unexpected diagnosis response status %d: %s", response.StatusCode, string(body))
	}
	var collections []panicdiag.Collection
	if err := json.NewDecoder(response.Body).Decode(&collections); err != nil {
		return nil, fmt.Errorf("decode diagnosis collections: %w", err)
	}
	return collections, nil
}

func (c *Collector) storeCollections(sourceEndpoint string, collections []panicdiag.Collection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, collection := range collections {
		key := sourceEndpoint + "::" + collection.ArtifactDir
		if _, exists := c.seenKeys[key]; exists {
			continue
		}
		c.records.Add(CollectionRecord{
			FetchedAt:      time.Now().UTC(),
			SourceEndpoint: sourceEndpoint,
			Collection:     collection,
		})
		c.records.FinalizeLastVisible()
		c.seenKeys[key] = struct{}{}
	}
}

func panicdiagDiagnosisEndpoint(metricsEndpoint string) string {
	return strings.TrimSuffix(metricsEndpoint, "/metrics") + "/diagnostics/collections"
}

func computeCapacity(capacitySizeBytes int64) int {
	if capacitySizeBytes <= 0 {
		return 1
	}
	capacity := int(capacitySizeBytes / estimatedRecordBytes)
	if capacity < 1 {
		return 1
	}
	return capacity
}
