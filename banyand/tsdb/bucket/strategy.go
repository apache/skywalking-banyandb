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

package bucket

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	ErrInvalidParameter = errors.New("parameters are invalid")
	ErrNoMoreBucket     = errors.New("no more buckets")
)

type Ratio float64

type Strategy struct {
	optionsErr   error
	ratio        Ratio
	ctrl         Controller
	current      atomic.Value
	currentRatio uint64
	logger       *logger.Logger

	closer *run.Closer
}

type StrategyOptions func(*Strategy)

func WithNextThreshold(ratio Ratio) StrategyOptions {
	return func(s *Strategy) {
		if ratio > 1.0 {
			s.optionsErr = multierr.Append(s.optionsErr,
				errors.Wrapf(ErrInvalidParameter, "ratio %v is more than 1.0", ratio))
			return
		}
		s.ratio = ratio
	}
}

func WithLogger(logger *logger.Logger) StrategyOptions {
	return func(s *Strategy) {
		s.logger = logger
	}
}

func NewStrategy(ctrl Controller, options ...StrategyOptions) (*Strategy, error) {
	if ctrl == nil {
		return nil, errors.Wrap(ErrInvalidParameter, "controller is absent")
	}
	strategy := &Strategy{
		ctrl:   ctrl,
		ratio:  0.8,
		closer: run.NewCloser(1),
	}
	for _, opt := range options {
		opt(strategy)
	}
	if strategy.optionsErr != nil {
		return nil, strategy.optionsErr
	}
	if strategy.logger == nil {
		strategy.logger = logger.GetLogger("bucket-strategy")
	}
	if err := strategy.resetCurrent(); err != nil {
		return nil, err
	}
	return strategy, nil
}

func (s *Strategy) resetCurrent() error {
	c, err := s.ctrl.Current()
	if err != nil {
		return err
	}
	s.current.Store(c)
	return nil
}

func (s *Strategy) Run() {
	go func(s *Strategy) {
		defer s.closer.Done()
		for {
			c, err := s.current.Load().(Reporter).Report()
			if errors.Is(err, ErrReporterClosed) {
				return
			}
			if err != nil {
				s.logger.Error().Err(err).Msg("failed to get reporter")
				if err := s.resetCurrent(); err != nil {
					panic(err)
				}
				continue
			}
			if !s.observe(c) {
				return
			}
		}
	}(s)
}

func (s *Strategy) String() string {
	c := s.current.Load()
	if c == nil {
		return "nil"
	}
	return fmt.Sprintf("%s:%f", c.(Reporter).String(),
		math.Float64frombits(atomic.LoadUint64(&s.currentRatio)))
}

func (s *Strategy) observe(c Channel) bool {
	var next Reporter
	moreBucket := true
	for {
		select {
		case status, more := <-c:
			if !more {
				return moreBucket
			}
			ratio := Ratio(status.Volume) / Ratio(status.Capacity)
			atomic.StoreUint64(&s.currentRatio, math.Float64bits(float64(ratio)))
			if ratio >= s.ratio && next == nil && moreBucket {
				n, err := s.ctrl.Next()
				if errors.Is(err, ErrNoMoreBucket) {
					moreBucket = false
				} else if err != nil {
					s.logger.Err(err).Msg("failed to create the next bucket")
				} else {
					s.logger.Info().Stringer("next", n).Msg("created the next bucket")
					next = n
				}
			}
			if ratio >= 1.0 {
				s.ctrl.OnMove(s.current.Load().(Reporter), next)
				if next != nil {
					s.current.Store(next)
				}
				return moreBucket
			}
		case <-s.closer.CloseNotify():
			return false
		}
	}
}

func (s *Strategy) Close() {
	s.closer.CloseThenWait()
}
