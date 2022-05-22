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
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	ErrInvalidParameter = errors.New("parameters are invalid")
	ErrNoMoreBucket     = errors.New("no more buckets")
)

type Ratio float64

type Strategy struct {
	optionsErr error
	ratio      Ratio
	ctrl       Controller
	current    Reporter
	next       Reporter
	logger     *logger.Logger
	stopCh     chan struct{}
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
		stopCh: make(chan struct{}),
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
	return strategy, nil
}

func (s *Strategy) Run() {
	reset := func() {
		for s.current == nil {
			s.current = s.ctrl.Current()
		}
		s.next = nil
	}
	reset()
	go func(s *Strategy) {
		var err error
	bucket:
		c := s.current.Report()
		for {
			select {
			case status, closed := <-c:
				if !closed {
					reset()
					goto bucket
				}
				ratio := Ratio(status.Volume) / Ratio(status.Capacity)
				if ratio >= s.ratio && s.next == nil {
					s.next, err = s.ctrl.Next()
					if errors.Is(err, ErrNoMoreBucket) {
						return
					}
					if err != nil {
						s.logger.Err(err).Msg("failed to create the next bucket")
					}
				}
				if ratio >= 1.0 {
					s.ctrl.OnMove(s.current, s.next)
					s.current = s.next
					s.next = nil
					goto bucket
				}
			case <-s.stopCh:
				return
			}
		}
	}(s)
}

func (s *Strategy) Close() {
	s.ctrl.OnMove(s.current, nil)
	close(s.stopCh)
}
