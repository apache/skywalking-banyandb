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

// Package signal implements a handler to listen to system signals.
package signal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/skywalking-banyandb/pkg/run"
)

// ErrSignal is returned when a termination signal is received.
var (
	ErrSignal             = errors.New("signal received")
	_         run.Service = (*Handler)(nil)
)

// Handler implements a unix signal handler as run.GroupService.
type Handler struct {
	signal chan os.Signal
	cancel chan struct{}
}

// Name shows the handler's name.
func (h *Handler) Name() string {
	return "signal"
}

// PreRun implements run.PreRunner to initialize the handler.
func (h *Handler) PreRun(_ context.Context) error {
	h.cancel = make(chan struct{})
	h.signal = make(chan os.Signal, 1)
	signal.Notify(h.signal,
		syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	return nil
}

// Serve implements run.Service and listens for incoming unix signals.
func (h *Handler) Serve() run.StopNotify {
	go func() {
		sig := <-h.signal
		fmt.Fprintf(os.Stderr, "%s %v", sig, ErrSignal)
		close(h.cancel)
	}()
	return h.cancel
}

// GracefulStop implements run.GroupService and will close the signal handler.
func (h *Handler) GracefulStop() {
	signal.Stop(h.signal)
	close(h.signal)
}
