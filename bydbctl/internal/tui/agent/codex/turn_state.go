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

package codex

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

// turnState is the per-turn half of a connection: the event channel the gateway reads, the streamed
// message accumulated so far, and the provider turn id, which arrives asynchronously after the turn
// has already started.
type turnState struct {
	ctx        context.Context
	events     chan agent.Event
	idReady    chan struct{}
	done       chan struct{}
	threadID   string
	id         string
	message    strings.Builder
	idOnce     sync.Once
	finishOnce sync.Once
	unsafeOnce sync.Once
	messageMu  sync.Mutex
}

func (turn *turnState) setID(turnID string) error {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return errors.New("codex turn id is empty")
	}
	var setErr error
	turn.messageMu.Lock()
	if turn.id != "" && turn.id != turnID {
		setErr = fmt.Errorf("codex changed active turn id from %q to %q", turn.id, turnID)
	} else {
		turn.id = turnID
	}
	turn.messageMu.Unlock()
	if setErr == nil {
		turn.idOnce.Do(func() { close(turn.idReady) })
	}
	return setErr
}

func (turn *turnState) currentID() string {
	turn.messageMu.Lock()
	defer turn.messageMu.Unlock()
	return turn.id
}

func (turn *turnState) waitID(ctx context.Context) (string, error) {
	select {
	case <-turn.idReady:
		return turn.currentID(), nil
	case <-turn.done:
		if turnID := turn.currentID(); turnID != "" {
			return turnID, nil
		}
		return "", errors.New("codex turn ended before returning an id")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (turn *turnState) appendMessage(delta string) {
	turn.messageMu.Lock()
	turn.message.WriteString(delta)
	turn.messageMu.Unlock()
}

func (turn *turnState) messageText() string {
	turn.messageMu.Lock()
	defer turn.messageMu.Unlock()
	return strings.TrimSpace(turn.message.String())
}

func (turn *turnState) emit(event agent.Event) {
	select {
	case turn.events <- event:
	case <-turn.ctx.Done():
	case <-turn.done:
	}
}

func (turn *turnState) finish(event agent.Event) {
	turn.finishOnce.Do(func() {
		turn.emit(event)
		close(turn.done)
		close(turn.events)
	})
}
