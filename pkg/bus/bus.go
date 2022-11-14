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

package bus

import (
	"errors"
	"io"
	"sync"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Payload represents a simple data
type Payload interface{}
type (
	MessageID uint64
	Future    interface {
		Get() (Message, error)
		GetAll() ([]Message, error)
	}
)

// Message is send on the bus to all subscribed listeners
type Message struct {
	id      MessageID
	payload Payload
}

func (m Message) ID() MessageID {
	return m.id
}

func (m Message) Data() interface{} {
	return m.payload
}

func NewMessage(id MessageID, data interface{}) Message {
	return Message{id: id, payload: data}
}

// MessageListener is the signature of functions that can handle an EventMessage.
type MessageListener interface {
	Rev(message Message) Message
}

type Subscriber interface {
	Subscribe(topic Topic, listener MessageListener) error
}

type Publisher interface {
	Publish(topic Topic, message ...Message) (Future, error)
}

type Channel chan Event

type ChType int

var (
	ChTypeUnidirectional ChType = 0
	ChTypeBidirectional  ChType = 1
)

type Topic struct {
	ID   string
	Type ChType
}

func UniTopic(ID string) Topic {
	return Topic{ID: ID, Type: ChTypeUnidirectional}
}

func BiTopic(ID string) Topic {
	return Topic{ID: ID, Type: ChTypeBidirectional}
}

// The Bus allows publish-subscribe-style communication between components
type Bus struct {
	topics map[Topic][]Channel
	mutex  sync.RWMutex
	closer *run.Closer
}

func NewBus() *Bus {
	b := new(Bus)
	b.topics = make(map[Topic][]Channel)
	b.closer = run.NewCloser(0)
	return b
}

var (
	ErrTopicEmpty    = errors.New("the topic is empty")
	ErrTopicNotExist = errors.New("the topic does not exist")
	ErrListenerEmpty = errors.New("the message listener is empty")
	ErrEmptyFuture   = errors.New("can't invoke Get() on an empty future")
)

type emptyFuture struct{}

func (e *emptyFuture) Get() (Message, error) {
	return Message{}, ErrEmptyFuture
}

func (e *emptyFuture) GetAll() ([]Message, error) {
	return nil, ErrEmptyFuture
}

type localFuture struct {
	retCh    chan Message
	retCount int
}

func (l *localFuture) Get() (Message, error) {
	if l.retCount < 1 {
		return Message{}, io.EOF
	}
	m, ok := <-l.retCh
	if ok {
		l.retCount = l.retCount - 1
		return m, nil
	}
	return Message{}, io.EOF
}

func (l *localFuture) GetAll() ([]Message, error) {
	var globalErr error
	ret := make([]Message, 0, l.retCount)
	for {
		m, err := l.Get()
		if err == io.EOF {
			return ret, globalErr
		}
		if err != nil {
			globalErr = multierr.Append(globalErr, err)
			continue
		}
		ret = append(ret, m)
	}
}

type Event struct {
	m Message
	f Future
}

func (b *Bus) Publish(topic Topic, message ...Message) (Future, error) {
	if topic.ID == "" {
		return nil, ErrTopicEmpty
	}
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	cc, exit := b.topics[topic]
	if !exit {
		return nil, ErrTopicNotExist
	}
	var f Future
	switch topic.Type {
	case ChTypeUnidirectional:
		f = nil
	case ChTypeBidirectional:
		f = &localFuture{retCount: len(message), retCh: make(chan Message)}
	}
	for _, each := range cc {
		for _, m := range message {
			go func(ch Channel, message Message) {
				if !b.closer.AddRunning() {
					return
				}
				defer b.closer.Done()
				select {
				case <-b.closer.CloseNotify():
					return
				case ch <- Event{
					m: message,
					f: f,
				}:
				}
			}(each, m)
		}
	}
	if f == nil {
		return &emptyFuture{}, nil
	}
	return f, nil
}

// Subscribe adds an MessageListener to be called when a message of a Topic is posted.
func (b *Bus) Subscribe(topic Topic, listener MessageListener) error {
	if topic.ID == "" {
		return ErrTopicEmpty
	}
	if listener == nil {
		return ErrListenerEmpty
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, exist := b.topics[topic]; !exist {
		b.topics[topic] = make([]Channel, 0)
	}
	ch := make(Channel)
	list := b.topics[topic]
	list = append(list, ch)
	b.topics[topic] = list
	go func(listener MessageListener, ch Channel) {
		for {
			c, ok := <-ch
			if ok {
				ret := listener.Rev(c.m)
				if c.f == nil {
					continue
				}
				if lf, ok := c.f.(*localFuture); ok {
					lf.retCh <- ret
				}
			} else {
				break
			}
		}
	}(listener, ch)
	return nil
}

func (b *Bus) Close() {
	b.closer.CloseThenWait()
	for _, chs := range b.topics {
		for _, ch := range chs {
			close(ch)
		}
	}
}
