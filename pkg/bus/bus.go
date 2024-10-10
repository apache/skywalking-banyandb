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

// Package bus implements a message bus which is a common data model and a messaging infrastructure
// to allow different modules to communicate locally or remotely.
package bus

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

type (
	payload interface{}

	// MessageID the identity of a Message.
	MessageID uint64

	// Future represents a future result of an asynchronous publishing.
	Future interface {
		Get() (Message, error)
		GetAll() ([]Message, error)
	}
)

// Message is send on the bus to all subscribed listeners.
type Message struct {
	payload   payload
	node      string
	id        MessageID
	batchMode bool
}

// ID outputs the MessageID of the Message.
func (m Message) ID() MessageID {
	return m.id
}

// Data returns the data wrapped in the Message.
func (m Message) Data() interface{} {
	return m.payload
}

// Node returns the node name of the Message.
func (m Message) Node() string {
	return m.node
}

// BatchModeEnabled returns whether the Message is sent in batch mode.
func (m Message) BatchModeEnabled() bool {
	return m.batchMode
}

// NewMessage returns a new Message with a MessageID and embed data.
func NewMessage(id MessageID, data interface{}) Message {
	return Message{id: id, node: "local", payload: data}
}

// NewBatchMessageWithNode returns a new Message with a MessageID and NodeID and embed data.
func NewBatchMessageWithNode(id MessageID, node string, data interface{}) Message {
	return Message{id: id, node: node, payload: data, batchMode: true}
}

// NewMessageWithNode returns a new Message with a MessageID and NodeID and embed data.
func NewMessageWithNode(id MessageID, node string, data interface{}) Message {
	return Message{id: id, node: node, payload: data}
}

// MessageListener is the signature of functions that can handle an EventMessage.
type MessageListener interface {
	Rev(ctx context.Context, message Message) Message
}

// Subscriber allow subscribing a Topic's messages.
type Subscriber interface {
	Subscribe(topic Topic, listener MessageListener) error
}

// Publisher allow sending Messages to a Topic.
type Publisher interface {
	Publish(ctx context.Context, topic Topic, message ...Message) (Future, error)
}

// Broadcaster allow sending Messages to a Topic and receiving the responses.
type Broadcaster interface {
	Broadcast(timeout time.Duration, topic Topic, message Message) ([]Future, error)
}

type chType int

var (
	chTypeUnidirectional chType
	chTypeBidirectional  chType = 1
)

// Topic is the object which messages are sent to or received from.
type Topic struct {
	id  string
	typ chType
}

// UniTopic returns an unary Topic.
func UniTopic(id string) Topic {
	return Topic{id: id, typ: chTypeUnidirectional}
}

// BiTopic returns bidirectional Topic.
func BiTopic(id string) Topic {
	return Topic{id: id, typ: chTypeBidirectional}
}

// String returns the string representation of the Topic.
func (t Topic) String() string {
	return t.id
}

// The Bus allows publish-subscribe-style communication between components.
type Bus struct {
	topics map[Topic][]MessageListener
	mutex  sync.RWMutex
}

// NewBus returns a Bus.
func NewBus() *Bus {
	b := new(Bus)
	b.topics = make(map[Topic][]MessageListener)
	return b
}

var (
	// ErrTopicNotExist hints the topic published doesn't exist.
	ErrTopicNotExist = errors.New("the topic does not exist")

	errTopicEmpty    = errors.New("the topic is empty")
	errListenerEmpty = errors.New("the message listener is empty")
	errEmptyFuture   = errors.New("can't invoke Get() on an empty future")
)

type emptyFuture struct{}

func (e *emptyFuture) Get() (Message, error) {
	return Message{}, errEmptyFuture
}

func (e *emptyFuture) GetAll() ([]Message, error) {
	return nil, errEmptyFuture
}

type localFuture struct {
	messages []Message
}

func (l *localFuture) Get() (Message, error) {
	if len(l.messages) == 0 {
		return Message{}, io.EOF
	}
	m := l.messages[0]
	l.messages = l.messages[1:]
	return m, nil
}

func (l *localFuture) GetAll() ([]Message, error) {
	return l.messages, nil
}

// Publish sends Messages to a Topic.
func (b *Bus) Publish(ctx context.Context, topic Topic, message ...Message) (Future, error) {
	if topic.id == "" {
		return nil, errTopicEmpty
	}
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	mll, exit := b.topics[topic]
	if !exit {
		return nil, ErrTopicNotExist
	}
	var f *localFuture
	switch topic.typ {
	case chTypeUnidirectional:
		f = nil
	case chTypeBidirectional:
		f = &localFuture{messages: make([]Message, 0, len(message))}
	}
	for _, ml := range mll {
		for _, m := range message {
			if f != nil {
				f.messages = append(f.messages, ml.Rev(ctx, m))
			} else {
				ml.Rev(ctx, m)
			}
		}
	}
	if f == nil {
		return &emptyFuture{}, nil
	}
	return f, nil
}

// Subscribe adds an MessageListener to be called when a message of a Topic is posted.
func (b *Bus) Subscribe(topic Topic, listener MessageListener) error {
	if topic.id == "" {
		return errTopicEmpty
	}
	if listener == nil {
		return errListenerEmpty
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, exist := b.topics[topic]; !exist {
		b.topics[topic] = make([]MessageListener, 0)
	}
	list := b.topics[topic]
	list = append(list, listener)
	b.topics[topic] = list
	return nil
}

// Close a Bus until all Messages are sent to Subscribers.
func (b *Bus) Close() {
}
