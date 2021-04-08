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

	"go.uber.org/atomic"
)

// Payload represents a simple data
type Payload interface{}
type MessageID uint64

// Message is send on the bus to all subscribed listeners
type Message struct {
	id      MessageID
	payload Payload
}

func (m Message) Data() interface{} {
	return m.payload
}

func NewMessage(id MessageID, data interface{}) Message {
	return Message{id: id, payload: data}
}

// EventListener is the signature of functions that can handle an EventMessage.
type MessageListener interface {
	Rev(message Message)
	io.Closer
}

type Channel chan Message

type Topic string

// The Bus allows publish-subscribe-style communication between components
type Bus struct {
	topics map[Topic][]Channel
	closed atomic.Bool
	mutex  sync.RWMutex
}

func NewBus() *Bus {
	b := new(Bus)
	b.topics = make(map[Topic][]Channel)
	return b
}

var (
	ErrTopicEmpty    = errors.New("the topic is empty")
	ErrTopicNotExist = errors.New("the topic does not exist")
	ErrListenerEmpty = errors.New("the message listener is empty")
	ErrClosed        = errors.New("the bus is closed")
)

func (b *Bus) Publish(topic Topic, message ...Message) error {
	if topic == "" {
		return ErrTopicEmpty
	}
	if b.closed.Load() {
		return ErrClosed
	}
	cc, exit := b.topics[topic]
	if !exit {
		return ErrTopicNotExist
	}
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	for _, each := range cc {
		for _, m := range message {
			go func(ch Channel, message Message) {
				ch <- message
			}(each, m)
		}
	}
	return nil
}

// Subscribe adds an MessageListener to be called when a message of a Topic is posted.
func (b *Bus) Subscribe(topic Topic, listener MessageListener) error {
	if topic == "" {
		return ErrTopicEmpty
	}
	if listener == nil {
		return ErrListenerEmpty
	}
	if b.closed.Load() {
		return ErrClosed
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
				listener.Rev(c)
			} else {
				_ = listener.Close()
				break
			}
		}
	}(listener, ch)
	return nil
}
