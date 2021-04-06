/*
 *  Licensed to Apache Software Foundation (ASF) under one or more contributor
 *  license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright
 *  ownership. Apache Software Foundation (ASF) licenses this file to you under
 *  the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http:www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package bus

import (
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestBus_PubAndSub(t *testing.T) {
	type message struct {
		topic      Topic
		messageIDS []MessageID
		wantErr    bool
	}
	type listener struct {
		wantTopic    Topic
		wantMessages []MessageID
		wantErr      bool
	}
	tests := []struct {
		name      string
		messages  []message
		listeners []listener
	}{
		{
			name: "golden path",
			messages: []message{
				{
					topic:      Topic("default"),
					messageIDS: []MessageID{12, 33},
				},
			},
			listeners: []listener{
				{
					wantTopic:    Topic("default"),
					wantMessages: []MessageID{12, 33},
				},
			},
		},
		{
			name: "two topics",
			messages: []message{
				{
					topic:      Topic("t1"),
					messageIDS: []MessageID{12, 33},
				},
				{
					topic:      Topic("t2"),
					messageIDS: []MessageID{101, 102},
				},
			},
			listeners: []listener{
				{
					wantTopic:    Topic("t1"),
					wantMessages: []MessageID{12, 33},
				},
				{
					wantTopic:    Topic("t2"),
					wantMessages: []MessageID{101, 102},
				},
			},
		},
		{
			name: "two topics with two listeners",
			messages: []message{
				{
					topic:      Topic("t1"),
					messageIDS: []MessageID{12, 33},
				},
				{
					topic:      Topic("t2"),
					messageIDS: []MessageID{101, 102},
				},
			},
			listeners: []listener{
				{
					wantTopic:    Topic("t1"),
					wantMessages: []MessageID{12, 33},
				},
				{
					wantTopic:    Topic("t1"),
					wantMessages: []MessageID{12, 33},
				},
				{
					wantTopic:    Topic("t2"),
					wantMessages: []MessageID{101, 102},
				},
				{
					wantTopic:    Topic("t2"),
					wantMessages: []MessageID{101, 102},
				},
			},
		},
		{
			name: "publish invalid topic",
			messages: []message{
				{
					topic:      Topic(""),
					messageIDS: []MessageID{12, 33},
					wantErr:    true,
				},
			},
		},
		{
			name: "publish empty message",
			messages: []message{
				{
					topic:      Topic("default"),
					messageIDS: []MessageID{},
				},
			},
			listeners: []listener{
				{
					wantTopic:    Topic("default"),
					wantMessages: []MessageID{},
				},
			},
		},
		{
			name: "subscribe invalid topic",
			listeners: []listener{
				{
					wantTopic: Topic(""),
					wantErr:   true,
				},
			},
		},
	}
	for _, tt := range tests {
		e := NewBus()
		t.Run(tt.name, func(t *testing.T) {
			wg := sync.WaitGroup{}
			mll := make([]*mockListener, 0)
			for _, l := range tt.listeners {
				ml := &mockListener{wg: &wg}
				mll = append(mll, ml)
				wg.Add(len(l.wantMessages))
				if err := e.Subscribe(l.wantTopic, ml); (err != nil) != l.wantErr {
					t.Errorf("Subscribe() error = %v, wantErr %v", err, l.wantErr)
				}
			}
			for _, m := range tt.messages {
				mm := make([]Message, 0)
				for _, id := range m.messageIDS {
					mm = append(mm, NewMessage(id, nil))
				}
				err := e.Publish(m.topic, mm...)
				if (err != nil) != m.wantErr {
					t.Errorf("Publish() error = %v, wantErr %v", err, m.wantErr)
				}
			}
			if waitTimeout(&wg, 10*time.Second) {
				t.Error("message receiving is time out")
			}
			wg.Wait()
			for i, l := range tt.listeners {
				if len(mll[i].queue) > 0 && len(l.wantMessages) > 0 &&
					!reflect.DeepEqual(mll[i].queue, l.wantMessages) {
					t.Errorf("Bus got = %v, wanted %v", mll[i].queue, l.wantMessages)
				}
			}
		})
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

var _ MessageListener = new(mockListener)

type mockListener struct {
	queue   []MessageID
	wg      *sync.WaitGroup
	closeWg *sync.WaitGroup
}

func (m *mockListener) Rev(message Message) {
	m.queue = append(m.queue, message.id)
	sort.SliceStable(m.queue, func(i, j int) bool {
		return uint64(m.queue[i]) < uint64(m.queue[j])
	})
	m.wg.Done()
}

func (m *mockListener) Close() error {
	m.queue = nil
	m.closeWg.Done()
	return nil
}
