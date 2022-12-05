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
		wantRet    []MessageID
		wantErr    bool
	}
	type listener struct {
		wantTopic    Topic
		wantMessages []MessageID
		ret          []MessageID
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
					topic:      BiTopic("default"),
					messageIDS: []MessageID{12, 33},
					wantRet:    []MessageID{22, 43},
				},
			},
			listeners: []listener{
				{
					wantTopic:    BiTopic("default"),
					wantMessages: []MessageID{12, 33},
					ret:          []MessageID{22, 43},
				},
			},
		},
		{
			name: "two topics",
			messages: []message{
				{
					topic:      UniTopic("t1"),
					messageIDS: []MessageID{12, 33},
				},
				{
					topic:      UniTopic("t2"),
					messageIDS: []MessageID{101, 102},
				},
			},
			listeners: []listener{
				{
					wantTopic:    UniTopic("t1"),
					wantMessages: []MessageID{12, 33},
				},
				{
					wantTopic:    UniTopic("t2"),
					wantMessages: []MessageID{101, 102},
				},
			},
		},
		{
			name: "two topics with two listeners",
			messages: []message{
				{
					topic:      UniTopic("t1"),
					messageIDS: []MessageID{12, 33},
				},
				{
					topic:      UniTopic("t2"),
					messageIDS: []MessageID{101, 102},
				},
			},
			listeners: []listener{
				{
					wantTopic:    UniTopic("t1"),
					wantMessages: []MessageID{12, 33},
				},
				{
					wantTopic:    UniTopic("t1"),
					wantMessages: []MessageID{12, 33},
				},
				{
					wantTopic:    UniTopic("t2"),
					wantMessages: []MessageID{101, 102},
				},
				{
					wantTopic:    UniTopic("t2"),
					wantMessages: []MessageID{101, 102},
				},
			},
		},
		{
			name: "publish invalid topic",
			messages: []message{
				{
					topic:      UniTopic(""),
					messageIDS: []MessageID{12, 33},
					wantErr:    true,
				},
			},
		},
		{
			name: "publish empty message",
			messages: []message{
				{
					topic:      UniTopic("default"),
					messageIDS: []MessageID{},
				},
			},
			listeners: []listener{
				{
					wantTopic:    UniTopic("default"),
					wantMessages: []MessageID{},
				},
			},
		},
		{
			name: "subscribe invalid topic",
			listeners: []listener{
				{
					wantTopic: UniTopic(""),
					wantErr:   true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wg := sync.WaitGroup{}
			mll := make([]*mockListener, 0)
			e := NewBus()
			for _, l := range tt.listeners {
				var ret chan Message
				if len(l.ret) > 0 {
					ret = make(chan Message, len(l.ret))
					for _, id := range l.ret {
						ret <- NewMessage(id, nil)
					}
				}
				ml := &mockListener{wg: &wg, ret: ret}
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
				f, err := e.Publish(m.topic, mm...)
				if err != nil && !m.wantErr {
					t.Errorf("Publish() error = %v, wantErr %v", err, m.wantErr)
					continue
				}
				if f == nil {
					continue
				}
				go func(want []MessageID) {
					ret, errRet := f.GetAll()
					if errors.Is(errRet, errEmptyFuture) {
						return
					} else if errRet != nil {
						t.Errorf("Publish()'s return message error = %v", err)
					}
					ids := make([]MessageID, 0, len(ret))
					for i := range ret {
						ids = append(ids, ret[i].ID())
					}
					ids = sortMessage(ids)
					if !reflect.DeepEqual(ids, want) {
						t.Errorf("Publish()'s return = %v, want %v", ret, want)
					}
					for i := 0; i < len(ret); i++ {
						wg.Done()
					}
				}(m.wantRet)
			}
			if waitTimeout(&wg, 10*time.Second) {
				t.Error("message receiving is time out")
			}
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
	wg      *sync.WaitGroup
	closeWg *sync.WaitGroup
	ret     chan Message
	queue   []MessageID
}

func (m *mockListener) Rev(message Message) Message {
	m.queue = append(m.queue, message.id)
	sort.SliceStable(m.queue, func(i, j int) bool {
		return uint64(m.queue[i]) < uint64(m.queue[j])
	})
	if m.ret != nil {
		r := <-m.ret
		return r
	}
	m.wg.Done()
	return Message{}
}

func (m *mockListener) Close() error {
	m.queue = nil
	m.closeWg.Done()
	return nil
}

func sortMessage(ids []MessageID) []MessageID {
	sort.SliceStable(ids, func(i, j int) bool {
		return uint64(ids[i]) < uint64(ids[j])
	})
	return ids
}
