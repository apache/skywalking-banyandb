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

package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	ErrInterrupted    = errors.New("the query is interrupted by context")
	ErrNoServiceExist = errors.New("the service does not exist")
	globalRegister    SvcRegister

	_ run.Unit = (*SvcRegister)(nil)
)

type Service interface {
	Name() string
	Query(ctx context.Context, param interface{}) (interface{}, error)
}

type SvcRegister struct {
	mutex sync.RWMutex
	lst   map[string]Service
}

func (r *SvcRegister) Name() string {
	return "service-register"
}

func (r *SvcRegister) Register(svc ...Service) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, s := range svc {
		r.lst[s.Name()] = s
	}
}

type result struct {
	err    error
	result interface{}
}

func InitRegister() *SvcRegister {
	once := sync.Once{}
	once.Do(func() {
		globalRegister = SvcRegister{}
	})
	return &globalRegister
}

func Async(ctx context.Context, svc string, param interface{}) (interface{}, error) {
	if s, ok := globalRegister.lst[svc]; ok {
		ch := make(chan result)
		go func(ctx context.Context, param interface{}, ch chan result) {
			r, err := s.Query(ctx, param)
			ch <- result{err: err, result: r}
		}(ctx, param, ch)

		select {
		case res := <-ch:
			return res.result, res.err
		case <-ctx.Done():
			return nil, ErrInterrupted
		}
	}
	return nil, ErrNoServiceExist
}
