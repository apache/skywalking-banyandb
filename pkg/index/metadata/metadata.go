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

package metadata

import (
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type Term interface {
	ID(term []byte) (id []byte, err error)
	Literal(id []byte) (term []byte, err error)
}

var _ Term = (*term)(nil)

type term struct {
	store kv.Store
}

type TermOpts struct {
	Path   string
	Logger *logger.Logger
}

func NewTerm(opts TermOpts) (Term, error) {
	var store kv.Store
	var err error
	if store, err = kv.OpenStore(0, opts.Path, kv.StoreWithNamedLogger("term_metadata", opts.Logger)); err != nil {
		return nil, err
	}
	return &term{
		store: store,
	}, nil
}

func (t *term) ID(term []byte) (id []byte, err error) {
	id = convert.Uint64ToBytes(convert.Hash(term))
	_, err = t.store.Get(id)
	if errors.Is(err, kv.ErrKeyNotFound) {
		return id, t.store.Put(id, term)
	}
	return id, nil
}

func (t *term) Literal(id []byte) (term []byte, err error) {
	return t.store.Get(id)
}
