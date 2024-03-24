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

// Package inverted implements a inverted index repository.
package inverted

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/blugelabs/bluge"
)

type sortIterator struct {
	query            bluge.Query
	reader           *bluge.Reader
	sortedKey        string
	fk               string
	shouldDecodeTerm bool
	size             int

	current *blugeMatchIterator
	lastKey []byte

	err error
}

func (si *sortIterator) Next() bool {
	if si.err != nil {
		return false
	}
	if si.current == nil {
		return si.loadCurrent()
	}

	if si.current.Next() {
		si.lastKey = si.current.current.Term
		return true
	}
	si.current.Close()
	return si.loadCurrent()
}

func (si *sortIterator) loadCurrent() bool {
	topNSearch := bluge.NewTopNSearch(si.size, si.query).SortBy([]string{si.sortedKey})
	if si.lastKey != nil {
		if strings.HasPrefix(si.sortedKey, "-") {
			topNSearch = topNSearch.Before([][]byte{si.lastKey})
		} else {
			topNSearch = topNSearch.After([][]byte{si.lastKey})
		}
	}

	documentMatchIterator, err := si.reader.Search(context.Background(), topNSearch)
	if err != nil {
		si.err = err
		return false
	}

	iter := newBlugeMatchIterator(documentMatchIterator, si.fk, si.shouldDecodeTerm, nil)
	si.current = &iter
	if si.current.Next() {
		si.lastKey = si.current.current.Term
		return true
	} else {
		si.err = io.EOF
		return false
	}
}

func (si *sortIterator) Val() *index.PostingValue {
	return si.current.Val()
}

func (si *sortIterator) Close() error {
	if errors.Is(si.err, io.EOF) {
		si.err = nil
		return errors.Join(si.current.Close(), si.reader.Close())
	}
	return errors.Join(si.err, si.current.Close(), si.reader.Close())
}
