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

// Package encoding implements encoding/decoding data points.
package encoding

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var errEncodeEmpty = errors.New("encode an empty value")

var (
	rawSize = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "banyand_encoding_raw_size",
		Help:        "The raw size of series",
		ConstLabels: prometheus.Labels{"model": "encoding"},
	}, []string{"name", "type"})
	encodedSize = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "banyand_encoding_encoded_size",
		Help:        "The encoded size of series",
		ConstLabels: prometheus.Labels{"model": "encoding"},
	}, []string{"name", "type"})
	itemsNum = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "banyand_encoding_items_num",
		Help:        "The number of items in a encoded series",
		ConstLabels: prometheus.Labels{"model": "encoding"},
	}, []string{"name", "type"})
)

// SeriesEncoderPool allows putting and getting SeriesEncoder.
type SeriesEncoderPool interface {
	Get(metadata []byte) SeriesEncoder
	Put(encoder SeriesEncoder)
}

// SeriesEncoder encodes time series data point.
type SeriesEncoder interface {
	// Append a data point
	Append(ts uint64, value []byte)
	// IsFull returns whether the encoded data reached its capacity
	IsFull() bool
	// Reset the underlying buffer
	Reset(key []byte)
	// Encode the time series data point to a binary
	Encode() ([]byte, error)
	// StartTime indicates the first entry's time
	StartTime() uint64
}

// SeriesDecoderPool allows putting and getting SeriesDecoder.
type SeriesDecoderPool interface {
	Get(metadata []byte) SeriesDecoder
	Put(encoder SeriesDecoder)
}

// SeriesDecoder decodes encoded time series data.
type SeriesDecoder interface {
	// Decode the time series data
	Decode(key, data []byte) error
	// Len denotes the size of iterator
	Len() int
	// IsFull returns whether the encoded data reached its capacity
	IsFull() bool
	// Get the data point by its time
	Get(ts uint64) ([]byte, error)
	// Iterator returns a SeriesIterator
	Iterator() SeriesIterator
	// Range returns the start and end time of this series
	Range() (start, end uint64)
}

// SeriesIterator iterates time series data.
type SeriesIterator interface {
	// Next scroll the cursor to the next
	Next() bool
	// Val returns the value of the current data point
	Val() []byte
	// Time returns the time of the current data point
	Time() uint64
	// Error might return an error indicates a decode failure
	Error() error
}
