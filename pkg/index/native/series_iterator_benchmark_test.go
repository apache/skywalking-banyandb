// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package native

import (
	"context"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func BenchmarkNativeSeriesIterator(b *testing.B) {
	path := b.TempDir()
	oracle, err := inverted.NewStore(inverted.StoreOpts{Path: path})
	if err != nil {
		b.Fatal(err)
	}
	legacyDocuments := make([]index.Document, 0, 32)
	for i := 0; i < 32; i++ {
		series := &pbv1.Series{Subject: "cpu"}
		if marshalErr := series.Marshal(); marshalErr != nil {
			b.Fatal(err)
		}
		series.Buffer = append(series.Buffer, byte(i))
		legacyDocuments = append(legacyDocuments, index.Document{EntityValues: series.Buffer})
	}
	if insertErr := oracle.InsertSeriesBatch(index.Batch{Documents: legacyDocuments}); insertErr != nil {
		b.Fatal(err)
	}
	if closeErr := oracle.Close(); closeErr != nil {
		b.Fatal(err)
	}
	nativeReader, err := OpenReadOnlyGeneration(path)
	if err != nil {
		b.Fatal(err)
	}
	defer nativeReader.Close()
	oracle, err = inverted.NewStore(inverted.StoreOpts{Path: path})
	if err != nil {
		b.Fatal(err)
	}
	defer oracle.Close()
	countNative, countOracle := 0, 0
	checkNative, checkErr := nativeReader.NewSeriesIterator(context.Background())
	if checkErr != nil {
		b.Fatal(checkErr)
	}
	for {
		value, nextErr := checkNative.Next()
		if nextErr != nil {
			b.Fatal(nextErr)
		}
		if value == nil {
			break
		}
		countNative++
	}
	checkOracle, checkErr := oracle.SeriesIterator(context.Background())
	if checkErr != nil {
		b.Fatal(checkErr)
	}
	for checkOracle.Next() {
		countOracle++
	}
	if checkErr := checkOracle.Close(); checkErr != nil {
		b.Fatal(checkErr)
	}
	if countNative != countOracle || countNative == 0 {
		b.Fatalf("fixture parity mismatch: native=%d oracle=%d", countNative, countOracle)
	}

	b.Run("native", func(b *testing.B) {
		for range b.N {
			it, err := nativeReader.NewSeriesIterator(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			for {
				v, err := it.Next()
				if err != nil {
					b.Fatal(err)
				}
				if v == nil {
					break
				}
			}
			if err := it.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("oracle", func(b *testing.B) {
		for range b.N {
			it, err := oracle.SeriesIterator(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			for it.Next() {
			}
			if err := it.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
