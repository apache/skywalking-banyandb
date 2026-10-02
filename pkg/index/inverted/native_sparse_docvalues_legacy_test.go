// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inverted

import (
	"bytes"
	"testing"

	segment "github.com/blugelabs/bluge_segment_api"
	legacyice "github.com/blugelabs/ice"
	"github.com/stretchr/testify/require"
)

func TestNativePluginLoadsLegacySparseDocValueChunk(t *testing.T) {
	documents := make([]segment.Document, 1025)
	for documentNumber := range documents {
		fields := []segment.Field{
			reviewIDField("id"),
			&reviewField{name: "common", value: []byte("common"), store: true, docValues: true},
		}
		if documentNumber == 1024 {
			fields = append(fields, &reviewField{
				name: "rare", value: []byte("v"), index: true, store: true, docValues: true,
				terms: []reviewTerm{{value: []byte("v"), frequency: 1}},
			})
		}
		documents[documentNumber] = &reviewDocument{fields: fields}
	}

	legacy, _, newErr := legacyice.New(documents, func(string, int) float32 { return 1 })
	require.NoError(t, newErr)
	var payload bytes.Buffer
	_, writeErr := legacy.WriteTo(&payload, nil)
	require.NoError(t, writeErr)

	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload.Bytes()))
	require.NoError(t, loadErr)
	docValues, readerErr := loaded.DocumentValueReader([]string{"rare"})
	require.NoError(t, readerErr)
	for _, documentNumber := range []uint64{0, 1024} {
		var values []string
		visitErr := docValues.VisitDocumentValues(documentNumber, func(_ string, value []byte) {
			values = append(values, string(value))
		})
		require.NoError(t, visitErr)
		if documentNumber == 0 {
			require.Empty(t, values)
		} else {
			require.Equal(t, []string{"v"}, values)
		}
	}
}
