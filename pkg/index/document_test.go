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

package index

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
)

func TestDocument_Marshal_Unmarshal(t *testing.T) {
	tests := []struct {
		name     string
		document Document
	}{
		{
			name: "Document with multiple fields",
			document: Document{
				EntityValues: []byte("entity-data"),
				Timestamp:    1640995200000, // 2022-01-01 00:00:00 UTC
				DocID:        12345,
				Version:      1,
				Fields: []Field{
					{
						Key: FieldKey{
							TimeRange: &RangeOpts{
								Upper:         &BytesTermValue{Value: []byte("2024-12-31")},
								Lower:         &BytesTermValue{Value: []byte("2024-01-01")},
								IncludesUpper: true,
								IncludesLower: false,
							},
							Analyzer:    "keyword",
							TagName:     "service.name",
							SeriesID:    common.SeriesID(123456789),
							IndexRuleID: 42,
						},
						term:   &BytesTermValue{Value: []byte("test-service")},
						NoSort: false,
						Store:  true,
						Index:  true,
					},
					{
						Key: FieldKey{
							TimeRange: &RangeOpts{
								Upper:         &FloatTermValue{Value: 100.0},
								Lower:         &FloatTermValue{Value: 0.0},
								IncludesUpper: true,
								IncludesLower: true,
							},
							Analyzer:    "standard",
							TagName:     "response.time",
							SeriesID:    common.SeriesID(987654321),
							IndexRuleID: 99,
						},
						term:   &FloatTermValue{Value: 45.67},
						NoSort: true,
						Store:  false,
						Index:  true,
					},
				},
			},
		},
		{
			name: "Document with empty fields",
			document: Document{
				EntityValues: []byte{},
				Timestamp:    0,
				DocID:        0,
				Version:      0,
				Fields:       []Field{},
			},
		},
		{
			name: "Document with single field",
			document: Document{
				EntityValues: []byte("single-field-entity"),
				Timestamp:    1640995200000,
				DocID:        999,
				Version:      5,
				Fields: []Field{
					{
						Key: FieldKey{
							TimeRange:   nil,
							Analyzer:    "simple",
							TagName:     "status",
							SeriesID:    common.SeriesID(555),
							IndexRuleID: 1,
						},
						term:   &BytesTermValue{Value: []byte("success")},
						NoSort: false,
						Store:  true,
						Index:  false,
					},
				},
			},
		},
		{
			name: "Document with mixed field types",
			document: Document{
				EntityValues: []byte("mixed-types-entity"),
				Timestamp:    1640995200000,
				DocID:        777,
				Version:      3,
				Fields: []Field{
					{
						Key: FieldKey{
							TimeRange:   nil,
							Analyzer:    "keyword",
							TagName:     "string_field",
							SeriesID:    common.SeriesID(111),
							IndexRuleID: 10,
						},
						term:   &BytesTermValue{Value: []byte("string value")},
						NoSort: false,
						Store:  true,
						Index:  true,
					},
					{
						Key: FieldKey{
							TimeRange:   nil,
							Analyzer:    "standard",
							TagName:     "float_field",
							SeriesID:    common.SeriesID(222),
							IndexRuleID: 20,
						},
						term:   &FloatTermValue{Value: 3.14159},
						NoSort: true,
						Store:  false,
						Index:  true,
					},
				},
			},
		},
		{
			name: "Document with large values",
			document: Document{
				EntityValues: bytes.Repeat([]byte("large-entity-data-"), 100),
				Timestamp:    1640995200000,
				DocID:        123456789,
				Version:      1000,
				Fields: []Field{
					{
						Key: FieldKey{
							TimeRange:   nil,
							Analyzer:    "keyword",
							TagName:     "large_field",
							SeriesID:    common.SeriesID(999999999),
							IndexRuleID: 999,
						},
						term:   &BytesTermValue{Value: bytes.Repeat([]byte("large-field-value-"), 50)},
						NoSort: false,
						Store:  true,
						Index:  true,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			marshaled, err := tt.document.Marshal()
			require.NoError(t, err)
			assert.NotNil(t, marshaled)
			assert.Greater(t, len(marshaled), 0)

			// Unmarshal
			var unmarshaled Document
			err = unmarshaled.Unmarshal(marshaled)
			require.NoError(t, err)

			// Verify basic fields
			assert.Equal(t, tt.document.EntityValues, unmarshaled.EntityValues)
			assert.Equal(t, tt.document.Timestamp, unmarshaled.Timestamp)
			assert.Equal(t, tt.document.DocID, unmarshaled.DocID)
			assert.Equal(t, tt.document.Version, unmarshaled.Version)

			// Verify fields count
			assert.Equal(t, len(tt.document.Fields), len(unmarshaled.Fields))

			// Verify each field
			for i, expectedField := range tt.document.Fields {
				require.Less(t, i, len(unmarshaled.Fields))
				actualField := unmarshaled.Fields[i]

				// Check FieldKey
				assert.Equal(t, expectedField.Key.Analyzer, actualField.Key.Analyzer)
				assert.Equal(t, expectedField.Key.TagName, actualField.Key.TagName)
				assert.Equal(t, expectedField.Key.SeriesID, actualField.Key.SeriesID)
				assert.Equal(t, expectedField.Key.IndexRuleID, actualField.Key.IndexRuleID)

				// Check TimeRange
				if expectedField.Key.TimeRange != nil {
					assert.NotNil(t, actualField.Key.TimeRange)
					assert.Equal(t, expectedField.Key.TimeRange.IncludesUpper, actualField.Key.TimeRange.IncludesUpper)
					assert.Equal(t, expectedField.Key.TimeRange.IncludesLower, actualField.Key.TimeRange.IncludesLower)

					// Compare Upper bound
					switch expected := expectedField.Key.TimeRange.Upper.(type) {
					case *BytesTermValue:
						actual, ok := actualField.Key.TimeRange.Upper.(*BytesTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					case *FloatTermValue:
						actual, ok := actualField.Key.TimeRange.Upper.(*FloatTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					}

					// Compare Lower bound
					switch expected := expectedField.Key.TimeRange.Lower.(type) {
					case *BytesTermValue:
						actual, ok := actualField.Key.TimeRange.Lower.(*BytesTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					case *FloatTermValue:
						actual, ok := actualField.Key.TimeRange.Lower.(*FloatTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					}
				} else {
					assert.Nil(t, actualField.Key.TimeRange)
				}

				// Check term value
				switch expected := expectedField.term.(type) {
				case *BytesTermValue:
					actual, ok := actualField.term.(*BytesTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				case *FloatTermValue:
					actual, ok := actualField.term.(*FloatTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				}

				// Check flags
				assert.Equal(t, expectedField.NoSort, actualField.NoSort)
				assert.Equal(t, expectedField.Store, actualField.Store)
				assert.Equal(t, expectedField.Index, actualField.Index)
			}
		})
	}
}

func TestDocument_Unmarshal_InvalidData(t *testing.T) {
	tests := []struct {
		name        string
		expectedErr string
		data        []byte
	}{
		{
			name:        "empty data",
			data:        []byte{},
			expectedErr: "empty data for document",
		},
		{
			name:        "insufficient data for entity values",
			data:        []byte{0x01}, // Just a single byte, insufficient for varint length
			expectedErr: "failed to decode entity values",
		},
		{
			name:        "insufficient data for timestamp",
			data:        []byte{0x00}, // Empty entity values, but no timestamp data
			expectedErr: "failed to decode timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var document Document
			err := document.Unmarshal(tt.data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestDocument_Marshal_Unmarshal_RoundTrip(t *testing.T) {
	// Test round-trip marshaling/unmarshaling with a complex document
	original := Document{
		EntityValues: []byte("round-trip-test-entity"),
		Timestamp:    1640995200000,
		DocID:        987654321,
		Version:      42,
		Fields: []Field{
			{
				Key: FieldKey{
					TimeRange: &RangeOpts{
						Upper:         &BytesTermValue{Value: []byte("2024-12-31T23:59:59Z")},
						Lower:         &FloatTermValue{Value: 1672531200.0},
						IncludesUpper: true,
						IncludesLower: false,
					},
					Analyzer:    "standard",
					TagName:     "service.instance.name",
					SeriesID:    common.SeriesID(1234567890123456789),
					IndexRuleID: 999,
				},
				term:   &BytesTermValue{Value: []byte("test-instance")},
				NoSort: true,
				Store:  true,
				Index:  false,
			},
			{
				Key: FieldKey{
					TimeRange:   nil,
					Analyzer:    "keyword",
					TagName:     "status",
					SeriesID:    common.SeriesID(9876543210987654321),
					IndexRuleID: 888,
				},
				term:   &FloatTermValue{Value: 200.0},
				NoSort: false,
				Store:  false,
				Index:  true,
			},
		},
	}

	// First round-trip
	marshaled1, err := original.Marshal()
	require.NoError(t, err)

	var unmarshaled1 Document
	err = unmarshaled1.Unmarshal(marshaled1)
	require.NoError(t, err)

	// Second round-trip
	marshaled2, err := unmarshaled1.Marshal()
	require.NoError(t, err)

	var unmarshaled2 Document
	err = unmarshaled2.Unmarshal(marshaled2)
	require.NoError(t, err)

	// Verify all fields are preserved through multiple round-trips
	assert.Equal(t, original.EntityValues, unmarshaled2.EntityValues)
	assert.Equal(t, original.Timestamp, unmarshaled2.Timestamp)
	assert.Equal(t, original.DocID, unmarshaled2.DocID)
	assert.Equal(t, original.Version, unmarshaled2.Version)
	assert.Equal(t, len(original.Fields), len(unmarshaled2.Fields))

	// Verify each field is preserved
	for i, expectedField := range original.Fields {
		actualField := unmarshaled2.Fields[i]
		assert.Equal(t, expectedField.Key.Analyzer, actualField.Key.Analyzer)
		assert.Equal(t, expectedField.Key.TagName, actualField.Key.TagName)
		assert.Equal(t, expectedField.Key.SeriesID, actualField.Key.SeriesID)
		assert.Equal(t, expectedField.Key.IndexRuleID, actualField.Key.IndexRuleID)
		assert.Equal(t, expectedField.NoSort, actualField.NoSort)
		assert.Equal(t, expectedField.Store, actualField.Store)
		assert.Equal(t, expectedField.Index, actualField.Index)

		// Verify term values
		switch expected := expectedField.term.(type) {
		case *BytesTermValue:
			actual, ok := actualField.term.(*BytesTermValue)
			require.True(t, ok)
			assert.Equal(t, expected.Value, actual.Value)
		case *FloatTermValue:
			actual, ok := actualField.term.(*FloatTermValue)
			require.True(t, ok)
			assert.Equal(t, expected.Value, actual.Value)
		}
	}
}

func TestDocuments_Marshal_Unmarshal(t *testing.T) {
	tests := []struct {
		name      string
		documents Documents
	}{
		{
			name:      "Empty documents collection",
			documents: Documents{},
		},
		{
			name: "Single document",
			documents: Documents{
				{
					EntityValues: []byte("single-doc-entity"),
					Timestamp:    1640995200000,
					DocID:        12345,
					Version:      1,
					Fields: []Field{
						{
							Key: FieldKey{
								TimeRange:   nil,
								Analyzer:    "keyword",
								TagName:     "service.name",
								SeriesID:    common.SeriesID(123456789),
								IndexRuleID: 42,
							},
							term:   &BytesTermValue{Value: []byte("test-service")},
							NoSort: false,
							Store:  true,
							Index:  true,
						},
					},
				},
			},
		},
		{
			name: "Multiple documents with different field types",
			documents: Documents{
				{
					EntityValues: []byte("doc1-entity"),
					Timestamp:    1640995200000,
					DocID:        1,
					Version:      1,
					Fields: []Field{
						{
							Key: FieldKey{
								TimeRange: &RangeOpts{
									Upper:         &BytesTermValue{Value: []byte("2024-12-31")},
									Lower:         &BytesTermValue{Value: []byte("2024-01-01")},
									IncludesUpper: true,
									IncludesLower: false,
								},
								Analyzer:    "keyword",
								TagName:     "service.name",
								SeriesID:    common.SeriesID(111),
								IndexRuleID: 10,
							},
							term:   &BytesTermValue{Value: []byte("service-1")},
							NoSort: false,
							Store:  true,
							Index:  true,
						},
					},
				},
				{
					EntityValues: []byte("doc2-entity"),
					Timestamp:    1640995260000,
					DocID:        2,
					Version:      2,
					Fields: []Field{
						{
							Key: FieldKey{
								TimeRange: &RangeOpts{
									Upper:         &FloatTermValue{Value: 100.0},
									Lower:         &FloatTermValue{Value: 0.0},
									IncludesUpper: true,
									IncludesLower: true,
								},
								Analyzer:    "standard",
								TagName:     "response.time",
								SeriesID:    common.SeriesID(222),
								IndexRuleID: 20,
							},
							term:   &FloatTermValue{Value: 45.67},
							NoSort: true,
							Store:  false,
							Index:  true,
						},
					},
				},
				{
					EntityValues: []byte("doc3-entity"),
					Timestamp:    1640995320000,
					DocID:        3,
					Version:      3,
					Fields: []Field{
						{
							Key: FieldKey{
								TimeRange:   nil,
								Analyzer:    "simple",
								TagName:     "status",
								SeriesID:    common.SeriesID(333),
								IndexRuleID: 30,
							},
							term:   &BytesTermValue{Value: []byte("success")},
							NoSort: false,
							Store:  true,
							Index:  false,
						},
						{
							Key: FieldKey{
								TimeRange:   nil,
								Analyzer:    "standard",
								TagName:     "error.count",
								SeriesID:    common.SeriesID(444),
								IndexRuleID: 40,
							},
							term:   &FloatTermValue{Value: 0.0},
							NoSort: true,
							Store:  true,
							Index:  true,
						},
					},
				},
			},
		},
		{
			name: "Documents with empty fields",
			documents: Documents{
				{
					EntityValues: []byte{},
					Timestamp:    0,
					DocID:        0,
					Version:      0,
					Fields:       []Field{},
				},
				{
					EntityValues: []byte("empty-fields-entity"),
					Timestamp:    1640995200000,
					DocID:        999,
					Version:      1,
					Fields:       []Field{},
				},
			},
		},
		{
			name: "Large documents collection",
			documents: Documents{
				{
					EntityValues: bytes.Repeat([]byte("large-entity-1-"), 50),
					Timestamp:    1640995200000,
					DocID:        1001,
					Version:      1,
					Fields: []Field{
						{
							Key: FieldKey{
								TimeRange:   nil,
								Analyzer:    "keyword",
								TagName:     "large_field_1",
								SeriesID:    common.SeriesID(1001),
								IndexRuleID: 1001,
							},
							term:   &BytesTermValue{Value: bytes.Repeat([]byte("large-value-1-"), 25)},
							NoSort: false,
							Store:  true,
							Index:  true,
						},
					},
				},
				{
					EntityValues: bytes.Repeat([]byte("large-entity-2-"), 50),
					Timestamp:    1640995260000,
					DocID:        1002,
					Version:      2,
					Fields: []Field{
						{
							Key: FieldKey{
								TimeRange:   nil,
								Analyzer:    "standard",
								TagName:     "large_field_2",
								SeriesID:    common.SeriesID(1002),
								IndexRuleID: 1002,
							},
							term:   &FloatTermValue{Value: 999999.999},
							NoSort: true,
							Store:  false,
							Index:  true,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			marshaled, err := tt.documents.Marshal()
			require.NoError(t, err)
			assert.NotNil(t, marshaled)

			// For empty documents, the marshaled data should be minimal
			if len(tt.documents) == 0 {
				assert.Equal(t, 1, len(marshaled)) // Just the varint for count (0)
			} else {
				assert.Greater(t, len(marshaled), 0)
			}

			// Unmarshal
			var unmarshaled Documents
			err = unmarshaled.Unmarshal(marshaled)
			require.NoError(t, err)

			// Verify documents count
			assert.Equal(t, len(tt.documents), len(unmarshaled))

			// Verify each document
			for i, expectedDoc := range tt.documents {
				require.Less(t, i, len(unmarshaled))
				actualDoc := unmarshaled[i]

				// Verify basic fields
				assert.Equal(t, expectedDoc.EntityValues, actualDoc.EntityValues)
				assert.Equal(t, expectedDoc.Timestamp, actualDoc.Timestamp)
				assert.Equal(t, expectedDoc.DocID, actualDoc.DocID)
				assert.Equal(t, expectedDoc.Version, actualDoc.Version)

				// Verify fields count
				assert.Equal(t, len(expectedDoc.Fields), len(actualDoc.Fields))

				// Verify each field
				for j, expectedField := range expectedDoc.Fields {
					require.Less(t, j, len(actualDoc.Fields))
					actualField := actualDoc.Fields[j]

					// Check FieldKey
					assert.Equal(t, expectedField.Key.Analyzer, actualField.Key.Analyzer)
					assert.Equal(t, expectedField.Key.TagName, actualField.Key.TagName)
					assert.Equal(t, expectedField.Key.SeriesID, actualField.Key.SeriesID)
					assert.Equal(t, expectedField.Key.IndexRuleID, actualField.Key.IndexRuleID)

					// Check TimeRange
					if expectedField.Key.TimeRange != nil {
						assert.NotNil(t, actualField.Key.TimeRange)
						assert.Equal(t, expectedField.Key.TimeRange.IncludesUpper, actualField.Key.TimeRange.IncludesUpper)
						assert.Equal(t, expectedField.Key.TimeRange.IncludesLower, actualField.Key.TimeRange.IncludesLower)

						// Compare Upper bound
						switch expected := expectedField.Key.TimeRange.Upper.(type) {
						case *BytesTermValue:
							actual, ok := actualField.Key.TimeRange.Upper.(*BytesTermValue)
							require.True(t, ok)
							assert.Equal(t, expected.Value, actual.Value)
						case *FloatTermValue:
							actual, ok := actualField.Key.TimeRange.Upper.(*FloatTermValue)
							require.True(t, ok)
							assert.Equal(t, expected.Value, actual.Value)
						}

						// Compare Lower bound
						switch expected := expectedField.Key.TimeRange.Lower.(type) {
						case *BytesTermValue:
							actual, ok := actualField.Key.TimeRange.Lower.(*BytesTermValue)
							require.True(t, ok)
							assert.Equal(t, expected.Value, actual.Value)
						case *FloatTermValue:
							actual, ok := actualField.Key.TimeRange.Lower.(*FloatTermValue)
							require.True(t, ok)
							assert.Equal(t, expected.Value, actual.Value)
						}
					} else {
						assert.Nil(t, actualField.Key.TimeRange)
					}

					// Check term value
					switch expected := expectedField.term.(type) {
					case *BytesTermValue:
						actual, ok := actualField.term.(*BytesTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					case *FloatTermValue:
						actual, ok := actualField.term.(*FloatTermValue)
						require.True(t, ok)
						assert.Equal(t, expected.Value, actual.Value)
					}

					// Check flags
					assert.Equal(t, expectedField.NoSort, actualField.NoSort)
					assert.Equal(t, expectedField.Store, actualField.Store)
					assert.Equal(t, expectedField.Index, actualField.Index)
				}
			}
		})
	}
}

func TestDocuments_Unmarshal_InvalidData(t *testing.T) {
	tests := []struct {
		name        string
		expectedErr string
		data        []byte
	}{
		{
			name:        "insufficient data for documents",
			data:        []byte{0x02}, // Claim 2 documents but no actual data
			expectedErr: "insufficient data for documents",
		},
		{
			name:        "invalid document data",
			data:        []byte{0x01, 0x01, 0x00}, // 1 document, but invalid document data
			expectedErr: "failed to unmarshal document",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var documents Documents
			err := documents.Unmarshal(tt.data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestDocuments_Marshal_Unmarshal_RoundTrip(t *testing.T) {
	// Test round-trip marshaling/unmarshaling with a complex documents collection
	original := Documents{
		{
			EntityValues: []byte("round-trip-doc1-entity"),
			Timestamp:    1640995200000,
			DocID:        987654321,
			Version:      42,
			Fields: []Field{
				{
					Key: FieldKey{
						TimeRange: &RangeOpts{
							Upper:         &BytesTermValue{Value: []byte("2024-12-31T23:59:59Z")},
							Lower:         &FloatTermValue{Value: 1672531200.0},
							IncludesUpper: true,
							IncludesLower: false,
						},
						Analyzer:    "standard",
						TagName:     "service.instance.name",
						SeriesID:    common.SeriesID(1234567890123456789),
						IndexRuleID: 999,
					},
					term:   &BytesTermValue{Value: []byte("test-instance")},
					NoSort: true,
					Store:  true,
					Index:  false,
				},
			},
		},
		{
			EntityValues: []byte("round-trip-doc2-entity"),
			Timestamp:    1640995260000,
			DocID:        987654322,
			Version:      43,
			Fields: []Field{
				{
					Key: FieldKey{
						TimeRange:   nil,
						Analyzer:    "keyword",
						TagName:     "status",
						SeriesID:    common.SeriesID(9876543210987654321),
						IndexRuleID: 888,
					},
					term:   &FloatTermValue{Value: 200.0},
					NoSort: false,
					Store:  false,
					Index:  true,
				},
			},
		},
	}

	// First round-trip
	marshaled1, err := original.Marshal()
	require.NoError(t, err)

	var unmarshaled1 Documents
	err = unmarshaled1.Unmarshal(marshaled1)
	require.NoError(t, err)

	// Second round-trip
	marshaled2, err := unmarshaled1.Marshal()
	require.NoError(t, err)

	var unmarshaled2 Documents
	err = unmarshaled2.Unmarshal(marshaled2)
	require.NoError(t, err)

	// Verify all documents are preserved through multiple round-trips
	assert.Equal(t, len(original), len(unmarshaled2))

	// Verify each document is preserved
	for i, expectedDoc := range original {
		actualDoc := unmarshaled2[i]
		assert.Equal(t, expectedDoc.EntityValues, actualDoc.EntityValues)
		assert.Equal(t, expectedDoc.Timestamp, actualDoc.Timestamp)
		assert.Equal(t, expectedDoc.DocID, actualDoc.DocID)
		assert.Equal(t, expectedDoc.Version, actualDoc.Version)
		assert.Equal(t, len(expectedDoc.Fields), len(actualDoc.Fields))

		// Verify each field is preserved
		for j, expectedField := range expectedDoc.Fields {
			actualField := actualDoc.Fields[j]
			assert.Equal(t, expectedField.Key.Analyzer, actualField.Key.Analyzer)
			assert.Equal(t, expectedField.Key.TagName, actualField.Key.TagName)
			assert.Equal(t, expectedField.Key.SeriesID, actualField.Key.SeriesID)
			assert.Equal(t, expectedField.Key.IndexRuleID, actualField.Key.IndexRuleID)
			assert.Equal(t, expectedField.NoSort, actualField.NoSort)
			assert.Equal(t, expectedField.Store, actualField.Store)
			assert.Equal(t, expectedField.Index, actualField.Index)

			// Verify term values
			switch expected := expectedField.term.(type) {
			case *BytesTermValue:
				actual, ok := actualField.term.(*BytesTermValue)
				require.True(t, ok)
				assert.Equal(t, expected.Value, actual.Value)
			case *FloatTermValue:
				actual, ok := actualField.term.(*FloatTermValue)
				require.True(t, ok)
				assert.Equal(t, expected.Value, actual.Value)
			}
		}
	}
}
