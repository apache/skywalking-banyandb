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

package model

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestStreamResult_CopyFrom(t *testing.T) {
	tests := []struct {
		sr       *StreamResult
		other    *StreamResult
		name     string
		wantTS   []int64
		wantTags []TagFamily
		wantSIDs []common.SeriesID
		topN     int
		wantLen  int
		asc      bool
		wantRet  bool
	}{
		{
			name:     "both empty",
			sr:       NewStreamResult(3, true),
			other:    NewStreamResult(3, true),
			topN:     3,
			asc:      true,
			wantLen:  0,
			wantRet:  false,
			wantTS:   []int64{},
			wantTags: []TagFamily{},
			wantSIDs: []common.SeriesID{},
		},
		{
			name: "one with data, one empty",
			sr: &StreamResult{
				asc:        true,
				topN:       3,
				Timestamps: []int64{4, 2},
				ElementIDs: []uint64{100, 101},
				TagFamilies: []TagFamily{
					{
						Name: "family1",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value1"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value2"},
									},
								},
							},
						}},
					},
					{
						Name: "family2",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value1"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value2"},
									},
								},
							},
						}},
					},
				},
				SIDs: []common.SeriesID{1, 2},
			},
			other:   NewStreamResult(3, true),
			topN:    3,
			asc:     true,
			wantLen: 2,
			wantRet: false,
			wantTS:  []int64{4, 2},
			wantTags: []TagFamily{
				{
					Name: "family1",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value1"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value2"},
								},
							},
						},
					}},
				},
				{
					Name: "family2",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value1"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value2"},
								},
							},
						},
					}},
				},
			},
			wantSIDs: []common.SeriesID{1, 2},
		},
		{
			name: "one empty, one with data",
			sr:   NewStreamResult(3, true),
			other: &StreamResult{
				asc:        true,
				topN:       3,
				Timestamps: []int64{4, 2},
				ElementIDs: []uint64{100, 101},
				TagFamilies: []TagFamily{
					{
						Name: "family1",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value1"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value2"},
									},
								},
							},
						}},
					},
					{
						Name: "family2",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value1"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value2"},
									},
								},
							},
						}},
					},
				},
				SIDs: []common.SeriesID{1, 2},
			},
			topN:    3,
			asc:     true,
			wantLen: 2,
			wantRet: false,
			wantTS:  []int64{4, 2},
			wantTags: []TagFamily{
				{
					Name: "family1",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value1"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value2"},
								},
							},
						},
					}},
				},
				{
					Name: "family2",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value1"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value2"},
								},
							},
						},
					}},
				},
			},
			wantSIDs: []common.SeriesID{1, 2},
		},
		{
			name: "both have data, combined < topN",
			sr: &StreamResult{
				asc:        true,
				topN:       5,
				Timestamps: []int64{10, 30},
				ElementIDs: []uint64{1, 2},
				TagFamilies: []TagFamily{
					{
						Name: "family1",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value11"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value12"},
									},
								},
							},
						}},
					},
					{
						Name: "family2",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value21"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value22"},
									},
								},
							},
						}},
					},
				},
				SIDs: []common.SeriesID{1, 2},
			},
			other: &StreamResult{
				asc:        true,
				topN:       5,
				Timestamps: []int64{20},
				ElementIDs: []uint64{3},
				TagFamilies: []TagFamily{
					{
						Name: "family1",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value13"},
									},
								},
							},
						}},
					},
					{
						Name: "family2",
						Tags: []Tag{
							{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value23"},
										},
									},
								},
							},
						},
					},
				},
				SIDs: []common.SeriesID{3},
			},
			topN:    5,
			asc:     true,
			wantLen: 3,
			wantRet: false,
			wantTS:  []int64{10, 20, 30},
			wantTags: []TagFamily{
				{
					Name: "family1",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value11"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value13"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value12"},
								},
							},
						},
					}},
				},
				{
					Name: "family2",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value21"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value23"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value22"},
								},
							},
						},
					}},
				},
			},
			wantSIDs: []common.SeriesID{1, 3, 2},
		},
		{
			name: "both have data, combined >= topN",
			sr: &StreamResult{
				asc:        false,
				topN:       3,
				Timestamps: []int64{30, 20, 10},
				ElementIDs: []uint64{5, 6, 7},
				TagFamilies: []TagFamily{
					{
						Name: "family1",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value11"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value12"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value13"},
									},
								},
							},
						}},
					},
					{
						Name: "family2",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value21"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value22"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value23"},
									},
								},
							},
						}},
					},
				},
				SIDs: []common.SeriesID{1, 2, 3},
			},
			other: &StreamResult{
				asc:        false,
				topN:       3,
				Timestamps: []int64{50, 40},
				ElementIDs: []uint64{8, 9},
				TagFamilies: []TagFamily{
					{
						Name: "family1",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value14"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value15"},
									},
								},
							},
						}},
					},
					{
						Name: "family2",
						Tags: []Tag{{
							Name: "tag1",
							Values: []*modelv1.TagValue{
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value24"},
									},
								},
								{
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: "value25"},
									},
								},
							},
						}},
					},
				},
				SIDs: []common.SeriesID{4, 5},
			},
			topN:    3,
			asc:     false,
			wantLen: 3,
			wantRet: true,
			wantTS:  []int64{50, 40, 30},
			wantTags: []TagFamily{
				{
					Name: "family1",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value14"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value15"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value11"},
								},
							},
						},
					}},
				},
				{
					Name: "family2",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value24"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value25"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value21"},
								},
							},
						},
					}},
				},
			},
			wantSIDs: []common.SeriesID{4, 5, 1},
		},
	}
	tmp := &StreamResult{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.sr.CopyFrom(tmp, tt.other)
			assert.Equal(t, tt.wantLen, tt.sr.Len(), "unexpected length")
			assert.Equal(t, tt.wantRet, got, "unexpected return value")
			assert.Equal(t, tt.wantTS, tt.sr.Timestamps, "unexpected timestamps")
			assert.Equal(t, tt.wantTags, tt.sr.TagFamilies, "unexpected tag families")
			assert.Equal(t, tt.wantSIDs, tt.sr.SIDs, "unexpected SIDs")
		})
	}
}

func TestMergeStreamResults(t *testing.T) {
	tests := []struct {
		name     string
		results  []*StreamResult
		wantTS   []int64
		wantTags []TagFamily
		wantSIDs []common.SeriesID
		topN     int
		asc      bool
	}{
		{
			name:     "all empty",
			results:  []*StreamResult{NewStreamResult(3, true), NewStreamResult(3, true)},
			topN:     3,
			asc:      true,
			wantTS:   nil,
			wantTags: nil,
			wantSIDs: nil,
		},
		{
			name: "one with data, one empty",
			results: []*StreamResult{
				{
					asc:        true,
					topN:       3,
					Timestamps: []int64{2, 4},
					ElementIDs: []uint64{100, 101},
					TagFamilies: []TagFamily{
						{
							Name: "family1",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value1"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value2"},
										},
									},
								},
							}},
						},
						{
							Name: "family2",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value1"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value2"},
										},
									},
								},
							}},
						},
					},
					SIDs: []common.SeriesID{1, 2},
				},
				NewStreamResult(3, true),
			},
			topN:   3,
			asc:    true,
			wantTS: []int64{2, 4},
			wantTags: []TagFamily{
				{
					Name: "family1",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value1"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value2"},
								},
							},
						},
					}},
				},
				{
					Name: "family2",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value1"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value2"},
								},
							},
						},
					}},
				},
			},
			wantSIDs: []common.SeriesID{1, 2},
		},
		{
			name: "both have data, combined < topN",
			results: []*StreamResult{
				{
					asc:        true,
					topN:       5,
					Timestamps: []int64{10, 30},
					ElementIDs: []uint64{1, 2},
					TagFamilies: []TagFamily{
						{
							Name: "family1",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value11"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value12"},
										},
									},
								},
							}},
						},
						{
							Name: "family2",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value21"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value22"},
										},
									},
								},
							}},
						},
					},
					SIDs: []common.SeriesID{1, 2},
				},
				{
					asc:        true,
					topN:       5,
					Timestamps: []int64{20},
					ElementIDs: []uint64{3},
					TagFamilies: []TagFamily{
						{
							Name: "family1",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value13"},
										},
									},
								},
							}},
						},
						{
							Name: "family2",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value23"},
										},
									},
								},
							}},
						},
					},
					SIDs: []common.SeriesID{3},
				},
			},
			topN:   5,
			asc:    true,
			wantTS: []int64{10, 20, 30},
			wantTags: []TagFamily{
				{
					Name: "family1",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value11"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value13"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value12"},
								},
							},
						},
					}},
				},
				{
					Name: "family2",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value21"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value23"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value22"},
								},
							},
						},
					}},
				},
			},
			wantSIDs: []common.SeriesID{1, 3, 2},
		},
		{
			name: "both have data, combined >= topN",
			results: []*StreamResult{
				{
					asc:        false,
					topN:       3,
					Timestamps: []int64{30, 20, 10},
					ElementIDs: []uint64{5, 6, 7},
					TagFamilies: []TagFamily{
						{
							Name: "family1",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value11"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value12"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value13"},
										},
									},
								},
							}},
						},
						{
							Name: "family2",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value21"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value22"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value23"},
										},
									},
								},
							}},
						},
					},
					SIDs: []common.SeriesID{1, 2, 3},
				},
				{
					asc:        false,
					topN:       3,
					Timestamps: []int64{50, 40},
					ElementIDs: []uint64{8, 9},
					TagFamilies: []TagFamily{
						{
							Name: "family1",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value14"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value15"},
										},
									},
								},
							}},
						},
						{
							Name: "family2",
							Tags: []Tag{{
								Name: "tag1",
								Values: []*modelv1.TagValue{
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value24"},
										},
									},
									{
										Value: &modelv1.TagValue_Str{
											Str: &modelv1.Str{Value: "value25"},
										},
									},
								},
							}},
						},
					},
					SIDs: []common.SeriesID{4, 5},
				},
			},
			topN:   3,
			asc:    false,
			wantTS: []int64{50, 40, 30},
			wantTags: []TagFamily{
				{
					Name: "family1",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value14"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value15"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value11"},
								},
							},
						},
					}},
				},
				{
					Name: "family2",
					Tags: []Tag{{
						Name: "tag1",
						Values: []*modelv1.TagValue{
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value24"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value25"},
								},
							},
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: "value21"},
								},
							},
						},
					}},
				},
			},
			wantSIDs: []common.SeriesID{4, 5, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeStreamResults(tt.results, tt.topN, tt.asc)
			assert.Equal(t, tt.wantTS, got.Timestamps, "unexpected timestamps")
			assert.Equal(t, tt.wantTags, got.TagFamilies, "unexpected tag families")
			assert.Equal(t, tt.wantSIDs, got.SIDs, "unexpected SIDs")
		})
	}
}

func TestStreamResult_TopNTooLarge(t *testing.T) {
	tests := []struct {
		name    string
		topN    int
		asc     bool
		wantLen int
	}{
		{
			name:    "topN exceeds maxTopN",
			topN:    math.MaxUint32,
			asc:     true,
			wantLen: maxTopN,
		},
		{
			name:    "topN within limit",
			topN:    maxTopN - 5,
			asc:     true,
			wantLen: maxTopN - 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sr := NewStreamResult(tt.topN, tt.asc)
			assert.Equal(t, tt.wantLen, cap(sr.Timestamps), "unexpected capacity")
		})
	}
}
