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

package stream

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestDisableRetentionByStage(t *testing.T) {
	tempDir := t.TempDir()
	l := logger.GetLogger("test-stream")

	tests := []struct {
		nodeLabels   map[string]string
		name         string
		description  string
		stages       []*commonv1.LifecycleStage
		wantDisabled bool
	}{
		{
			name: "two stages - node in first stage",
			stages: []*commonv1.LifecycleStage{
				{
					Name:         "cold",
					NodeSelector: "role=cold",
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  4,
					},
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					ShardNum: 2,
				},
			},
			nodeLabels: map[string]string{
				"role": "hot",
			},
			wantDisabled: true,
			description:  "When node is in first stage of two stages, retention should be disabled",
		},
		{
			name: "two stages - node in last stage",
			stages: []*commonv1.LifecycleStage{
				{
					Name:         "cold",
					NodeSelector: "role=cold",
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  4,
					},
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					ShardNum: 2,
				},
			},
			nodeLabels: map[string]string{
				"role": "cold",
			},
			wantDisabled: false,
			description:  "When node is in last stage of two stages, retention should be enabled",
		},
		{
			name: "three stages - node in warm stage",
			stages: []*commonv1.LifecycleStage{
				{
					Name:         "warm",
					NodeSelector: "role=warm",
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  3,
					},
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					ShardNum: 2,
				},
				{
					Name:         "cold",
					NodeSelector: "role=cold",
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  2,
					},
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					ShardNum: 2,
				},
			},
			nodeLabels: map[string]string{
				"role": "warm",
			},
			wantDisabled: true,
			description:  "When node is in middle stage of three stages, retention should be disabled",
		},
		{
			name: "three stages - node in cold stage",
			stages: []*commonv1.LifecycleStage{
				{
					Name:         "warm",
					NodeSelector: "role=warm",
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  3,
					},
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					ShardNum: 2,
				},
				{
					Name:         "cold",
					NodeSelector: "role=cold",
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  2,
					},
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					ShardNum: 2,
				},
			},
			nodeLabels: map[string]string{
				"role": "cold",
			},
			wantDisabled: false,
			description:  "When node is in middle stage of three stages, retention should be disabled",
		},
		{
			name:   "single stage - node in only stage",
			stages: []*commonv1.LifecycleStage{},
			nodeLabels: map[string]string{
				"role": "hot",
			},
			wantDisabled: false,
			description:  "When node is in the only stage, retention should be enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a unique subdirectory for this test
			testDir := filepath.Join(tempDir, tt.name)
			err := os.MkdirAll(testDir, 0o755)
			require.NoError(t, err)

			// Create supplier with node labels
			omr := observability.NewBypassRegistry()
			pm := protector.NewMemory(omr)

			s := &supplier{
				path:       testDir,
				metadata:   nil, // Not needed for this test
				option:     option{},
				omr:        omr,
				pm:         pm,
				l:          l,
				schemaRepo: nil,
				nodeLabels: tt.nodeLabels,
			}

			// Create group schema with stages
			groupSchema := &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: "test-group-" + tt.name,
				},
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 2,
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  7,
					},
					Stages: tt.stages,
				},
			}

			// Call OpenDB
			db, err := s.OpenDB(groupSchema)
			require.NoError(t, err, "OpenDB should not return error")
			require.NotNil(t, db, "DB should not be nil")
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()

			// Verify DisableRetention setting using reflection
			// Since db is a generic type and disableRetention is private,
			// we need to use reflection to access the field
			dbValue := reflect.ValueOf(db)
			if dbValue.Kind() == reflect.Ptr {
				dbValue = dbValue.Elem()
			}

			disableRetentionField := dbValue.FieldByName("disableRetention")
			require.True(t, disableRetentionField.IsValid(), "disableRetention field should exist")

			actualDisabled := disableRetentionField.Bool()
			assert.Equal(t, tt.wantDisabled, actualDisabled, tt.description)
		})
	}
}
