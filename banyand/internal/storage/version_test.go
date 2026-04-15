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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSegmentMeta_NewFormat(t *testing.T) {
	data := []byte(`{"version":"1.4.0","endTime":"2026-04-07T00:00:00+08:00"}`)
	meta, err := readSegmentMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "1.4.0", meta.Version)
	assert.Equal(t, "2026-04-07T00:00:00+08:00", meta.EndTime)
}

func TestReadSegmentMeta_OldFormat(t *testing.T) {
	data := []byte("1.4.0")
	meta, err := readSegmentMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "1.4.0", meta.Version)
	assert.Equal(t, "", meta.EndTime)
}

func TestReadSegmentMeta_OldFormatWithNewline(t *testing.T) {
	data := []byte("1.4.0\n")
	meta, err := readSegmentMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "1.4.0", meta.Version)
	assert.Equal(t, "", meta.EndTime)
}

func TestReadSegmentMeta_IncompatibleVersion(t *testing.T) {
	data := []byte(`{"version":"0.1.0","endTime":"2026-04-07T00:00:00+08:00"}`)
	_, err := readSegmentMeta(data)
	assert.Error(t, err)
}

func TestReadSegmentMeta_NewFormatNoEndTime(t *testing.T) {
	data := []byte(`{"version":"1.4.0"}`)
	meta, err := readSegmentMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "1.4.0", meta.Version)
	assert.Equal(t, "", meta.EndTime)
}
