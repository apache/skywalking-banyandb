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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLabels_Empty(t *testing.T) {
	labels, err := parseLabels("")

	assert.NoError(t, err)
	assert.Empty(t, labels)
}

func TestParseLabels_SingleLabel(t *testing.T) {
	labels, err := parseLabels("env=test")

	assert.NoError(t, err)
	assert.Len(t, labels, 1)
	assert.Equal(t, "test", labels["env"])
}

func TestParseLabels_MultipleLabels(t *testing.T) {
	labels, err := parseLabels("env=test,region=us-west-1,zone=1a")

	assert.NoError(t, err)
	assert.Len(t, labels, 3)
	assert.Equal(t, "test", labels["env"])
	assert.Equal(t, "us-west-1", labels["region"])
	assert.Equal(t, "1a", labels["zone"])
}

func TestParseLabels_WithSpaces(t *testing.T) {
	labels, err := parseLabels(" env = test , region = us-west-1 ")

	assert.NoError(t, err)
	assert.Len(t, labels, 2)
	assert.Equal(t, "test", labels["env"])
	assert.Equal(t, "us-west-1", labels["region"])
}

func TestParseLabels_InvalidFormat(t *testing.T) {
	_, err := parseLabels("invalid,key=value,noequals")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid label format")
}

func TestParseLabels_EmptyValues(t *testing.T) {
	_, err := parseLabels("key1=,key2=value2,=invalid")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty label")
}

func TestParseLabels_MissingEquals(t *testing.T) {
	_, err := parseLabels("validkey=value,invalidlabel")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid label format")
	assert.Contains(t, err.Error(), "invalidlabel")
}

func TestParseLabels_EmptyKey(t *testing.T) {
	_, err := parseLabels("=value")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty label key")
}

func TestParseLabels_EmptyValue(t *testing.T) {
	_, err := parseLabels("key=")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty label value")
}
