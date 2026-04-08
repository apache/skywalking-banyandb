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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseNameFromYAMLEmptyGroupsList(t *testing.T) {
	filePath = "-"
	yaml := `
name: test-resource
groups: []
`
	reader := bytes.NewBufferString(yaml)
	requests, err := parseNameFromYAML(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "please specify a non-empty groups list")
	assert.Nil(t, requests)
}

func TestParseNameFromYAMLNonStringGroup(t *testing.T) {
	filePath = "-"
	yaml := `
name: test-resource
groups: [123]
`
	reader := bytes.NewBufferString(yaml)
	requests, err := parseNameFromYAML(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "the first element of groups must be a string")
	assert.Nil(t, requests)
}

func TestParseNameFromYAMLNonStringGroupObject(t *testing.T) {
	filePath = "-"
	yaml := `
name: test-resource
groups: [{"key": "value"}]
`
	reader := bytes.NewBufferString(yaml)
	requests, err := parseNameFromYAML(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "the first element of groups must be a string")
	assert.Nil(t, requests)
}

func TestParseNameFromYAMLValidGroups(t *testing.T) {
	filePath = "-"
	yaml := `
name: test-resource
groups: [test-group]
`
	reader := bytes.NewBufferString(yaml)
	requests, err := parseNameFromYAML(reader)
	assert.NoError(t, err)
	assert.Len(t, requests, 1)
	assert.Equal(t, "test-resource", requests[0].name)
	assert.Equal(t, "test-group", requests[0].group)
}

func TestParseFromYAMLForPropertyEmptyID(t *testing.T) {
	filePath = "-"
	yaml := `
metadata:
  name: test-property
  group: test-group
id: ""
`
	reader := bytes.NewBufferString(yaml)
	requests, err := parseFromYAMLForProperty(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "please provide a non-empty id in input json")
	assert.Nil(t, requests)
}

func TestParseFromYAMLForPropertyMissingID(t *testing.T) {
	filePath = "-"
	yaml := `
metadata:
  name: test-property
  group: test-group
`
	reader := bytes.NewBufferString(yaml)
	requests, err := parseFromYAMLForProperty(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "absent node: id")
	assert.Nil(t, requests)
}

func TestParseFromYAMLForPropertyValidID(t *testing.T) {
	filePath = "-"
	yaml := `
metadata:
  name: test-property
  group: test-group
id: test-id-123
`
	reader := bytes.NewBufferString(yaml)
	requests, err := parseFromYAMLForProperty(reader)
	assert.NoError(t, err)
	assert.Len(t, requests, 1)
	assert.Equal(t, "test-property", requests[0].name)
	assert.Equal(t, "test-group", requests[0].group)
	assert.Equal(t, "test-id-123", requests[0].id)
}
