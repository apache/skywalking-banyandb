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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/schema/topnschema"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

func topNColumns() []csvfmt.Column {
	cols := []csvfmt.Column{{Header: "_name", Kind: csvfmt.KindName}}
	for _, name := range topnschema.TagNames {
		cols = append(cols, csvfmt.Column{Header: topnschema.TagFamily + "." + name, Family: topnschema.TagFamily, Name: name})
	}
	return cols
}

// A _top_n_result series flushed into two shards is exported once from each and answered as one point.
func TestTrackCountsTopNRowsOfOneSeriesAndTimeOnce(t *testing.T) {
	g := &groupResult{units: map[*manifest.Unit]map[string]*unitRows{}}
	u := &manifest.Unit{}
	cols := topNColumns()
	layer := []string{topnschema.Name, "endpoint_sla-layer", "2", "GENERAL", `{"limit":1000}`}
	service := []string{topnschema.Name, "endpoint_sla-service", "2", "svc-a", `{"limit":1000}`}
	g.track(u, topnschema.Name, "100", seriesOf(cols, layer, topnschema.Name))   // shard 0
	g.track(u, topnschema.Name, "100", seriesOf(cols, layer, topnschema.Name))   // shard 1
	g.track(u, topnschema.Name, "100", seriesOf(cols, service, topnschema.Name)) // another series
	g.track(u, topnschema.Name, "160", seriesOf(cols, layer, topnschema.Name))   // another time
	rows := g.units[u][topnschema.Name]
	assert.EqualValues(t, 4, rows.n)
	assert.EqualValues(t, 3, rows.points)
	assert.EqualValues(t, 160, rows.maxTS)
}

// Rows of any other resource are points of their own: a repeated one must still be found.
func TestTrackCountsEveryRowOfOtherResources(t *testing.T) {
	g := &groupResult{units: map[*manifest.Unit]map[string]*unitRows{}}
	u := &manifest.Unit{}
	row := []string{"service_cpm_minute", "a", "b"}
	cols := []csvfmt.Column{{Header: "_name", Kind: csvfmt.KindName}}
	g.track(u, "service_cpm_minute", "100", seriesOf(cols, row, "service_cpm_minute"))
	g.track(u, "service_cpm_minute", "100", seriesOf(cols, row, "service_cpm_minute"))
	rows := g.units[u]["service_cpm_minute"]
	assert.EqualValues(t, 2, rows.n)
	assert.EqualValues(t, 2, rows.points)
}
