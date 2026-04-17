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

// TagType represents the type of a tag in the measure schema.
type TagType int

const (
	TagTypeString TagType = iota
	TagTypeInt
	TagTypeStringArray
	TagTypeIntArray
)

// FieldKind represents the type of a field in the measure schema.
type FieldKind int

const (
	FieldKindInt FieldKind = iota
	FieldKindFloat
)

// TagDef defines a tag in a measure schema.
type TagDef struct {
	Name     string
	Family   string
	Type     TagType
	IsEntity bool
}

// FieldDef defines a field in a measure schema.
type FieldDef struct {
	Name string
	Kind FieldKind
}

// DataPoint represents a single seed data point.
type DataPoint struct {
	TagValues   map[string]interface{} // tag name -> value (string, int64, []string, []int64)
	FieldValues map[string]interface{} // field name -> value (int64, float64)
}

// Measure represents a test measure with its schema and seed data.
type Measure struct {
	Name       string
	Group      string
	Tags       []TagDef
	Fields     []FieldDef
	DataPoints []DataPoint
	IndexMode  bool
}

// SeedData returns all target measures with their seed data.
func SeedData() []Measure {
	return []Measure{
		serviceCPMMinute(),
		serviceTraffic(),
		serviceInstanceTraffic(),
		instanceCPUCPUMinute(),
	}
}

// FindMeasure returns a measure by name from the predefined seed data.
func FindMeasure(name string) *Measure {
	allMeasures := SeedData()
	for idx := range allMeasures {
		if allMeasures[idx].Name == name {
			return &allMeasures[idx]
		}
	}
	return nil
}

func serviceCPMMinute() Measure {
	return Measure{
		Name:  "service_cpm_minute",
		Group: "sw_metric",
		Tags: []TagDef{
			{Name: "id", Type: TagTypeString, Family: "default"},
			{Name: "entity_id", Type: TagTypeString, Family: "default", IsEntity: true},
		},
		Fields: []FieldDef{
			{Name: "total", Kind: FieldKindInt},
			{Name: "value", Kind: FieldKindInt},
		},
		DataPoints: []DataPoint{
			{TagValues: map[string]interface{}{"id": "svc1", "entity_id": "entity_1"}, FieldValues: map[string]interface{}{"total": int64(100), "value": int64(1)}},
			{TagValues: map[string]interface{}{"id": "svc1", "entity_id": "entity_2"}, FieldValues: map[string]interface{}{"total": int64(100), "value": int64(2)}},
			{TagValues: map[string]interface{}{"id": "svc1", "entity_id": "entity_3"}, FieldValues: map[string]interface{}{"total": int64(100), "value": int64(3)}},
			{TagValues: map[string]interface{}{"id": "svc2", "entity_id": "entity_4"}, FieldValues: map[string]interface{}{"total": int64(100), "value": int64(5)}},
			{TagValues: map[string]interface{}{"id": "svc2", "entity_id": "entity_5"}, FieldValues: map[string]interface{}{"total": int64(50), "value": int64(4)}},
			{TagValues: map[string]interface{}{"id": "svc3", "entity_id": "entity_6"}, FieldValues: map[string]interface{}{"total": int64(300), "value": int64(6)}},
		},
	}
}

func serviceTraffic() Measure {
	return Measure{
		Name:      "service_traffic",
		Group:     "index_mode",
		IndexMode: true,
		Tags: []TagDef{
			{Name: "id", Type: TagTypeString, Family: "default", IsEntity: true},
			{Name: "service_id", Type: TagTypeString, Family: "default"},
			{Name: "name", Type: TagTypeString, Family: "default"},
			{Name: "short_name", Type: TagTypeString, Family: "default"},
			{Name: "service_group", Type: TagTypeString, Family: "default"},
			{Name: "layer", Type: TagTypeInt, Family: "default"},
		},
		Fields: []FieldDef{},
		DataPoints: []DataPoint{
			{TagValues: map[string]interface{}{
				"id": "1", "service_id": "service_1", "name": "service_name_1",
				"short_name": "service_short_name_1", "service_group": "group1", "layer": int64(1),
			}},
			{TagValues: map[string]interface{}{
				"id": "2", "service_id": "service_2", "name": "service_name_2",
				"short_name": "service_short_name_2", "service_group": "group1", "layer": int64(2),
			}},
			{TagValues: map[string]interface{}{
				"id": "3", "service_id": "service_3", "name": "service_name_3",
				"short_name": "service_short_name_3", "service_group": "group1", "layer": int64(0),
			}},
		},
	}
}

func serviceInstanceTraffic() Measure {
	return Measure{
		Name:  "service_instance_traffic",
		Group: "sw_metric",
		Tags: []TagDef{
			{Name: "id", Type: TagTypeString, Family: "default", IsEntity: true},
			{Name: "service_id", Type: TagTypeString, Family: "default"},
			{Name: "name", Type: TagTypeString, Family: "default"},
			{Name: "last_ping", Type: TagTypeInt, Family: "default"},
			{Name: "layer", Type: TagTypeInt, Family: "default"},
		},
		Fields: []FieldDef{},
		DataPoints: []DataPoint{
			{TagValues: map[string]interface{}{"id": "1", "service_id": "svc_1", "name": "nodea@west-us", "last_ping": int64(100), "layer": int64(0)}},
			{TagValues: map[string]interface{}{"id": "2", "service_id": "svc_1", "name": "nodeb@west-us", "last_ping": int64(100), "layer": int64(0)}},
			{TagValues: map[string]interface{}{"id": "3", "service_id": "svc_1", "name": "nodea@east-cn", "last_ping": int64(100), "layer": int64(0)}},
		},
	}
}

func instanceCPUCPUMinute() Measure {
	return Measure{
		Name:  "instance_clr_cpu_minute",
		Group: "sw_metric",
		Tags: []TagDef{
			{Name: "service_id", Type: TagTypeString, Family: "default"},
			{Name: "entity_id", Type: TagTypeString, Family: "default", IsEntity: true},
		},
		Fields: []FieldDef{
			{Name: "summation", Kind: FieldKindFloat},
			{Name: "count", Kind: FieldKindInt},
			{Name: "value", Kind: FieldKindFloat},
		},
		DataPoints: []DataPoint{
			{
				TagValues:   map[string]interface{}{"service_id": "1", "entity_id": "entity_1"},
				FieldValues: map[string]interface{}{"summation": 18.1, "count": int64(1), "value": 18.1},
			},
			{
				TagValues:   map[string]interface{}{"service_id": "4", "entity_id": "entity_2"},
				FieldValues: map[string]interface{}{"summation": 100.4, "count": int64(2), "value": 50.2},
			},
			{
				TagValues:   map[string]interface{}{"service_id": "5", "entity_id": "entity_2"},
				FieldValues: map[string]interface{}{"summation": 100.0, "count": int64(3), "value": 33.333},
			},
		},
	}
}
