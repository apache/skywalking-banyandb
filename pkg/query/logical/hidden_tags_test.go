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

package logical

import (
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestHiddenTagSet(t *testing.T) {
	t.Run("NewHiddenTagSet creates empty set", func(t *testing.T) {
		h := NewHiddenTagSet()
		if !h.IsEmpty() {
			t.Error("expected new HiddenTagSet to be empty")
		}
	})

	t.Run("Add and Contains", func(t *testing.T) {
		h := NewHiddenTagSet()
		h.Add("tag1")
		h.Add("tag2")

		if !h.Contains("tag1") {
			t.Error("expected tag1 to be in set")
		}
		if !h.Contains("tag2") {
			t.Error("expected tag2 to be in set")
		}
		if h.Contains("tag3") {
			t.Error("expected tag3 not to be in set")
		}
		if h.IsEmpty() {
			t.Error("expected set not to be empty after adding tags")
		}
	})

	t.Run("StripHiddenTags removes hidden tags", func(t *testing.T) {
		h := NewHiddenTagSet()
		h.Add("hidden1")
		h.Add("hidden2")

		families := []*modelv1.TagFamily{
			{
				Name: "family1",
				Tags: []*modelv1.Tag{
					{Key: "visible", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "keep"}}}},
					{Key: "hidden1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "drop"}}}},
				},
			},
			{
				Name: "family2",
				Tags: []*modelv1.Tag{
					{Key: "hidden2", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "drop"}}}},
				},
			},
			{
				Name: "family3",
				Tags: []*modelv1.Tag{
					{Key: "also_visible", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "keep"}}}},
				},
			},
		}

		result := h.StripHiddenTags(families)

		// family2 should be removed entirely because all tags are hidden
		if len(result) != 2 {
			t.Fatalf("expected 2 families after stripping, got %d", len(result))
		}

		// Check family1
		if result[0].Name != "family1" {
			t.Errorf("expected first family to be 'family1', got %s", result[0].Name)
		}
		if len(result[0].Tags) != 1 {
			t.Fatalf("expected 1 tag in family1, got %d", len(result[0].Tags))
		}
		if result[0].Tags[0].Key != "visible" {
			t.Errorf("expected tag 'visible' in family1, got %s", result[0].Tags[0].Key)
		}

		// Check family3
		if result[1].Name != "family3" {
			t.Errorf("expected second family to be 'family3', got %s", result[1].Name)
		}
		if len(result[1].Tags) != 1 {
			t.Fatalf("expected 1 tag in family3, got %d", len(result[1].Tags))
		}
		if result[1].Tags[0].Key != "also_visible" {
			t.Errorf("expected tag 'also_visible' in family3, got %s", result[1].Tags[0].Key)
		}
	})

	t.Run("StripHiddenTags with empty hidden set returns unchanged", func(t *testing.T) {
		h := NewHiddenTagSet()
		families := []*modelv1.TagFamily{
			{
				Name: "family1",
				Tags: []*modelv1.Tag{
					{Key: "tag1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "value"}}}},
				},
			},
		}

		result := h.StripHiddenTags(families)

		if len(result) != 1 {
			t.Fatalf("expected 1 family, got %d", len(result))
		}
		if len(result[0].Tags) != 1 {
			t.Fatalf("expected 1 tag, got %d", len(result[0].Tags))
		}
	})

	t.Run("StripHiddenTags with nil families returns nil", func(t *testing.T) {
		h := NewHiddenTagSet()
		h.Add("hidden")

		result := h.StripHiddenTags(nil)

		if result != nil {
			t.Errorf("expected nil result for nil input, got %v", result)
		}
	})

	t.Run("StripHiddenTags handles nil tag families", func(t *testing.T) {
		h := NewHiddenTagSet()
		h.Add("hidden")

		families := []*modelv1.TagFamily{
			nil,
			{
				Name: "family1",
				Tags: []*modelv1.Tag{
					{Key: "visible", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "keep"}}}},
				},
			},
		}

		result := h.StripHiddenTags(families)

		if len(result) != 1 {
			t.Fatalf("expected 1 family after stripping nil, got %d", len(result))
		}
	})

	t.Run("StripHiddenTags handles nil tags", func(t *testing.T) {
		h := NewHiddenTagSet()
		h.Add("hidden")

		families := []*modelv1.TagFamily{
			{
				Name: "family1",
				Tags: []*modelv1.Tag{
					nil,
					{Key: "visible", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "keep"}}}},
				},
			},
		}

		result := h.StripHiddenTags(families)

		if len(result) != 1 {
			t.Fatalf("expected 1 family, got %d", len(result))
		}
		if len(result[0].Tags) != 1 {
			t.Fatalf("expected 1 tag after stripping nil, got %d", len(result[0].Tags))
		}
	})
}
