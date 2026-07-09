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

package bydbql

import (
	"math"
	"strings"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func tvStr(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func tvInt(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

func tvStrArray(vs ...string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: vs}}}
}

func TestPrepareNumbersPlaceholdersInTextOrder(t *testing.T) {
	ps, err := Prepare("SELECT * FROM STREAM sw IN default TIME > ? WHERE service_id = ? AND codes IN (?) LIMIT ? OFFSET ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if got := len(ps.specs); got != 5 {
		t.Fatalf("placeholder count = %d, want 5", got)
	}
	wantSpecs := []placeholderSpec{
		{kind: phTime},
		{kind: phScalar},
		{kind: phList},
		{kind: phCount, maxCount: math.MaxUint32},
		{kind: phCount, maxCount: math.MaxUint32},
	}
	for i, want := range wantSpecs {
		if ps.specs[i] != want {
			t.Errorf("specs[%d] = %+v, want %+v", i, ps.specs[i], want)
		}
	}
	sel := ps.template.Select
	if idx := sel.Time.Value.ParamIndex; idx != 0 {
		t.Errorf("TIME ParamIndex = %d, want 0", idx)
	}
	if idx := sel.Where.Expr.Left.Left.Binary.Tail.Compare.Value.ParamIndex; idx != 1 {
		t.Errorf("scalar ParamIndex = %d, want 1", idx)
	}
	if idx := sel.Where.Expr.Left.Right[0].Right.In.Values[0].ParamIndex; idx != 2 {
		t.Errorf("IN element ParamIndex = %d, want 2", idx)
	}
	if idx := sel.Limit.ParamIndex; idx != 3 {
		t.Errorf("LIMIT ParamIndex = %d, want 3", idx)
	}
	if idx := sel.Offset.ParamIndex; idx != 4 {
		t.Errorf("OFFSET ParamIndex = %d, want 4", idx)
	}
}

func TestPrepareNumbersShowTopCount(t *testing.T) {
	ps, err := Prepare("SHOW TOP ? FROM MEASURE m IN default TIME > ? WHERE service = ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	wantSpecs := []placeholderSpec{
		{kind: phCount, maxCount: math.MaxInt32},
		{kind: phTime},
		{kind: phScalar},
	}
	if len(ps.specs) != len(wantSpecs) {
		t.Fatalf("specs len = %d, want %d", len(ps.specs), len(wantSpecs))
	}
	for i, want := range wantSpecs {
		if ps.specs[i] != want {
			t.Errorf("specs[%d] = %+v, want %+v", i, ps.specs[i], want)
		}
	}
	if idx := ps.template.TopN.NParamIndex; idx != 0 {
		t.Errorf("SHOW TOP NParamIndex = %d, want 0", idx)
	}
}

func TestPrepareLiteralQueryHasNoPlaceholders(t *testing.T) {
	ps, err := Prepare("SELECT * FROM STREAM sw IN default WHERE service_id = 'webapp'")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if got := len(ps.specs); got != 0 {
		t.Fatalf("placeholder count = %d, want 0", got)
	}
}

func TestPrepareCoversSamePlaceholdersAsBinder(t *testing.T) {
	// Prepare and binder.collect are parallel traversals; a query exercising
	// every placeholder position must yield the same count from both, or one
	// walk has drifted from the other.
	query := "SELECT TOP ? f FROM MEASURE m IN default TIME BETWEEN ? AND ? " +
		"WHERE a = ? AND b IN (?) AND c MATCH(?) AND d HAVING (?) LIMIT ? OFFSET ?"
	ps, err := Prepare(query)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	g, errs := ParseQuery(query)
	if errs != nil {
		t.Fatalf("ParseQuery: %v", errs)
	}
	if got, want := len(ps.specs), countUnboundParams(g); got != want {
		t.Fatalf("Prepare counted %d placeholders, binder counted %d", got, want)
	}
}

func TestBindResolvesEachKind(t *testing.T) {
	ps, err := Prepare("SELECT * FROM STREAM sw IN default TIME > ? WHERE service_id = ? AND codes IN (?) LIMIT ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	bound, err := ps.Bind([]*modelv1.TagValue{tvStr("-30m"), tvStr("webapp"), tvStrArray("a", "b"), tvInt(10)})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	p := bound.values
	if p[0].timeStr != "-30m" {
		t.Errorf("time = %q, want -30m", p[0].timeStr)
	}
	if len(p[1].values) != 1 || *p[1].values[0].String != "webapp" {
		t.Errorf("scalar = %+v, want [webapp]", p[1].values)
	}
	if len(p[2].values) != 2 || *p[2].values[0].String != "a" || *p[2].values[1].String != "b" {
		t.Errorf("list = %+v, want [a b]", p[2].values)
	}
	if p[3].count != 10 {
		t.Errorf("count = %d, want 10", p[3].count)
	}
}

func TestBindTemplateStaysImmutable(t *testing.T) {
	ps, err := Prepare("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err = ps.Bind([]*modelv1.TagValue{tvStr("first")}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	node := ps.template.Select.Where.Expr.Left.Left.Binary.Tail.Compare.Value
	if !node.Param || node.String != nil {
		t.Fatalf("template mutated by Bind: Param=%v String=%v", node.Param, node.String)
	}
	// A second bind with a different value must succeed on the same template.
	bound, err := ps.Bind([]*modelv1.TagValue{tvStr("second")})
	if err != nil {
		t.Fatalf("second Bind: %v", err)
	}
	if *bound.values[0].values[0].String != "second" {
		t.Errorf("second bind value = %q, want second", *bound.values[0].values[0].String)
	}
}

func TestBindErrors(t *testing.T) {
	ps, err := Prepare("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err = ps.Bind(nil); err == nil || !strings.Contains(err.Error(), "count mismatch") {
		t.Errorf("count mismatch: got %v", err)
	}
	if _, err = ps.Bind([]*modelv1.TagValue{tvStrArray("a", "b")}); err == nil || !strings.Contains(err.Error(), "parameter #1") {
		t.Errorf("array in scalar: got %v", err)
	}
	if _, err = ps.Bind([]*modelv1.TagValue{{}}); err == nil || !strings.Contains(err.Error(), "has no value") {
		t.Errorf("nil value: got %v", err)
	}
}

// BenchmarkBinding contrasts re-parsing per request with parse-once/bind-many.
func BenchmarkBinding(b *testing.B) {
	const query = "SELECT * FROM STREAM sw IN default TIME > ? WHERE service_id = ? AND codes IN (?) LIMIT ?"
	params := func() []*modelv1.TagValue {
		return []*modelv1.TagValue{tvStr("-30m"), tvStr("webapp"), tvStrArray("a", "b"), tvInt(10)}
	}
	b.Run("ParseAndBindEachRequest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			g, errs := ParseQuery(query)
			if errs != nil {
				b.Fatal(errs)
			}
			if err := BindParams(g, params()); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("PrepareOnceBindMany", func(b *testing.B) {
		ps, err := Prepare(query)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := ps.Bind(params()); err != nil {
				b.Fatal(err)
			}
		}
	})
}
