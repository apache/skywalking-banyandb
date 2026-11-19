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

package trace

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

// dropCounter records what a write listener counts as dropped, keyed by "group|reason".
type dropCounter map[string]float64

func (c dropCounter) Inc(delta float64, labelValues ...string) {
	c[strings.Join(labelValues, "|")] += delta
}

func (c dropCounter) Delete(_ ...string) bool { return true }

// heldRepository holds the groups in its set and no trace, as a node does that holds a group
// whose schema has not reached it yet.
type heldRepository struct {
	resourceSchema.Repository
	groups map[string]bool
}

func (r heldRepository) LoadGroup(name string) (resourceSchema.Group, bool) {
	return nil, r.groups[name]
}

func (heldRepository) LoadResource(*commonv1.Metadata) (resourceSchema.Resource, bool) {
	return nil, false
}

func dropRepo() *schemaRepo {
	return &schemaRepo{Repository: heldRepository{groups: map[string]bool{"sw_trace": true}}}
}

// dropBatch has one row for each reason a trace write listener drops a row for before it
// stores anything. A trace looks its schema up first, so an unknown trace is the drop a
// missing schema causes.
func dropBatch() []any {
	unknown := &tracev1.InternalWriteRequest{Request: &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: "sw_trace", Name: "segment"},
	}}
	arbitrary := &tracev1.InternalWriteRequest{Request: &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: "any-name-a-writer-sends", Name: "segment"},
	}}
	return []any{[]byte{0xff}, 42, unknown, unknown, arbitrary}
}

var wantDrops = dropCounter{
	"|" + common.DropReasonDecode:                 1,
	"|" + common.DropReasonInvalidType:            1,
	"sw_trace|" + common.DropReasonSchemaNotFound: 2,
	"|" + common.DropReasonSchemaNotFound:         1,
}

func TestWriteCallbackCountsTheRowsItDrops(t *testing.T) {
	dropped := dropCounter{}
	w := &writeCallback{
		l:          logger.GetLogger("test-trace-drop"),
		schemaRepo: dropRepo(),
		dropped:    dropped,
	}

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")
	require.Equal(t, wantDrops, dropped)

	recorder := common.NewWriteRecorder(time.Time{})
	w.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(2, dropBatch()))
	require.Len(t, recorder.RowFailures(), 5, "a recorder still gets every dropped row")
	require.Equal(t, wantDrops, dropped, "a write with a recorder reports its failures to its caller, so it counts nothing")

	dropped = dropCounter{}
	w.dropped = dropped
	w.Rev(context.Background(), bus.NewMessage(2, "not a batch"))
	w.Rev(context.Background(), bus.NewMessage(3, []any{}))
	require.Equal(t, dropCounter{"|" + common.DropReasonInvalidType: 1}, dropped,
		"a message that is not a batch is one drop; an empty batch drops nothing")
}

func TestWriteQueueCallbackCountsTheRowsItDrops(t *testing.T) {
	dropped := dropCounter{}
	w := &writeQueueCallback{
		l:          logger.GetLogger("test-trace-drop-liaison"),
		schemaRepo: dropRepo(),
		dropped:    dropped,
	}

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")
	require.Equal(t, wantDrops, dropped)
}

// namingRegistry records the full name and labels of each counter created through it.
type namingRegistry struct {
	observability.MetricsRegistry
	created map[string][]string
}

func (r *namingRegistry) With(scope meter.Scope) observability.Factory {
	return &namingFactory{Factory: nil, r: r, ns: scope.GetNamespace()}
}

type namingFactory struct {
	observability.Factory
	r  *namingRegistry
	ns string
}

func (f *namingFactory) NewCounter(name string, labelNames ...string) meter.Counter {
	f.r.created[f.ns+"_"+name] = labelNames
	return dropCounter{}
}

func TestWriteDropCounterName(t *testing.T) {
	r := &namingRegistry{created: map[string][]string{}}
	require.NotNil(t, newWriteDropCounter(r))
	require.Equal(t, map[string][]string{"banyandb_trace_write_dropped_total": {"group", "reason"}}, r.created,
		"the name is unique across models, which the native meter provider keys a measure by")
	require.Nil(t, newWriteDropCounter(nil), "a service without a registry counts nothing")
}
