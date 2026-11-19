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

package measure

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
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

// heldRepository holds the groups in its set and no measure, as a node does that holds a group
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
	return &schemaRepo{Repository: heldRepository{groups: map[string]bool{"sw_metric": true}}}
}

func measureWrite(group string, ts time.Time) *measurev1.InternalWriteRequest {
	return &measurev1.InternalWriteRequest{Request: &measurev1.WriteRequest{
		Metadata:  &commonv1.Metadata{Group: group, Name: "service_cpm_minute"},
		DataPoint: &measurev1.DataPointValue{Timestamp: timestamppb.New(ts)},
	}}
}

// dropBatch has one row for each reason a measure write listener drops a row for before it
// stores anything. A timestamp finer than a millisecond is out of range.
func dropBatch() []any {
	return []any{
		measureWrite("sw_metric", time.Unix(1_800_000_000, 1)),
		measureWrite("any-name-a-writer-sends", time.Unix(1_800_000_000, 1)),
		[]byte{0xff},
		42,
		measureWrite("deleted", time.Unix(1_800_000_000, 0)),
		measureWrite("deleted", time.Unix(1_800_000_001, 0)),
	}
}

var wantDrops = dropCounter{
	"sw_metric|" + common.DropReasonInvalidTimestamp: 1,
	"|" + common.DropReasonInvalidTimestamp:          1,
	"|" + common.DropReasonDecode:                    1,
	"|" + common.DropReasonInvalidType:               1,
	"|" + common.DropReasonGroupNotFound:             2,
}

func TestWriteCallbackCountsTheRowsItDrops(t *testing.T) {
	dropped := dropCounter{}
	w := &writeCallback{
		l:          logger.GetLogger("test-measure-drop"),
		schemaRepo: dropRepo(),
		dropped:    dropped,
	}

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")
	require.Equal(t, wantDrops, dropped)

	recorder := common.NewWriteRecorder(time.Time{})
	w.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(2, dropBatch()))
	require.Len(t, recorder.RowFailures(), 6, "a recorder still gets every dropped row")
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
		l:          logger.GetLogger("test-measure-drop-liaison"),
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
	require.Equal(t, map[string][]string{"banyandb_measure_write_dropped_total": {"group", "reason"}}, r.created,
		"the name is unique across models, which the native meter provider keys a measure by")
	require.Nil(t, newWriteDropCounter(nil), "a service without a registry counts nothing")
}
