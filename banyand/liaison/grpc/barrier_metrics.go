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

package grpc

import (
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

const (
	// barrierLabelRevisionApplied / SchemaApplied / SchemaDeleted are the
	// three values used for the `barrier` label on
	// schema_barrier_laggard_nodes_total. The histogram metrics carry the
	// barrier identity in their metric name instead, so this label is only
	// emitted on the laggard counter.
	barrierLabelRevisionApplied = "revision_applied"
	barrierLabelSchemaApplied   = "schema_applied"
	barrierLabelSchemaDeleted   = "schema_deleted"

	// resultLabel* are the values for the `result` label on the three
	// schema_await_*_duration_seconds histograms. Recorded once per call
	// at return time.
	resultLabelApplied         = "applied"
	resultLabelTimeout         = "timeout"
	resultLabelInvalidArgument = "invalid_argument"
	resultLabelError           = "error"

	// roleLabelSelf is used when a laggard's Node field is unprefixed —
	// the standalone barrier path emits a single self-laggard with no name
	// on timeout, where there is no role to label against.
	roleLabelSelf = "self"

	// statusReasonWaitTimeout is the only `reason` value emitted for
	// schema_status_schema_not_applied_total in v0.11.0. The label is
	// retained for forward-compat with optional fast-sync paths.
	statusReasonWaitTimeout = "wait_timeout"

	// rpcLabel* are the values for the `rpc` label on the two status
	// counters (schema_status_schema_not_applied_total /
	// schema_status_expired_schema_total). Bound at call sites in
	// measure.go / stream.go / trace.go for the write and query gates.
	rpcLabelMeasureWrite = "measure_write"
	rpcLabelStreamWrite  = "stream_write"
	rpcLabelTraceWrite   = "trace_write"
	rpcLabelMeasureQuery = "measure_query"
	rpcLabelStreamQuery  = "stream_query"
	rpcLabelTraceQuery   = "trace_query"
)

// splitRoleNode extracts the `<role>` and `<name>` halves of a laggard's
// Node identifier per the cluster barrier's `<role>-<Metadata.Name>`
// convention (see member.laggardName in barrier_cluster.go). Unprefixed
// values — emitted by the standalone barrier path on timeout — map to
// role="self" and name="" so the laggard counter still increments without
// dropping the observation.
func splitRoleNode(node string) (role, name string) {
	if i := strings.IndexByte(node, '-'); i >= 0 {
		return node[:i], node[i+1:]
	}
	return roleLabelSelf, node
}

// barrierResultLabel maps a barrier RPC's (response.Applied, error) outcome
// to the `result` label value used on the duration histogram. An
// InvalidArgument status reflects the 10 000-key cap rejection; any other
// error is reported as "error" so dashboards can flag transport-layer
// failures separately from cache-applied vs cache-timeout.
func barrierResultLabel(applied bool, err error) string {
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.InvalidArgument {
			return resultLabelInvalidArgument
		}
		return resultLabelError
	}
	if applied {
		return resultLabelApplied
	}
	return resultLabelTimeout
}

// recordBarrierLaggards bumps the schema_barrier_laggard_nodes_total counter
// once per laggard in the response. The role/name split lets dashboards
// answer "which node fell behind on which barrier" without unbounded
// cardinality on a single label.
//
// Metric is silently skipped when counter is nil (test fixtures that
// construct barrierService without metrics) or when laggards is empty.
func recordBarrierLaggards(counter meter.Counter, barrier string, laggards []*schemav1.NodeLaggard) {
	if counter == nil {
		return
	}
	for _, lag := range laggards {
		role, name := splitRoleNode(lag.GetNode())
		counter.Inc(1, barrier, role, name)
	}
}

// recordAwaitRevisionAppliedResult observes the duration histogram, bumps
// the laggard counter, and emits a structured access-log line for one
// AwaitRevisionApplied call. Defensive about nil metrics / nil logger so
// fixtures that construct barrierService directly (without server.PreRun)
// stay zero-cost.
func (b *barrierService) recordAwaitRevisionAppliedResult(
	start time.Time,
	req *schemav1.AwaitRevisionAppliedRequest,
	resp *schemav1.AwaitRevisionAppliedResponse,
	err error,
) {
	duration := time.Since(start)
	result := barrierResultLabel(resp.GetApplied(), err)
	if b.metrics != nil {
		if h := b.metrics.schemaAwaitRevisionAppliedDuration; h != nil {
			h.Observe(duration.Seconds(), result)
		}
		recordBarrierLaggards(b.metrics.schemaBarrierLaggards, barrierLabelRevisionApplied, resp.GetLaggards())
	}
	if b.l != nil {
		b.l.Info().
			Str("barrier", barrierLabelRevisionApplied).
			Str("result", result).
			Int64("min_revision", req.GetMinRevision()).
			Int("laggards", len(resp.GetLaggards())).
			Dur("duration", duration).
			Msg("schema barrier completed")
	}
}

// recordAwaitSchemaAppliedResult is the AwaitSchemaApplied counterpart of
// recordAwaitRevisionAppliedResult. The access-log carries the request's
// key count instead of min_revision because per-key revisions are too
// chatty to log at INFO level.
func (b *barrierService) recordAwaitSchemaAppliedResult(
	start time.Time,
	req *schemav1.AwaitSchemaAppliedRequest,
	resp *schemav1.AwaitSchemaAppliedResponse,
	err error,
) {
	duration := time.Since(start)
	result := barrierResultLabel(resp.GetApplied(), err)
	if b.metrics != nil {
		if h := b.metrics.schemaAwaitSchemaAppliedDuration; h != nil {
			h.Observe(duration.Seconds(), result)
		}
		recordBarrierLaggards(b.metrics.schemaBarrierLaggards, barrierLabelSchemaApplied, resp.GetLaggards())
	}
	if b.l != nil {
		b.l.Info().
			Str("barrier", barrierLabelSchemaApplied).
			Str("result", result).
			Int("keys", len(req.GetKeys())).
			Int("laggards", len(resp.GetLaggards())).
			Dur("duration", duration).
			Msg("schema barrier completed")
	}
}

// emitStatusExpired bumps schema_status_expired_schema_total once for an
// (rpc, group) pair. Skipped when m or the counter is nil so legacy test
// fixtures stay zero-cost.
func (m *metrics) emitStatusExpired(rpc, group string) {
	if m == nil || m.schemaStatusExpired == nil {
		return
	}
	m.schemaStatusExpired.Inc(1, rpc, group)
}

// emitStatusNotAppliedTimeout bumps schema_status_schema_not_applied_total
// with reason="wait_timeout" — the only reason emitted in v0.11.0. The
// reason label is retained on the metric so dashboards built against
// v0.11.0 do not break if fast-sync paths land in a later release.
func (m *metrics) emitStatusNotAppliedTimeout(rpc, group string) {
	if m == nil || m.schemaStatusNotApplied == nil {
		return
	}
	m.schemaStatusNotApplied.Inc(1, rpc, group, statusReasonWaitTimeout)
}

// recordQueryGateStatuses iterates the per-group gate verdicts produced by
// checkQueryGate and bumps the matching status counters. STATUS_SUCCEED is
// a no-op; STATUS_EXPIRED_SCHEMA bumps schema_status_expired_schema_total;
// STATUS_SCHEMA_NOT_APPLIED bumps schema_status_schema_not_applied_total
// with reason="wait_timeout". Other statuses (STATUS_NOT_FOUND) carry no
// counter — they are surfaced through the gate's normal response shape.
func recordQueryGateStatuses(m *metrics, rpc string, statuses map[string]modelv1.Status) {
	if m == nil {
		return
	}
	for group, st := range statuses {
		switch st {
		case modelv1.Status_STATUS_EXPIRED_SCHEMA:
			m.emitStatusExpired(rpc, group)
		case modelv1.Status_STATUS_SCHEMA_NOT_APPLIED:
			m.emitStatusNotAppliedTimeout(rpc, group)
		default:
			// STATUS_SUCCEED (no-op) and STATUS_NOT_FOUND / other non-gate
			// statuses are surfaced through the response shape and carry
			// no counter in v0.11.0.
		}
	}
}

// recordAwaitSchemaDeletedResult mirrors recordAwaitSchemaAppliedResult for
// the deletion barrier; the only differences are the metric routing and
// the access-log barrier label.
func (b *barrierService) recordAwaitSchemaDeletedResult(
	start time.Time,
	req *schemav1.AwaitSchemaDeletedRequest,
	resp *schemav1.AwaitSchemaDeletedResponse,
	err error,
) {
	duration := time.Since(start)
	result := barrierResultLabel(resp.GetApplied(), err)
	if b.metrics != nil {
		if h := b.metrics.schemaAwaitSchemaDeletedDuration; h != nil {
			h.Observe(duration.Seconds(), result)
		}
		recordBarrierLaggards(b.metrics.schemaBarrierLaggards, barrierLabelSchemaDeleted, resp.GetLaggards())
	}
	if b.l != nil {
		b.l.Info().
			Str("barrier", barrierLabelSchemaDeleted).
			Str("result", result).
			Int("keys", len(req.GetKeys())).
			Int("laggards", len(resp.GetLaggards())).
			Dur("duration", duration).
			Msg("schema barrier completed")
	}
}
