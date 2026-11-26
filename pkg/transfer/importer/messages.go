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

package importer

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// The lines an import writes to its log besides the plan, the progress and the reports. A NOTE
// says why something the operator asked for is not done, a WARN that something may not be what
// the operator expects; the other lines start with a lower-case label of what they are about.

// notef writes one NOTE line.
func notef(w io.Writer, format string, a ...any) {
	fmt.Fprintf(w, "NOTE "+format+"\n", a...)
}

// warnf writes one WARN line.
func warnf(w io.Writer, format string, a ...any) {
	fmt.Fprintf(w, "WARN "+format+"\n", a...)
}

func infoSchemaFromArtifact(w io.Writer, files int, input string) {
	fmt.Fprintf(w, "info: the schema is read from the %d schema file(s) of artifact %s\n", files, input)
}

func infoNativeWithoutSchema(w io.Writer, input string) {
	fmt.Fprintf(w, "info: artifact %s carries no schema; its native parts are imported into the target's schema\n", input)
}

func infoCSVWithoutSchema(w io.Writer, input string) {
	fmt.Fprintf(w, "info: artifact %s carries no schema; its CSV columns are read with the target's schema\n", input)
}

func infoLine(w io.Writer, line string) {
	fmt.Fprintf(w, "info: %s\n", line)
}

func finishedWithAcceptedItems(w io.Writer, finishedAt time.Time, items int) {
	fmt.Fprintf(w, "the import was finished at %s; the %d work item(s) a gate refused then and accepts now are imported\n",
		finishedAt.Local().Format(time.RFC3339), items)
}

func resumeWithoutAsking(w io.Writer, path, source string) {
	fmt.Fprintf(w, "resume: %s is continued without asking (%s)\n", path, source)
}

func noteSchemaOnlyNoProgress(w io.Writer, path string) {
	notef(w, "a schema-only import uses no progress file: --resume and --restart are ignored, %s is left as it is", path)
}

func noteDryRunRestart(w io.Writer, path string) {
	notef(w, "--restart: the plan below is that of a fresh import, %s is not read; the real run keeps it as %s.<timestamp>.bak", path, path)
}

func noteResumeEnvWithoutProgress(w io.Writer, path string) {
	notef(w, "BYDBCTL_IMPORT_RESUME is set, but there is no progress file at %s: a fresh import starts", path)
}

func warnRestart(w io.Writer, path, bak, consequence string) {
	warnf(w, "--restart: %s is kept as %s and the import starts over; %s", path, bak, consequence)
}

func warnError(w io.Writer, err error) {
	warnf(w, "%v", err)
}

func progressFile(w io.Writer, path string) {
	fmt.Fprintf(w, "progress: %s (kept after the import; the same command resumes an interrupted one)\n", path)
}

func schemaOnlyDone(w io.Writer, writes int) {
	fmt.Fprintf(w, "schema-only import done: %d resource(s) written, no data sent\n", writes)
}

func planItemsAdded(w io.Writer, added int) {
	fmt.Fprintf(w, "plan: %d work item(s) a gate refused when the plan was recorded are added to it\n", added)
}

func baselineGroupsAdded(w io.Writer, groups []string) {
	fmt.Fprintf(w, "baseline: %d group(s) the appended items add were counted before the push and recorded with the baseline: %s\n",
		len(groups), strings.Join(groups, ", "))
}

func baselineNodesAdded(w io.Writer, nodes []string) {
	fmt.Fprintf(w, "baseline: target node(s) %s, which the baseline did not count, were counted before the push and recorded with it\n",
		strings.Join(nodes, ", "))
}

func warnRecheck(w io.Writer, warning string) {
	warnf(w, "(re-check after the schema write) %s", warning)
}

func alreadyImported(w io.Writer, finishedAt time.Time, targets string) {
	fmt.Fprintf(w, "already imported into this cluster at %s (%s); nothing is pushed (--restart imports it again)\n",
		finishedAt.Local().Format(time.RFC3339), targets)
}

func noteVerifySkipped(w io.Writer, baseline string) {
	notef(w, "verify skipped: %s, and whatever was written into these groups since would count as a mismatch; "+
		"give --verify explicitly to check against it anyway", baseline)
}

func verifyPending(w io.Writer) {
	fmt.Fprintln(w, "verify: on, since no verify of this import has passed yet")
}

func verifyAgainstBaseline(w io.Writer, baseline string) {
	fmt.Fprintf(w, "verify: against the baseline the first run recorded (%s); rows written into these groups since by anything else count as mismatches\n", baseline)
}

func verifyForcedBySkip(w io.Writer) {
	fmt.Fprintln(w, "verify: on, since resumeDuplicationPolicy skip left payloads out that no verify has checked yet")
}

func verifyPlanFailed(w io.Writer, err error) {
	fmt.Fprintf(w, "verify: Plan failed: %v\n", err)
}

func verifyWaiting(w io.Writer, elapsed, timeout time.Duration, waiting string, next time.Duration) {
	fmt.Fprintf(w, "verify: waiting for the target to flush (%s elapsed, timeout %s): %s; next poll in %s\n", elapsed, timeout, waiting, next)
}

func warnNoBaselineForVerify(w io.Writer) {
	warnf(w, "the first run of this import recorded no baseline, so the verify after the push is not possible "+
		"(exit 4); --restart imports it again with a new baseline")
}

func warnBaselineNotNeeded(w io.Writer, err error) {
	warnf(w, "%v; neither --verify nor a reconciliation needs it, so the import goes on, "+
		"and a verify forced later has no baseline to compare against", err)
}

func warnNodeLeftUncounted(w io.Writer, node string) {
	warnf(w, "target node %s was not counted by the baseline, and this import already pushed to it, "+
		"so a count now would hold its rows: it is left uncounted", node)
}

func parallelismNative(w io.Writer, workers int, reason string, chunkKiB uint64) {
	fmt.Fprintf(w, "parallelism: %d (%s); one payload of up to %d KiB frames in flight per worker\n", workers, reason, chunkKiB)
}

func parallelismCSV(w io.Writer, workers int, reason string, window int, mib float64) {
	fmt.Fprintf(w, "parallelism: %d (%s); up to %d batch(es) in flight per stream, so at most %.1f MiB of rows per worker\n", workers, reason, window, mib)
}

func rateLimitsNative(w io.Writer, part uint64) {
	fmt.Fprintf(w, "rate limits: native parts %d bytes/s across the import\n", part)
}

func warnRowLimitsOnNative(w io.Writer) {
	warnf(w, "--node-rate-limit, --node-row-rate-limit, --liaison-rate-limit and --write-rate-limit "+
		"apply to CSV rows only; the native parts are limited by --part-rate-limit alone")
}

func warnPartLimitOnCSV(w io.Writer) {
	warnf(w, "--part-rate-limit applies to native parts only; this CSV import ignores it")
}

func rateLimitsCSV(w io.Writer, nodeBytes, nodeRows, liaison, client uint64) {
	fmt.Fprintf(w, "rate limits: node %d bytes/s, %d rows/s per target node; "+
		"liaison %d bytes/s per liaison; client %d rows/s (0 = unlimited)\n",
		nodeBytes, nodeRows, liaison, client)
}

func schemaWritten(w io.Writer, writes int) {
	fmt.Fprintf(w, "schema: %d resource(s) written and applied on every node\n", writes)
}

func warnForceNotNeeded(w io.Writer, groups []string) {
	warnf(w, "--force was not needed: every group to overwrite (%v) is empty", groups)
}

func notePresence(w io.Writer, line string) {
	notef(w, "%s", line)
}

// reconcileLine is the line that says what the reconciliation of a part on a node decided; the
// caller appends the reason of an undecidable outcome.
func reconcileLine(item string, partID uint64, source, node string, d reconcileDecision, rows uint64) string {
	return fmt.Sprintf("reconcile %s part %d of %s on %s: %s (%s, part %d rows)", item, partID, source, node, d.Outcome, d, rows)
}

func duplicationBlocked(w io.Writer, blocked []string) {
	fmt.Fprintf(w, "resumeDuplicationPolicy block: %d payload(s) may have landed already, nothing is pushed:\n", len(blocked))
	for _, b := range blocked {
		fmt.Fprintf(w, "  %s\n", b)
	}
}

func warnDuplicationPushed(w io.Writer, payload, source string, rows uint64) {
	warnf(w, "%s may have landed already; it is pushed again (resumeDuplicationPolicy warn, %s) and may store up to %d row(s) twice", payload, source, rows)
}

func warnDuplicationSkipped(w io.Writer, payload, source string) {
	warnf(w, "%s may have landed already; it is left out (resumeDuplicationPolicy skip, %s) and --verify checks it", payload, source)
}

func warnOrphanFiles(w io.Writer, item string, files int, prefix string) {
	warnf(w, "%s: %d file(s) of incomplete parts in %s are skipped, a receiver cannot load them", item, files, prefix)
}

func warnFewerCopies(w io.Writer, item string, copies uint32, kind fmt.Stringer, targets int) {
	warnf(w, "%s: the target stage asks for %d copies, the liaison routed %s to %d node(s)", item, copies, kind, targets)
}

func warnPayloadRetry(w io.Writer, item string, kind fmt.Stringer, failures string, backoff time.Duration) {
	warnf(w, "%s: %s failed on %s; pushing it again to them in %s", item, kind, failures, backoff)
}

func warnUnpredictedNode(w io.Writer, item string, kind fmt.Stringer, node, planned string) {
	warnf(w, "%s: the liaison routed %s to node %s, which the plan did not predict (%s)", item, kind, node, planned)
}

func warnLiaisonRefused(w io.Writer, liaison string, kind fmt.Stringer, err error, backoff time.Duration) {
	warnf(w, "liaison %s refused %s (%v); retrying in %s", liaison, kind, err, backoff)
}

func waitingForPayload(w io.Writer, drain time.Duration, kind fmt.Stringer, group, stage string, shard uint32, interrupted bool) {
	fmt.Fprintf(w, "waiting up to %s for the answer of the %s %s/%s shard %d in flight, so that it is recorded; %s\n",
		drain, kind, group, stage, shard, stopNowHint(interrupted))
}

func waitingForBatches(w io.Writer, drain time.Duration, batches int, interrupted bool) {
	fmt.Fprintf(w, "waiting up to %s for the acknowledgements of %d batch(es) in flight, so that they are recorded; %s\n",
		drain, batches, stopNowHint(interrupted))
}

// stopNowHint tells how to cut a drain short: after a Ctrl-C the next one stops at once; a drain a
// failure started takes two, the first being the stop the run is already doing.
func stopNowHint(interrupted bool) string {
	if interrupted {
		return "press Ctrl-C again to stop now"
	}
	return "press Ctrl-C twice to stop now"
}

func warnOversizedRow(w io.Writer, item, file string, record uint64, size, maxBytes int) {
	warnf(w, "%s: file %s record %d is %d bytes, more than --write-batch-bytes %d; it is sent in a batch of its own", item, file, record, size, maxBytes)
}

func streamRetry(w io.Writer, key streamKey, via string, code fmt.Stringer, msg, next string, backoff time.Duration) {
	fmt.Fprintf(w, "import stream %s/%s/%s refused via %s (%s: %s); retrying via %s in %s\n",
		key.catalog, key.group, stageLabel(key.stage), via, code, msg, next, backoff)
}
