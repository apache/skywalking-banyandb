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

package exporter

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// The lines an export writes besides the dry-run report, the resume summary and the export
// summary. A NOTE says why something the operator asked for is not done, a WARN that something
// may not be what the operator expects.

// notef writes one NOTE line.
func notef(w io.Writer, format string, a ...any) {
	fmt.Fprintf(w, "NOTE "+format+"\n", a...)
}

// warnf writes one WARN line.
func warnf(w io.Writer, format string, a ...any) {
	fmt.Fprintf(w, "WARN "+format+"\n", a...)
}

func removingLeftovers(w io.Writer, dir, names string) {
	fmt.Fprintf(w, "removing from %s what an export that stopped before it recorded its session left: %s\n", dir, names)
}

func schemaOnlyWritten(w io.Writer, dir string) {
	fmt.Fprintf(w, "schema-only export written to %s\n", dir)
}

func warnPreempted(w io.Writer, sessions []string) {
	fmt.Fprintf(w, "WARN ⚠ this export preempted the existing export session(s) %v: an export still running on them fails on its next read\n", sessions)
}

// releaseCommand is the command that releases the export session id; "<id>" stands for one that
// is not known.
func releaseCommand(id string) string {
	if id == "" {
		id = "<id>"
	}
	return "bydbctl data release-session --id " + id
}

// giveUpSteps is how to give up an export and begin again: release its session (id, "" when it is
// not known), delete files and export again.
func giveUpSteps(id, files string) string {
	return "release the session with: " + releaseCommand(id) + "; then delete " + files + " and export again"
}

func sessionLostHint(w io.Writer, sessID, files string) {
	fmt.Fprintf(w, "export session %s is lost, so this export cannot be resumed: if it is still held anywhere, %s\n",
		sessID, giveUpSteps(sessID, files))
}

func schemaChangedHint(w io.Writer, done int, sessID, files string) {
	fmt.Fprintf(w, "the schema changed after %d unit(s) were exported with the first one, so this export cannot be resumed: %s\n",
		done, giveUpSteps(sessID, files))
}

// keptHint names the session and the progress file a failed export keeps, and the two ways on.
func keptHint(w io.Writer, sessID, path, outputDir string) {
	fmt.Fprintf(w, "export session %s and the progress file %s are kept\n"+
		"  to resume: run the same command again (add --resume when not at a terminal)\n"+
		"  to give up: %s; a released session cannot be resumed\n",
		sessID, path, giveUpSteps(sessID, giveUpFiles(path, outputDir)))
}

func warnReleaseFailed(w io.Writer, sessID string, err error) {
	warnf(w, "release of export session %s failed (its lease expires on its own): %v", sessID, err)
}

func warnReleaseFailedOn(w io.Writer, sessID, node, reason string) {
	warnf(w, "release of export session %s failed on %s: %s", sessID, node, reason)
}

func releaseHint(w io.Writer, sessID string) {
	fmt.Fprintf(w, "release it with: %s\n", releaseCommand(sessID))
}

func warnProgressNotDeleted(w io.Writer, err error, path, side string) {
	warnf(w, "the export is complete but its progress file could not be deleted: %v; delete %s and %s by hand", err, path, side)
}

func stoppingRelease(w io.Writer, sessID string, wait time.Duration) {
	fmt.Fprintf(w, "stopping: releasing export session %s, waiting up to %s; press Ctrl-C again to stop now\n", sessID, wait)
}

func warnManifest(w io.Writer, warning string) {
	warnf(w, "%s", warning)
}

func warnHeartbeatFailed(w io.Writer, sessID string, err error) {
	warnf(w, "export session %s heartbeat failed, retrying: %v", sessID, err)
}

func nodeRetry(w io.Writer, node, via, code, next string, backoff time.Duration) {
	fmt.Fprintf(w, "node %s is busy or unreachable via %s (%s); retrying via %s in %s\n", node, via, code, next, backoff)
}

// summaryOutputDir continues the resume summary, aligned with its labels.
func summaryOutputDir(w io.Writer, dir string) {
	fmt.Fprintf(w, "         output directory %s (recorded in the progress file)\n", dir)
}

func noteManifestOnly(w io.Writer, sessID string) {
	notef(w, "every unit is done: the export resumes by writing the manifest, without export session %s", sessID)
}

func noteSessionNotContacted(w io.Writer, sessID string) {
	notef(w, "export session %s was not contacted: whether it is still held, its inventory unchanged and the schema "+
		"unchanged is checked when the export resumes", sessID)
}

func planReadOnResume(w io.Writer) {
	fmt.Fprintln(w, "the plan of what is left is read from the export session when the export resumes")
}

func notePreemptIgnored(w io.Writer, sessID string) {
	notef(w, "--preempt (BYDBCTL_PREEMPT) is ignored: a resumed export creates no session, it reads its own session %s again", sessID)
}

func noteResourcesAdded(w io.Writer, added []string) {
	notef(w, "resource(s) added to the schema since the first run are not part of this export: %s", strings.Join(added, ", "))
}

func writingManifestOnly(w io.Writer, sessID string, units int) {
	fmt.Fprintf(w, "every unit of export session %s is done (%d); writing the manifest\n", sessID, units)
}
