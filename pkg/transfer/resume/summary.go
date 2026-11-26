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

package resume

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// Summary is what the resume summary says about a progress file: what is done, per catalog,
// group and stage, and how much is left in total.
type Summary struct {
	UpdatedAt time.Time
	Now       time.Time
	// Path is the progress file as the operator gave it.
	Path string
	// Session is the export session id, "" for none; Command names the command ("export").
	Session string
	Command string
	// Noun counts the work: "unit(s)" for an export, "work item(s)" for an import.
	Noun string
	// LeftUnknown, when set, says why what is left is not known yet; the LEFT line is this text.
	LeftUnknown string
	// Finished, when set, is when the work the progress file records was finished: the title
	// is FINISHED and the LEFT line and Extra are left out.
	Finished *time.Time
	// rowOf indexes Rows by catalog, group and stage for Add.
	rowOf map[[3]string]int
	// Extra lines are printed after LEFT, e.g. the import's AMBIGUOUS section.
	Extra                []string
	Rows                 []Row
	Done, Total          int
	DoneFiles            int
	DoneBytes, LeftBytes uint64
	// LeftShards, when more than 0, is how many shards the work left spans.
	LeftShards int
	// LeftRows, when more than 0, is how many rows the work left holds.
	LeftRows uint64
}

// Add counts one unit of work of the catalog, group and stage into the totals and the row of the
// table: done, with its doneFiles and doneBytes, or left, with leftBytes. The summary's DoneFiles
// and DoneBytes are the caller's.
func (s *Summary) Add(catalog, group, stage string, done bool, doneFiles int, doneBytes, leftBytes uint64) {
	k := [3]string{catalog, group, stage}
	i, ok := s.rowOf[k]
	if !ok {
		if s.rowOf == nil {
			s.rowOf = map[[3]string]int{}
		}
		i = len(s.Rows)
		s.rowOf[k] = i
		s.Rows = append(s.Rows, Row{Catalog: catalog, Group: group, Stage: stage})
	}
	r := &s.Rows[i]
	r.Total++
	s.Total++
	if done {
		r.Done++
		s.Done++
		r.DoneFiles += doneFiles
		r.DoneBytes += doneBytes
		return
	}
	s.LeftBytes += leftBytes
}

// Row is one catalog, group and stage of the summary table.
type Row struct {
	Catalog, Group, Stage string
	Done, Total           int
	DoneFiles             int
	DoneBytes             uint64
}

// Render writes the summary:
//
//	RESUME   <path>  (last updated <time>, <age> ago)
//	         session <id> (<command>)
//	DONE     <g> group(s), <n> unit(s), <files> file(s), <size>
//	CATALOG  GROUP  STAGE  DONE  FILES  SIZE
//
//	LEFT     <g> group(s), <n> unit(s), <k> shard(s), <r> rows, est. <size>
//
// The shards and the rows are left out when not counted. The table lists what is done only; the caller prints what is left. With LeftUnknown the DONE
// line has no groups, the LEFT line is LeftUnknown and no table follows. With Finished the first
// line is
//
//	FINISHED <path>  (finished at <time>, <age> ago)
//
// and neither the LEFT line nor Extra follows the table.
func (s *Summary) Render(w io.Writer) error {
	if s.Finished != nil {
		fmt.Fprintf(w, "FINISHED %s  (finished at %s, %s ago)\n", s.Path, s.Finished.Local().Format(time.RFC3339), age(s.Now.Sub(*s.Finished)))
	} else {
		fmt.Fprintf(w, "RESUME   %s  (last updated %s, %s ago)\n", s.Path, s.UpdatedAt.Local().Format(time.RFC3339), age(s.Now.Sub(s.UpdatedAt)))
	}
	if s.Session != "" {
		fmt.Fprintf(w, "         session %s (%s)\n", s.Session, s.Command)
	}
	if s.LeftUnknown != "" {
		fmt.Fprintf(w, "DONE     %d %s, %d file(s), %s\n", s.Done, s.Noun, s.DoneFiles, output.HumanBytes(s.DoneBytes))
		fmt.Fprintf(w, "LEFT     %s\n", s.LeftUnknown)
		for _, l := range s.Extra {
			fmt.Fprintln(w, l)
		}
		return nil
	}
	fmt.Fprintf(w, "DONE     %d group(s), %d %s, %d file(s), %s\n", s.groups(func(r Row) bool { return r.Done > 0 }), s.Done, s.Noun,
		s.DoneFiles, output.HumanBytes(s.DoneBytes))
	if err := s.renderDone(w); err != nil {
		return err
	}
	if s.Finished != nil {
		return nil
	}
	left := fmt.Sprintf("LEFT     %d group(s), %d %s", s.groups(func(r Row) bool { return r.Total > r.Done }), s.Total-s.Done, s.Noun)
	if s.LeftShards > 0 {
		left += fmt.Sprintf(", %d shard(s)", s.LeftShards)
	}
	if s.LeftRows > 0 {
		left += fmt.Sprintf(", %s rows", output.HumanCount(s.LeftRows))
	}
	fmt.Fprintf(w, "\n%s, est. %s\n", left, output.HumanBytes(s.LeftBytes))
	for _, l := range s.Extra {
		fmt.Fprintln(w, l)
	}
	return nil
}

// column is the header of the count column of the done table: the last word of Noun, as
// "unit(s)" gives UNITS and "work item(s)" gives ITEMS.
func (s *Summary) column() string {
	words := strings.Fields(s.Noun)
	if len(words) == 0 {
		return "DONE"
	}
	return strings.ToUpper(strings.TrimSuffix(words[len(words)-1], "(s)") + "S")
}

// groups counts the distinct catalog and group of the rows keep accepts.
func (s *Summary) groups(keep func(Row) bool) int {
	seen := map[[2]string]struct{}{}
	for _, r := range s.Rows {
		if keep(r) {
			seen[[2]string{r.Catalog, r.Group}] = struct{}{}
		}
	}
	return len(seen)
}

// renderDone writes the table of what is done, if anything is.
func (s *Summary) renderDone(w io.Writer) error {
	rows := slices.DeleteFunc(slices.Clone(s.Rows), func(r Row) bool { return r.Done == 0 })
	if len(rows) == 0 {
		return nil
	}
	slices.SortFunc(rows, func(a, b Row) int {
		return cmp.Or(cmp.Compare(a.Catalog, b.Catalog), cmp.Compare(a.Group, b.Group), cmp.Compare(a.Stage, b.Stage))
	})
	// The table goes out in one Write: a terminal's progress writer starts every Write with a
	// carriage return, which would overwrite the cells a tabwriter writes one by one.
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "CATALOG\tGROUP\tSTAGE\t%s\tFILES\tSIZE\n", s.column())
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\n", r.Catalog, r.Group, r.Stage, r.Done, r.DoneFiles, output.HumanBytes(r.DoneBytes))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

// age spells how long ago something happened: 12s, 41m, 3h5m or 2d4h.
func age(d time.Duration) string {
	d = max(d, 0)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}
