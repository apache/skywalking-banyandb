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
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// CumulativeTTL is the retention of a group's stage the way a data node of that stage resolves
// it (pub.ResolveStageResourceOpts): the group ttl plus the ttl of every lifecycle stage up to
// and including the named one, matched case-insensitively. The default stage ("" or "hot"
// without a stage of that name) has the group ttl only. A stage the group lacks is an error.
func CumulativeTTL(ro *commonv1.ResourceOpts, stage string) (time.Duration, error) {
	if ro.GetTtl() == nil {
		return 0, errors.New("the group has no ttl")
	}
	num := ro.GetTtl().GetNum()
	if idx := stageIndex(ro, stage); idx >= 0 {
		for _, st := range ro.GetStages()[:idx+1] {
			num += st.GetTtl().GetNum()
		}
	} else if normalizeStage(stage) != transfer.DefaultStageName {
		return 0, fmt.Errorf("the group has no stage %q", stage)
	}
	return intervalDuration(ro.GetTtl().GetUnit(), num)
}

// stageIndex returns the index of the lifecycle stage named stage, case-insensitively, or -1.
func stageIndex(ro *commonv1.ResourceOpts, stage string) int {
	for i, st := range ro.GetStages() {
		if strings.EqualFold(st.GetName(), stage) {
			return i
		}
	}
	return -1
}

func intervalDuration(unit commonv1.IntervalRule_Unit, num uint32) (time.Duration, error) {
	switch unit {
	case commonv1.IntervalRule_UNIT_DAY:
		return time.Duration(num) * 24 * time.Hour, nil
	case commonv1.IntervalRule_UNIT_HOUR:
		return time.Duration(num) * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported interval unit %s", unit)
	}
}

// ttlBreakdown renders where an effective ttl comes from, e.g. "group 1d + warm 7d".
func ttlBreakdown(ro *commonv1.ResourceOpts, stage string) string {
	parts := []string{"group " + cmp.Or(output.IntervalLabel(ro.GetTtl()), "-")}
	if idx := stageIndex(ro, stage); idx >= 0 {
		for _, st := range ro.GetStages()[:idx+1] {
			parts = append(parts, st.GetName()+" "+cmp.Or(output.IntervalLabel(st.GetTtl()), "-"))
		}
	}
	return strings.Join(parts, " + ")
}

// formatDuration renders a ttl as days when it is a whole number of days, else as hours.
func formatDuration(d time.Duration) string {
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	}
	return fmt.Sprintf("%dh", d/time.Hour)
}

// Segment suffix layouts, as the storage layer names segment directories
// (banyand/internal/storage/storage.go hourFormat and dayFormat).
const (
	segmentHourLayout = "2006010215"
	segmentDayLayout  = "20060102"
)

// segmentBounds returns the start and the exclusive end of a source segment: its suffix parsed
// in the source node's time zone, plus the source stage's segment interval ("1d", "3h"), the
// way the storage layer opens and advances segments. An empty time zone is UTC.
func segmentBounds(suffix, timezone, segmentInterval string) (start, end time.Time, err error) {
	loc := time.UTC
	if timezone != "" {
		if loc, err = time.LoadLocation(timezone); err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("time zone %q: %w", timezone, err)
		}
	}
	num, unit, err := parseIntervalLabel(segmentInterval)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	layout := segmentDayLayout
	if unit == commonv1.IntervalRule_UNIT_HOUR {
		layout = segmentHourLayout
	}
	if start, err = time.ParseInLocation(layout, suffix, loc); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("segment suffix %q: %w", suffix, err)
	}
	if unit == commonv1.IntervalRule_UNIT_HOUR {
		return start, start.Add(time.Duration(num) * time.Hour), nil
	}
	return start, start.AddDate(0, 0, num), nil
}

// parseIntervalLabel parses the manifest's interval labels "<n>d" and "<n>h".
func parseIntervalLabel(label string) (int, commonv1.IntervalRule_Unit, error) {
	if len(label) < 2 {
		return 0, 0, fmt.Errorf("segment interval %q is not <n>d or <n>h", label)
	}
	num, err := strconv.Atoi(label[:len(label)-1])
	if err != nil || num <= 0 {
		return 0, 0, fmt.Errorf("segment interval %q is not <n>d or <n>h", label)
	}
	switch label[len(label)-1] {
	case 'd':
		return num, commonv1.IntervalRule_UNIT_DAY, nil
	case 'h':
		return num, commonv1.IntervalRule_UNIT_HOUR, nil
	default:
		return 0, 0, fmt.Errorf("segment interval %q is not <n>d or <n>h", label)
	}
}

// sourceStage returns the manifest stage of a unit's group, matched case-insensitively. The
// group default comes first as "hot", so the last match lets a lifecycle stage named hot
// take precedence over it, as the routing does.
func sourceStage(m *manifest.Manifest, group, stage string) (manifest.Stage, bool) {
	for _, g := range m.Groups {
		if g.Name != group {
			continue
		}
		for _, st := range slices.Backward(g.Stages) {
			if strings.EqualFold(st.Name, stage) {
				return st, true
			}
		}
	}
	return manifest.Stage{}, false
}

// ttlDetail is the GATE detail line of an expired work item. It carries no value derived from
// the current time, so a dry run and the real run print the same line.
func ttlDetail(item *WorkItem, targetStage string, newest int64, ttl time.Duration, breakdown string, forced bool) string {
	verdict := "skipped; --force-ttl imports it"
	if forced {
		verdict = "imported anyway because of --force-ttl"
	}
	return fmt.Sprintf("%s/%s/%s %s: every row is older than the target retention (newest %s, retention %s = %s); %s",
		item.CatalogName(), item.Group, targetStage, item.position(),
		time.Unix(0, newest).UTC().Format(time.RFC3339), formatDuration(ttl), breakdown, verdict)
}
