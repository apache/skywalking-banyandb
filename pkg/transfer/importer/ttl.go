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
	"sync"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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
		if loc = loadLocation(timezone); loc == nil {
			return time.Time{}, time.Time{}, fmt.Errorf("time zone %q does not load", timezone)
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
	if len(label) >= 2 {
		if num, err := strconv.Atoi(label[:len(label)-1]); err == nil && num > 0 {
			switch label[len(label)-1] {
			case 'd':
				return num, commonv1.IntervalRule_UNIT_DAY, nil
			case 'h':
				return num, commonv1.IntervalRule_UNIT_HOUR, nil
			}
		}
	}
	return 0, 0, fmt.Errorf("segment interval %q is not <n>d or <n>h", label)
}

// sourceStage returns the manifest stage of a unit's group, matched case-insensitively. The
// group default comes first as "hot", so the last match returns a lifecycle stage named hot
// over it; the routing lays out hot with the group's own opts, so a hot unit's layout is read
// with sourceGroupDefault.
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

// sourceGroupDefault returns the manifest entry of a group's own opts: the exporter always
// writes it first in the group's stages, before the lifecycle stages, one of which may also be
// named hot.
func sourceGroupDefault(m *manifest.Manifest, group string) (manifest.Stage, bool) {
	for _, g := range m.Groups {
		if g.Name == group && len(g.Stages) > 0 {
			return g.Stages[0], true
		}
	}
	return manifest.Stage{}, false
}

// ttlDetail is the GATE detail line of an expired work item. It carries no value derived from
// the current time, so a dry run and the real run print the same line.
func ttlDetail(item *WorkItem, targetStage string, newest int64, end time.Time, ttl time.Duration, breakdown string, forced bool) string {
	verdict := "skipped; --force-ttl imports it"
	if forced {
		verdict = "imported anyway because of --force-ttl"
	}
	return fmt.Sprintf("%s/%s/%s %s: every row is in a target segment past the retention (newest %s, its segment ends %s, retention %s = %s); %s",
		item.CatalogName(), item.Group, targetStage, item.position(),
		time.Unix(0, newest).UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), formatDuration(ttl), breakdown, verdict)
}

// stageSegmentInterval is the segment interval of a group's stage: the named lifecycle stage's,
// or the group's own for the default stage and a stage the group does not define.
func stageSegmentInterval(ro *commonv1.ResourceOpts, stage string) *commonv1.IntervalRule {
	if i := stageIndex(ro, stage); i >= 0 && ro.GetStages()[i].GetSegmentInterval() != nil {
		return ro.GetStages()[i].GetSegmentInterval()
	}
	return ro.GetSegmentInterval()
}

// itemLocation is the time zone segments of an item are cut in: its first source node's, else UTC.
func itemLocation(item *WorkItem) *time.Location {
	if loc := firstLocation(item.Sources, func(s WorkSource) string { return s.Timezone }); loc != nil {
		return loc
	}
	return time.UTC
}

// firstLocation is the location of the first of items whose zone name is set and loads, else nil.
func firstLocation[T any](items []T, zone func(T) string) *time.Location {
	for _, it := range items {
		if loc := loadLocation(zone(it)); loc != nil {
			return loc
		}
	}
	return nil
}

// locations caches time.LoadLocation by zone name, nil for a name that does not load: loading
// reads the zoneinfo database every time.
var locations sync.Map

// loadLocation is the location of a zone name, nil for an empty name or one that does not load.
func loadLocation(name string) *time.Location {
	if name == "" {
		return nil
	}
	if v, ok := locations.Load(name); ok {
		return v.(*time.Location)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		loc = nil
	}
	locations.Store(name, loc)
	return loc
}

// gridLocation is the time zone the target cuts an item's segments in. A native item keeps the
// source node's, which the TZ gate makes equal to the target's; a CSV item is written by the
// target data nodes, so the first route node reporting a loadable zone gives it, else the
// source node's.
func gridLocation(in Inputs, item *WorkItem, nodes []string) *time.Location {
	if (in.Artifact != nil && in.Artifact.Native()) || in.Target == nil {
		return itemLocation(item)
	}
	if loc := firstLocation(targetNodes(in.Target.DataNodes, nodes), (*databasev1.Node).GetTzName); loc != nil {
		return loc
	}
	return itemLocation(item)
}

// targetSegmentEnd is the exclusive end of the segment the storage layer opens for the instant
// t: the grid of interval anchored at 1970-01-01 in t's location
// (banyand/internal/storage IntervalRule.Standard and NextTime). Without an interval it is t.
func targetSegmentEnd(t time.Time, interval *commonv1.IntervalRule) time.Time {
	num := int64(interval.GetNum())
	if num <= 0 {
		return t
	}
	loc := t.Location()
	switch interval.GetUnit() {
	case commonv1.IntervalRule_UNIT_DAY:
		midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
		days := floorDiv(int64(midnight.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, loc)).Hours()+12), 24)
		start := time.Date(1970, 1, 1+int(floorDiv(days, num)*num), 0, 0, 0, 0, loc)
		return start.AddDate(0, 0, int(num))
	case commonv1.IntervalRule_UNIT_HOUR:
		hour := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc)
		hours := floorDiv(int64(hour.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, loc))), int64(time.Hour))
		start := time.Date(1970, 1, 1, int(floorDiv(hours, num)*num), 0, 0, 0, loc)
		return start.Add(time.Duration(num) * time.Hour)
	default:
		return t
	}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
