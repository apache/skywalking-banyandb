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

package schema

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type fakeTSDB struct{}

func (fakeTSDB) Close() error { return nil }

type fakeGroup struct {
	tsdb io.Closer
	name string
}

func (g *fakeGroup) GetSchema() *commonv1.Group {
	return &commonv1.Group{Metadata: &commonv1.Metadata{Name: g.name}}
}

func (g *fakeGroup) SupplyTSDB() io.Closer { return g.tsdb }

// fakeLoader lists every group once and then answers LoadGroup from current.
type fakeLoader struct {
	current map[string]Group
	all     []Group
}

func (f *fakeLoader) LoadGroup(name string) (Group, bool) {
	g, ok := f.current[name]
	return g, ok
}

func (f *fakeLoader) LoadAllGroups() []Group { return f.all }

func TestSnapshotGroups_SkipsGroupsDeletedMeanwhile(t *testing.T) {
	kept := &fakeGroup{name: "kept", tsdb: fakeTSDB{}}
	deleted := &fakeGroup{name: "deleted", tsdb: fakeTSDB{}}
	failing := &fakeGroup{name: "failing", tsdb: fakeTSDB{}}
	noTSDB := &fakeGroup{name: "empty"}
	loader := &fakeLoader{
		all:     []Group{kept, deleted, failing, noTSDB},
		current: map[string]Group{"kept": kept, "failing": failing, "empty": noTSDB},
	}
	var taken []string
	err := SnapshotGroups(context.Background(), loader, logger.GetLogger("test"), "/dst", func(dstDir, groupName string) (bool, error) {
		if dstDir != filepath.Join("/dst", groupName) {
			t.Errorf("unexpected destination %s for %s", dstDir, groupName)
		}
		switch groupName {
		case "deleted":
			return false, errors.New("group deleted not found")
		case "failing":
			return false, errors.New("disk full")
		}
		taken = append(taken, groupName)
		return true, nil
	})
	if err == nil || err.Error() != "disk full" {
		t.Fatalf("only the failure of a group that still exists must be reported, got %v", err)
	}
	if !slices.Equal(taken, []string{"kept"}) {
		t.Fatalf("want only kept snapshotted, got %v", taken)
	}
}

func TestSnapshotGroups_CanceledStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loader := &fakeLoader{all: []Group{&fakeGroup{name: "g", tsdb: fakeTSDB{}}}}
	err := SnapshotGroups(ctx, loader, logger.GetLogger("test"), "/dst", func(string, string) (bool, error) {
		t.Fatal("a canceled snapshot must not take any group")
		return false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
