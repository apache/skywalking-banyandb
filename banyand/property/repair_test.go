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

package property

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var (
	defaultGroupName    = "default_group"
	defaultPropertyName = "default_property"
	defaultValue        = 22
)

func TestBuildTree(t *testing.T) {
	tests := []struct {
		existingDoc      func(s *shard) ([]index.Document, error)
		statusVerify     func(t *testing.T, s *shard, data *repairData)
		afterFirstBuild  func(s *shard) ([]index.Document, error)
		nextStatusVerify func(t *testing.T, s *shard, before, after *repairData)
		name             string
	}{
		{
			name: "empty shard",
			statusVerify: func(t *testing.T, _ *shard, data *repairData) {
				if data.repairStatus != nil {
					t.Fatal("expected nil status")
				}
				tree := data.readTree(t, defaultGroupName)
				if tree != nil {
					t.Fatalf("expected no tree for group %s, but got one", defaultGroupName)
				}
			},
		},
		{
			name: "single property",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, data *repairData) {
				basicStatusVerify(t, data, defaultGroupName, 1)
				verifyContainsProperty(t, s, data, defaultGroupName, propertyBuilder{id: "test1"})
			},
		},
		{
			name: "single property with multiple versions",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1", version: 1},
					propertyBuilder{id: "test1", version: 2, deleteTime: 2000},
				)
			},
			statusVerify: func(t *testing.T, s *shard, data *repairData) {
				basicStatusVerify(t, data, defaultGroupName, 1)
				verifyContainsProperty(t, s, data, defaultGroupName,
					propertyBuilder{id: "test1", version: 2, deleteTime: 2000})
			},
		},
		{
			name: "multiple properties with versions",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1", version: 3, value: 1},
					propertyBuilder{id: "test1", version: 4, value: 2},
					propertyBuilder{id: "test2", version: 1, value: 1},
					propertyBuilder{id: "test2", version: 2, value: 2},
					propertyBuilder{id: "test3"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, data *repairData) {
				basicStatusVerify(t, data, defaultGroupName, 3)
				verifyContainsProperty(t, s, data, defaultGroupName, propertyBuilder{id: "test1", version: 4, value: 2})
				verifyContainsProperty(t, s, data, defaultGroupName, propertyBuilder{id: "test2", version: 2, value: 2})
				verifyContainsProperty(t, s, data, defaultGroupName, propertyBuilder{id: "test3"})
			},
		},
		{
			name: "build with multiple groups",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{group: "group1", id: "test1"},
					propertyBuilder{group: "group2", id: "test2"},
					propertyBuilder{group: "group3", id: "test3"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, data *repairData) {
				basicStatusVerify(t, data, "group1", 1, "group2", 1, "group3", 1)
				verifyContainsProperty(t, s, data, "group1", propertyBuilder{group: "group1", id: "test1"})
				verifyContainsProperty(t, s, data, "group2", propertyBuilder{group: "group2", id: "test2"})
				verifyContainsProperty(t, s, data, "group3", propertyBuilder{group: "group3", id: "test3"})
			},
		},
		{
			name: "build multiple times",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, data *repairData) {
				basicStatusVerify(t, data, defaultGroupName, 1)
				verifyContainsProperty(t, s, data, defaultGroupName, propertyBuilder{id: "test1"})
			},
			nextStatusVerify: func(t *testing.T, _ *shard, before, after *repairData) {
				basicStatusVerify(t, before, defaultGroupName, 1)
				basicStatusVerify(t, after, defaultGroupName, 1)
				if before.LastSnpID != after.LastSnpID {
					t.Fatalf("expected last snapshot ID to be the same, got before: %d, after: %d",
						before.LastSnpID, after.LastSnpID)
				}
				if before.readTree(t, defaultGroupName).root.shaValue != after.readTree(t, defaultGroupName).root.shaValue {
					t.Fatalf("expected Root sha value to be the same, got before: %s, after: %s",
						before.readTree(t, defaultGroupName).root.shaValue, after.readTree(t, defaultGroupName).root.shaValue)
				}
			},
		},
		{
			name: "build multiple times with new properties",
			existingDoc: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test1"},
				)
			},
			statusVerify: func(t *testing.T, s *shard, data *repairData) {
				basicStatusVerify(t, data, defaultGroupName, 1)
				verifyContainsProperty(t, s, data, defaultGroupName, propertyBuilder{id: "test1"})
			},
			afterFirstBuild: func(s *shard) ([]index.Document, error) {
				return buildPropertyDocuments(s,
					propertyBuilder{id: "test2"},
				)
			},
			nextStatusVerify: func(t *testing.T, s *shard, before, after *repairData) {
				basicStatusVerify(t, after, defaultGroupName, 2)
				verifyContainsProperty(t, s, after, defaultGroupName,
					propertyBuilder{id: "test1"},
					propertyBuilder{id: "test2"},
				)
				if before.LastSnpID == after.LastSnpID {
					t.Fatalf("expected last snapshot ID to be incremented by 1, got before: %d, after: %d",
						before.LastSnpID, after.LastSnpID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var defers []func()
			defer func() {
				for _, f := range defers {
					f()
				}
			}()

			dataDir, dataDeferFunc, err := test.NewSpace()
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, dataDeferFunc)
			snapshotDir, snapshotDeferFunc, err := test.NewSpace()
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, snapshotDeferFunc)
			db, err := openDB(context.Background(), dataDir, 3*time.Second, time.Hour, 32,
				observability.BypassRegistry, fs.NewLocalFileSystem(), true, snapshotDir,
				"@every 10m", time.Second*10, "* 2 * * *", nil, nil, func(context.Context) (string, error) {
					snapshotDir, defFunc, newSpaceErr := test.NewSpace()
					if newSpaceErr != nil {
						return "", newSpaceErr
					}
					defers = append(defers, defFunc)
					return snapshotDir, copyDirRecursive(dataDir, snapshotDir)
				})
			if err != nil {
				t.Fatal(err)
			}
			defers = append(defers, func() {
				_ = db.close()
			})

			newShard, err := db.loadShard(context.Background(), 0)
			if err != nil {
				t.Fatal(err)
			}

			var docs []index.Document
			if tt.existingDoc != nil {
				docs, err = tt.existingDoc(newShard)
				if err != nil {
					t.Fatal(err)
				}
				if err = newShard.updateDocuments(docs); err != nil {
					t.Fatal(err)
				}
			}

			err = newShard.repairState.scheduler.doBuildTree()
			if err != nil {
				t.Fatalf("failed to build status: %v", err)
			}
			status, err := newShard.repairState.readState()
			if err != nil {
				t.Fatalf("failed to read state: %v", err)
			}
			data := newRepairData(newShard.repairState, status)
			tt.statusVerify(t, newShard, data)
			olderData := data.clone()

			if tt.afterFirstBuild != nil {
				docs, err := tt.afterFirstBuild(newShard)
				if err != nil {
					t.Fatal(err)
				}
				if err := newShard.updateDocuments(docs); err != nil {
					t.Fatal(err)
				}
			}

			if tt.nextStatusVerify != nil {
				err := newShard.repairState.scheduler.doBuildTree()
				if err != nil {
					t.Fatalf("failed to build status after update: %v", err)
				}
				statusAfter, err := newShard.repairState.readState()
				if err != nil {
					t.Fatalf("failed to read state after update: %v", err)
				}
				tt.nextStatusVerify(t, newShard, olderData, newRepairData(newShard.repairState, statusAfter))
			}
		})
	}
}

func TestDocumentUpdatesNotify(t *testing.T) {
	gomega.RegisterTestingT(t)

	var defers []func()
	defer func() {
		for _, f := range defers {
			f()
		}
	}()

	dataDir, dataDeferFunc, err := test.NewSpace()
	if err != nil {
		t.Fatal(err)
	}
	defers = append(defers, dataDeferFunc)
	snapshotDir, snapshotDeferFunc, err := test.NewSpace()
	if err != nil {
		t.Fatal(err)
	}
	defers = append(defers, snapshotDeferFunc)
	db, err := openDB(context.Background(), dataDir, 3*time.Second, time.Hour, 32,
		observability.BypassRegistry, fs.NewLocalFileSystem(), true, snapshotDir,
		"@every 10m", time.Millisecond*50, "* 2 * * *", nil, nil, func(context.Context) (string, error) {
			tmpDir, defFunc, newSpaceErr := test.NewSpace()
			if newSpaceErr != nil {
				return "", newSpaceErr
			}
			defers = append(defers, defFunc)
			return tmpDir, copyDirRecursive(dataDir, tmpDir)
		})
	if err != nil {
		t.Fatal(err)
	}
	defers = append(defers, func() {
		_ = db.close()
	})

	newShard, err := db.loadShard(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}

	p1 := buildProperties(propertyBuilder{id: "1"})
	err = newShard.update(GetPropertyID(p1), p1)
	if err != nil {
		t.Fatalf("failed to update property: %v", err)
	}

	// wait for the repair tree to be built
	gomega.Eventually(func() bool {
		tree, _ := newShard.repairState.treeReader(defaultGroupName)
		if tree != nil {
			_ = tree.close()
		}
		return tree != nil
	}).WithTimeout(flags.EventuallyTimeout).Should(gomega.BeTrue())
}

func basicStatusVerify(t *testing.T, data *repairData, groupWithSlots ...interface{}) {
	if data == nil {
		t.Fatal("status is nil")
	}
	trees := make(map[string]*repairTestTree)
	for i := 0; i < len(groupWithSlots); i += 2 {
		trees[groupWithSlots[i].(string)] = data.readTree(t, groupWithSlots[i].(string))
	}
	if len(groupWithSlots)/2 != len(trees) {
		t.Fatalf("expected %d groups in Trees, but got %d", len(groupWithSlots)/2, len(trees))
	}
	if len(groupWithSlots) == 0 {
		return
	}
	for i := 0; i < len(groupWithSlots); i += 2 {
		group := groupWithSlots[i].(string)
		slotCount := groupWithSlots[i+1].(int)

		tree := trees[group]
		if tree == nil {
			t.Fatalf("expected group %s to be present in Trees, but it is not", group)
		}
		if tree.root == nil {
			t.Fatalf("expected group %s to have a Root, but it is nil", group)
		}
		if len(tree.root.children) != slotCount {
			t.Fatalf("expected group %s to have %d slots, but it has %d", group, slotCount, len(tree.root.children))
		}
		if slotCount == 0 {
			continue
		}

		if tree.root.shaValue == "" {
			t.Fatalf("expected group %s Root to have a sha value, but it is empty", group)
		}

		for _, child := range tree.root.children {
			if child == nil {
				t.Fatalf("expected group %s Root child to be non-nil, but it is nil", group)
			}
			if child.shaValue == "" {
				t.Fatalf("expected group %s Root child to have a sha value, but it is empty", group)
			}
			if len(child.children) == 0 {
				t.Fatalf("expected group %s Root child to have Children, but it has none", group)
			}
			for _, grandChild := range child.children {
				if grandChild == nil {
					t.Fatalf("expected group %s Root child grandchild to be non-nil, but it is nil", group)
				}
				if grandChild.shaValue == "" {
					t.Fatalf("expected group %s Root child grandchild to have a sha value, but it is empty", group)
				}
			}
		}
	}
}

func buildPropertyDocuments(s *shard, builders ...propertyBuilder) ([]index.Document, error) {
	result := make([]index.Document, 0, len(builders))
	for _, builder := range builders {
		p := buildProperties(builder)
		document, err := s.buildUpdateDocument([]byte(GetEntity(p)), p, builder.deleteTime)
		if err != nil {
			return nil, err
		}
		result = append(result, *document)
	}
	return result, nil
}

func buildProperties(builder propertyBuilder) *propertyv1.Property {
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{
			Group:       stringDefaultValue(builder.group, defaultGroupName),
			Name:        stringDefaultValue(builder.name, defaultPropertyName),
			ModRevision: builder.version,
		},
		Id: builder.id,
		Tags: []*modelv1.Tag{
			{
				Key: "test_key",
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Int{
						Int: &modelv1.Int{
							Value: int64DefaultValue(builder.value, int64(defaultValue)),
						},
					},
				},
			},
		},
	}
}

type propertyBuilder struct {
	group      string
	name       string
	id         string
	value      int64
	version    int64
	deleteTime int64
}

func stringDefaultValue(current, defaultVal string) string {
	if current == "" {
		return defaultVal
	}
	return current
}

func int64DefaultValue(current, defaultVal int64) int64 {
	if current == 0 {
		return defaultVal
	}
	return current
}

func generatePropertyEntity(group, name, id string) string {
	return fmt.Sprintf("%s/%s/%s",
		stringDefaultValue(group, defaultGroupName),
		stringDefaultValue(name, defaultPropertyName),
		id,
	)
}

func generateShaValue(t *testing.T, s *shard, builder propertyBuilder) string {
	property := buildProperties(builder)
	marshal, err := protojson.Marshal(property)
	if err != nil {
		t.Fatalf("failed to marshal property: %v", err)
	}
	shaValue, err := s.repairState.buildShaValue(marshal, builder.deleteTime)
	if err != nil {
		t.Fatalf("failed to build sha value: %v", err)
	}
	return shaValue
}

func verifyContainsProperty(t *testing.T, s *shard, data *repairData, group string, properties ...propertyBuilder) {
	if len(properties) == 0 {
		return
	}
	for _, builder := range properties {
		entity := generatePropertyEntity(builder.group, builder.name, builder.id)
		found := false
		for _, child := range data.readTree(t, group).root.children {
			for _, grandChild := range child.children {
				if grandChild.id == entity {
					found = true
					if grandChild.shaValue != generateShaValue(t, s, builder) {
						t.Fatalf("expected sha value '%s', got '%s'", child.shaValue, generateShaValue(t, s, builder))
					}
					break
				}
			}
		}
		if !found {
			t.Fatalf("property %s not found in the Trees", entity)
		}
	}
}

type repairData struct {
	*repairStatus

	repair *repair

	cache         map[string]*repairTestTree
	readCacheOnly bool
}

func newRepairData(repair *repair, status *repairStatus) *repairData {
	return &repairData{
		repairStatus: status,
		repair:       repair,
		cache:        make(map[string]*repairTestTree),
	}
}

func (r *repairData) readTree(t *testing.T, group string) *repairTestTree {
	if tree, exist := r.cache[group]; exist {
		return tree
	}
	if r.readCacheOnly {
		t.Fatalf("readTree called for group %s, but cache only mode is enabled", group)
	}
	reader, err := r.repair.treeReader(group)
	if err != nil {
		t.Fatalf("failed to get tree reader for group %s: %v", group, err)
	}
	if reader == nil {
		return nil
	}
	defer func() {
		_ = reader.close()
	}()

	roots, err := reader.read(nil, 10, false)
	if err != nil {
		t.Fatalf("failed to read tree for group %s: %v", group, err)
	}
	if len(roots) == 0 {
		t.Fatalf("expected at least one root for group %s, but got none", group)
	}
	tree := &repairTestTree{
		root: &repairTestTreeNode{
			id:       "root",
			shaValue: roots[0].shaValue,
		},
	}
	slots, err := reader.read(roots[0], 10, false)
	if err != nil {
		t.Fatalf("failed to read slots for group %s: %v", group, err)
	}
	if len(slots) == 0 {
		t.Fatalf("expected at least one slot for group %s, but got none", group)
	}
	for _, slot := range slots {
		slotNode := &repairTestTreeNode{
			id:       fmt.Sprintf("%d", slot.slotInx),
			shaValue: slot.shaValue,
		}
		tree.root.children = append(tree.root.children, slotNode)
		children, err := reader.read(slot, 10, false)
		if err != nil {
			t.Fatalf("failed to read children for slot %d in group %s: %v", slot.slotInx, group, err)
		}
		if len(children) == 0 {
			t.Fatalf("expected at least one child for slot %d in group %s, but got none", slot.slotInx, group)
		}
		for _, child := range children {
			childNode := &repairTestTreeNode{
				id:       child.entity,
				shaValue: child.shaValue,
			}
			slotNode.children = append(slotNode.children, childNode)
		}
	}

	r.cache[group] = tree
	return tree
}

func (r *repairData) clone() *repairData {
	return &repairData{
		repair:        r.repair,
		repairStatus:  r.repairStatus,
		cache:         r.cache,
		readCacheOnly: true,
	}
}

type repairTestTree struct {
	root *repairTestTreeNode
}

type repairTestTreeNode struct {
	id       string
	shaValue string
	children []*repairTestTreeNode
}

func copyFile(srcFile, dstFile string) (err error) {
	defer func() {
		// in the file not found, means the db is cleaning
		if err != nil && os.IsNotExist(err) {
			err = nil
		}
	}()
	src, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstFile)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}

	info, err := src.Stat()
	if err != nil {
		return err
	}
	return os.Chmod(dstFile, info.Mode())
}

func copyDirRecursive(srcDir, dstDir string) error {
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		dstPath := filepath.Join(dstDir, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		return copyFile(path, dstPath)
	})
}
