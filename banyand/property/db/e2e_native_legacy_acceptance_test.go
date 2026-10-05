// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestE2EPropertyNativeLegacyQueryMutationAcceptance exercises only public
// Database operations in fresh native and legacy roots. It intentionally keeps
// the two roots independent and compares the observable query/mutation trace.
func TestE2EPropertyNativeLegacyQueryMutationAcceptance(t *testing.T) {
	ctx := context.Background()
	type result struct {
		values []int64
		rows   []string
		count  int
	}
	run := func(nativeWriter bool) result {
		location, cleanup, err := test.NewSpace()
		require.NoError(t, err)
		defer cleanup()
		cfg := Config{
			Location: location, MetricsScopeName: "property_native_legacy_acceptance",
			FlushInterval: time.Second,
			Index:         IndexConfig{NativeWriter: nativeWriter, WaitForPersistence: true},
		}
		db, err := OpenDB(ctx, cfg, observability.BypassRegistry, fs.NewLocalFileSystem())
		require.NoError(t, err)
		first := generateProperty("acceptance-a", 100, 10)
		second := generateProperty("acceptance-b", 200, 20)
		firstID := GetPropertyID(first)
		secondID := GetPropertyID(second)
		require.NoError(t, db.Update(ctx, 0, firstID, first))
		require.NoError(t, db.Update(ctx, 0, secondID, second))
		updated := generateProperty("acceptance-a", 101, 30)
		require.NoError(t, db.Update(ctx, 0, firstID, updated))
		require.NoError(t, db.Delete(ctx, [][]byte{secondID}, time.Now()))
		rows := queryDB(ctx, t, db, "")
		fingerprints := make([]string, 0, len(rows))
		for _, row := range rows {
			fingerprints = append(fingerprints, propertyRowFingerprint(row))
		}
		sort.Strings(fingerprints)
		require.NoError(t, db.Close())
		db, err = OpenDB(ctx, cfg, observability.BypassRegistry, fs.NewLocalFileSystem())
		require.NoError(t, err)
		defer db.Close()
		reopened := queryDB(ctx, t, db, "")
		reopenedValues := make([]int64, 0, len(reopened))
		reopenedFingerprints := make([]string, 0, len(reopened))
		for _, row := range reopened {
			reopenedValues = append(reopenedValues, unmarshalProperty(t, row.Source()).Tags[0].Value.GetInt().Value)
			reopenedFingerprints = append(reopenedFingerprints, propertyRowFingerprint(row))
		}
		sort.Slice(reopenedValues, func(i, j int) bool { return reopenedValues[i] < reopenedValues[j] })
		sort.Strings(reopenedFingerprints)
		require.Equal(t, fingerprints, reopenedFingerprints)
		return result{values: reopenedValues, rows: reopenedFingerprints, count: len(reopened)}
	}
	legacy := run(false)
	native := run(true)
	sort.Slice(legacy.values, func(i, j int) bool { return legacy.values[i] < legacy.values[j] })
	sort.Slice(native.values, func(i, j int) bool { return native.values[i] < native.values[j] })
	require.Equal(t, legacy, native)
	require.Equal(t, []int64{20, 30}, native.values)
	require.Len(t, native.rows, 2)
	require.Equal(t, 2, native.count)
}

func propertyRowFingerprint(row QueriedProperty) string {
	return hex.EncodeToString(row.ID()) + ":" +
		fmt.Sprint(row.Timestamp()) + ":" + strconv.FormatBool(row.DeleteTime() != 0) + ":" +
		hex.EncodeToString(row.Source())
}

func TestE2EPropertyNativeRepairSnapshotAndWriterSwitch(t *testing.T) {
	ctx := context.Background()
	location, cleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer cleanup()
	cfg := Config{
		Location: location, MetricsScopeName: "property_native_repair_snapshot_acceptance",
		FlushInterval: time.Second, Index: IndexConfig{NativeWriter: true, WaitForPersistence: true},
		Snapshot: SnapshotConfig{Location: filepath.Join(location, "snapshots")},
	}
	db, err := OpenDB(ctx, cfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	property := generateProperty("repair-snapshot", 300, 1)
	id := GetPropertyID(property)
	require.NoError(t, db.Update(ctx, 0, id, property))
	repaired := generateProperty("repair-snapshot", 301, 9)
	require.NoError(t, db.Repair(ctx, id, 0, repaired, 0))
	rows := queryDB(ctx, t, db, "repair-snapshot")
	require.Len(t, rows, 2)
	require.Contains(t, propertyValues(t, rows), int64(9))
	snapshot := db.TakeSnapShot(ctx, "acceptance")
	require.NotNil(t, snapshot)
	require.Empty(t, snapshot.GetError())
	restoreLocation, restoreCleanup, restoreErr := test.NewSpace()
	require.NoError(t, restoreErr)
	defer restoreCleanup()
	require.NoError(t, copyAcceptanceTree(filepath.Join(cfg.Snapshot.Location, "acceptance", "data"), restoreLocation))
	restored, restoreOpenErr := OpenDB(ctx, Config{
		Location: restoreLocation, MetricsScopeName: "property_native_restore_acceptance", FlushInterval: time.Second,
		Index:    IndexConfig{NativeWriter: true, WaitForPersistence: true},
		Snapshot: SnapshotConfig{Location: filepath.Join(restoreLocation, "snapshots")},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, restoreOpenErr)
	restoreRows := queryDB(ctx, t, restored, "repair-snapshot")
	require.Len(t, restoreRows, 2)
	require.Contains(t, propertyValues(t, restoreRows), int64(9))
	require.NoError(t, restored.Close())
	require.NoError(t, db.Update(ctx, 0, id, generateProperty("repair-snapshot", 302, 11)))
	require.Contains(t, propertyValues(t, queryDB(ctx, t, db, "repair-snapshot")), int64(11))
	restored, restoreOpenErr = OpenDB(ctx, Config{
		Location: restoreLocation, MetricsScopeName: "property_native_restore_acceptance_reopen", FlushInterval: time.Second,
		Index:    IndexConfig{NativeWriter: true, WaitForPersistence: true},
		Snapshot: SnapshotConfig{Location: filepath.Join(restoreLocation, "snapshots")},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, restoreOpenErr)
	require.Contains(t, propertyValues(t, queryDB(ctx, t, restored, "repair-snapshot")), int64(9))
	require.NoError(t, restored.Close())
	require.NoError(t, db.SwitchIndexWriter(ctx, false))
	rows = queryDB(ctx, t, db, "repair-snapshot")
	require.Len(t, rows, 2)
	require.Contains(t, propertyValues(t, rows), int64(9))
	require.NoError(t, db.SwitchIndexWriter(ctx, true))
	rows = queryDB(ctx, t, db, "repair-snapshot")
	require.Len(t, rows, 2)
	require.Contains(t, propertyValues(t, rows), int64(9))
	require.NoError(t, db.Close())
	db, err = OpenDB(ctx, cfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	defer db.Close()
	rows = queryDB(ctx, t, db, "repair-snapshot")
	require.Len(t, rows, 2)
	require.Contains(t, propertyValues(t, rows), int64(9))
}

func copyAcceptanceTree(source, destination string) error {
	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func propertyValues(t *testing.T, rows []QueriedProperty) []int64 {
	values := make([]int64, 0, len(rows))
	for _, row := range rows {
		values = append(values, unmarshalProperty(t, row.Source()).Tags[0].Value.GetInt().Value)
	}
	return values
}

func TestE2EPropertyNativeCriteriaAndSort(t *testing.T) {
	ctx := context.Background()
	run := func(nativeWriter bool) (map[string][]string, []QueriedProperty, []QueriedProperty) {
		location, cleanup, err := test.NewSpace()
		require.NoError(t, err)
		defer cleanup()
		cfg := Config{
			Location: location, MetricsScopeName: "property_native_criteria_acceptance", FlushInterval: time.Second,
			Index: IndexConfig{NativeWriter: nativeWriter, WaitForPersistence: true},
		}
		db, err := OpenDB(ctx, cfg, observability.BypassRegistry, fs.NewLocalFileSystem())
		require.NoError(t, err)
		defer db.Close()
		for i, value := range []int64{10, 20, 30, 40, 50} {
			name := string(rune('a' + i))
			property := generateProperty(name, int64(100+i), int(value))
			require.NoError(t, db.Update(ctx, common.ShardID(i%2), GetPropertyID(property), property))
		}
		absent := generateProperty("f", 105, 0)
		absent.Tags = nil
		require.NoError(t, db.Update(ctx, 1, GetPropertyID(absent), absent))
		if nativeWriter {
			propertyDB := db.(*database)
			groupValue, loaded := propertyDB.groups.Load(testPropertyGroup)
			require.True(t, loaded)
			group := groupValue.(*groupShards)
			for _, shard := range *group.shards.Load() {
				require.NotNil(t, shard.nativeStore)
				require.Nil(t, shard.store)
			}
		}
		queries := map[string]*modelv1.Criteria{
			"eq":     criteriaInt(modelv1.Condition_BINARY_OP_EQ, 30),
			"ne":     criteriaInt(modelv1.Condition_BINARY_OP_NE, 30),
			"gt":     criteriaInt(modelv1.Condition_BINARY_OP_GT, 30),
			"ge":     criteriaInt(modelv1.Condition_BINARY_OP_GE, 30),
			"lt":     criteriaInt(modelv1.Condition_BINARY_OP_LT, 30),
			"le":     criteriaInt(modelv1.Condition_BINARY_OP_LE, 30),
			"in":     criteriaInts(modelv1.Condition_BINARY_OP_IN, 20, 40),
			"not-in": criteriaInts(modelv1.Condition_BINARY_OP_NOT_IN, 20, 40),
			"having": criteriaInts(modelv1.Condition_BINARY_OP_HAVING, 20, 40),
			"and":    logicalCriteria(modelv1.LogicalExpression_LOGICAL_OP_AND, criteriaInt(modelv1.Condition_BINARY_OP_EQ, 30), criteriaInt(modelv1.Condition_BINARY_OP_GT, 20)),
			"or":     logicalCriteria(modelv1.LogicalExpression_LOGICAL_OP_OR, criteriaInt(modelv1.Condition_BINARY_OP_EQ, 10), criteriaInt(modelv1.Condition_BINARY_OP_EQ, 50)),
		}
		results := make(map[string][]string, len(queries))
		for name, criteria := range queries {
			rows := queryCriteria(ctx, t, db, criteria)
			ids := make([]string, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, unmarshalProperty(t, row.Source()).Id)
			}
			sort.Strings(ids)
			results[name] = ids
		}
		ascending := queryOrdered(ctx, t, db, modelv1.Sort_SORT_ASC)
		descending := queryOrdered(ctx, t, db, modelv1.Sort_SORT_DESC)
		// Limit is applied after physical matches are collected. This guards the
		// revision/deduplication boundary from accidentally becoming a native top-k.
		require.Len(t, queryOrderedLimit(ctx, t, db, modelv1.Sort_SORT_ASC, 1), 6)
		return results, ascending, descending
	}
	legacyResults, legacyAscending, legacyDescending := run(false)
	nativeResults, nativeAscending, nativeDescending := run(true)
	require.Equal(t, legacyResults, nativeResults)
	require.Equal(t, publicPropertyRows(legacyAscending), publicPropertyRows(nativeAscending))
	require.Equal(t, publicPropertyRows(legacyDescending), publicPropertyRows(nativeDescending))
	require.Equal(t, []string{"c"}, nativeResults["eq"])
	require.Equal(t, []string{"a", "b", "d", "e", "f"}, nativeResults["ne"])
	require.Equal(t, []string{"d", "e"}, nativeResults["gt"])
	require.Equal(t, []string{"c", "d", "e"}, nativeResults["ge"])
	require.Equal(t, []string{"a", "b"}, nativeResults["lt"])
	require.Equal(t, []string{"a", "b", "c"}, nativeResults["le"])
	require.Equal(t, []string{"b", "d"}, nativeResults["in"])
	require.Equal(t, []string{"a", "c", "e", "f"}, nativeResults["not-in"])
	require.Empty(t, nativeResults["having"])
	require.Equal(t, []string{"a", "e"}, nativeResults["or"])
	for _, rows := range [][]QueriedProperty{nativeAscending, nativeDescending} {
		for _, row := range rows {
			require.NotEmpty(t, row.SortedValue())
		}
	}
	for i := 1; i < len(nativeAscending); i++ {
		require.LessOrEqual(t, bytes.Compare(nativeAscending[i-1].SortedValue(), nativeAscending[i].SortedValue()), 0)
	}
	for i := 1; i < len(nativeDescending); i++ {
		require.GreaterOrEqual(t, bytes.Compare(nativeDescending[i-1].SortedValue(), nativeDescending[i].SortedValue()), 0)
	}
}

func publicPropertyRows(rows []QueriedProperty) []string {
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		result = append(result, propertyRowFingerprint(row)+":"+hex.EncodeToString(row.SortedValue()))
	}
	return result
}

func criteriaInt(op modelv1.Condition_BinaryOp, value int64) *modelv1.Criteria {
	return criteriaValues(op, &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: value}}})
}

func criteriaInts(op modelv1.Condition_BinaryOp, values ...int64) *modelv1.Criteria {
	return criteriaValues(op, &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: values}}})
}

func criteriaValues(op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{Name: "tag1", Op: op, Value: value}}}
}

func logicalCriteria(op modelv1.LogicalExpression_LogicalOp, left, right *modelv1.Criteria) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: op, Left: left, Right: right}}}
}

func queryCriteria(ctx context.Context, t *testing.T, db Database, criteria *modelv1.Criteria) []QueriedProperty {
	rows, err := db.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}, Criteria: criteria})
	require.NoError(t, err)
	return rows
}

func queryOrdered(ctx context.Context, t *testing.T, db Database, order modelv1.Sort) []QueriedProperty {
	return queryOrderedLimit(ctx, t, db, order, 100)
}

func queryOrderedLimit(ctx context.Context, t *testing.T, db Database, order modelv1.Sort, limit int32) []QueriedProperty {
	rows, err := db.Query(ctx, &propertyv1.QueryRequest{
		Groups: []string{testPropertyGroup}, Limit: uint32(limit),
		OrderBy: &propertyv1.QueryOrder{TagName: "tag1", Sort: order},
	})
	require.NoError(t, err)
	return rows
}
