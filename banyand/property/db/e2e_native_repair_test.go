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

package db

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	// nidx01eCorpus is the checked-in Property shard issue #14012 declares, and
	// nidx01eProvenanceFile is the manifest recording what its bytes hold. The
	// corpus lives beside the reader that pages it because the reader's own
	// boundary suite reads the same bytes; this package reads them through the
	// repair build instead.
	nidx01eCorpus         = "../../../pkg/index/inverted/testdata/nidx01e/shard-0"
	nidx01eProvenanceFile = "../../../pkg/index/inverted/testdata/nidx01e/provenance.json"

	// nidx01ePageSize is the page size issue #14012 declares the end-to-end
	// repair build runs at, and nidx01eExpectedPages is how many calls it takes
	// to exhaust the corpus's four visible rows at that size: two full pages and
	// one empty page that ends the order.
	nidx01ePageSize      = 2
	nidx01eExpectedPages = 3

	// nidx01eTreeSlotCount is the slot count the repair tree is composed over,
	// matching the count the Property repair suite already builds trees at.
	nidx01eTreeSlotCount = 32

	// repairSourceFile is the file whose read path this leaf cuts over.
	repairSourceFile = "repair.go"

	// retiredReaderEntryPoint and retiredSearcherEntryPoint are the calls that
	// open a retired third-party index reader and build its ranked searcher.
	// Naming the calls rather than the module keeps this assertion free of the
	// dependency's own name.
	retiredReaderEntryPoint   = "OpenReader"
	retiredSearcherEntryPoint = "NewTopNSearch"

	banyanDBModulePrefix = "github.com/apache/skywalking-banyandb/"
)

// nidx01eDeclaredLeaves is the completed repair tree issue #14012 declares for
// the corpus: one leaf per entity, carrying that entity's newest revision's SHA.
// The corpus holds two revisions of e-1 and one deleted revision between them,
// so a build that collapsed revisions the wrong way, or that let a deleted
// revision win, produces a different SHA here.
var nidx01eDeclaredLeaves = map[string]string{
	"g-a/n-a/e-1": "sha-b",
	"g-a/n-b/e-2": "sha-c",
	"g-b/n-a/e-3": "sha-d",
}

// repairExternalImports is the whole third-party dependency budget of the
// Property repair build once its reads are native: a clock it schedules
// against, a hash its tree slots are assigned with, an error wrapper, a cron
// parser, an error combiner and the RPC runtime its gossip server registers on.
//
// None of them reads an index. An entry appearing here for a search engine, a
// segment format or a query language is the repair build keeping a retired
// reader that BDB-NIDX-SPEC-001 revision 0.2 NIDX-01 names for replacement.
var repairExternalImports = map[string]struct{}{
	"github.com/benbjohnson/clock": {},
	"github.com/cespare/xxhash/v2": {},
	"github.com/pkg/errors":        {},
	"github.com/robfig/cron/v3":    {},
	"go.uber.org/multierr":         {},
	"google.golang.org/grpc":       {},
}

// TestE2EPropertyRepairNativeBuildsTheDeclaredTree runs one complete Property
// repair build over the checked-in shard at the declared page size and reads
// back the tree and generation state it persisted.
//
// This is the milestone's real use case end to end: an operator's repair
// schedule hands a committed shard snapshot to the build, and the build has to
// produce the Merkle tree its peers compare against over gossip. The tree's
// leaves are compared against issue #14012's declaration, not against anything
// recomputed from the corpus.
//
// Requirements proved here:
//
//	R3 -- the existing repair build pages natively. It reads the shard through
//	      the pinned generation, in ascending (group, name, entity, revision)
//	      order, projecting the stored SHA, at the page size it is configured
//	      with, and it produces exactly the three declared entity leaves with
//	      the declared SHA values.
//	R4 -- the generation state the build records names the generation it
//	      actually paged, so a later build cannot skip a generation this one
//	      never read.
func TestE2EPropertyRepairNativeBuildsTheDeclaredTree(t *testing.T) {
	tester := require.New(t)
	shard := nidx01eShardCopy(t)
	repairState, observed := nidx01eRepair(t, shard, nil)

	tester.NoError(repairState.buildStatus(context.Background(), shard))

	tester.Equal(nidx01eExpectedPages, len(observed.requests),
		"a four-row shard at page size %d must be exhausted in %d native pages", nidx01ePageSize, nidx01eExpectedPages)
	for pageIndex, request := range observed.requests {
		tester.Equal(repairSortFields, request.SortFields, "page %d must order by the declared repair components", pageIndex)
		tester.Equal(shaValueField, request.ProjectField, "page %d must project the stored repair SHA", pageIndex)
		tester.Equal(nidx01ePageSize, request.PageSize, "page %d must honor the configured page size", pageIndex)
	}
	tester.Empty(observed.requests[0].After, "the first page must start the order rather than resume into it")
	tester.Len(observed.requests[1].After, inverted.RepairSortFieldCount,
		"a later page must resume after a complete prior tuple")

	tester.Equal(nidx01eDeclaredLeaves, nidx01eTreeLeaves(t, repairState))

	state, err := repairState.readState()
	tester.NoError(err)
	tester.NotNil(state)
	tester.Equal(nidx01eDeclaredSnapshotID(t), state.LastSnpID)
}

// TestE2EPropertyRepairNativeIgnoresAGenerationPublishedBetweenPages publishes
// a Property revision into the shard while a repair build is between its first
// and second page.
//
// A repair build runs for as long as a shard is large, and the Property writer
// does not stop while it does. A build that saw part of one generation and part
// of the next would hash a tree that never existed on disk, and its peers would
// repair against it.
//
// Requirements proved here:
//
//	R4 -- a generation published after the build pinned its own does not enter
//	      a later page. The tree holds exactly the pinned generation's declared
//	      leaves, and the generation state records the pinned generation rather
//	      than the one published underneath it.
func TestE2EPropertyRepairNativeIgnoresAGenerationPublishedBetweenPages(t *testing.T) {
	tester := require.New(t)
	shard := nidx01eShardCopy(t)
	published := false
	repairState, observed := nidx01eRepair(t, shard, func(pageIndex int, _ []inverted.RepairRow, _ error) error {
		if pageIndex != 1 || published {
			return nil
		}
		published = true
		nidx01ePublishRevision(t, shard, "g-a", "n-b", "e-4", 6, "sha-published")
		return nil
	})

	tester.NoError(repairState.buildStatus(context.Background(), shard))
	tester.True(published, "the build must read more than one page, or the pin is untested")
	tester.Greater(len(observed.requests), 1)

	tester.Equal(nidx01eDeclaredLeaves, nidx01eTreeLeaves(t, repairState),
		"a revision published between two pages must not reach the tree the build was already composing")

	state, err := repairState.readState()
	tester.NoError(err)
	tester.NotNil(state)
	tester.Equal(nidx01eDeclaredSnapshotID(t), state.LastSnpID,
		"the build must record the generation it paged, not the one published underneath it")

	newest, err := inverted.OpenReadOnlyGeneration(shard)
	tester.NoError(err)
	defer func() {
		tester.NoError(newest.Close())
	}()
	tester.NotEqual(nidx01eDeclaredSnapshotID(t), newest.SnapshotID(),
		"the publication must have committed a newer generation, or nothing was raced")
}

// TestE2EPropertyRepairNativePublishesNoStateWhenAPageFails fails the build's
// second page and inspects what it left behind.
//
// Requirement proved here:
//
//	R4 -- a page that fails part way through a build publishes no repair state.
//	      Neither the tree nor the generation marker appears, so the next build
//	      redoes the work instead of trusting a tree that covers half a shard.
func TestE2EPropertyRepairNativePublishesNoStateWhenAPageFails(t *testing.T) {
	tester := require.New(t)
	shard := nidx01eShardCopy(t)
	failure := errors.New("page failed")
	repairState, observed := nidx01eRepair(t, shard, func(pageIndex int, _ []inverted.RepairRow, _ error) error {
		if pageIndex < 2 {
			return nil
		}
		return failure
	})

	buildErr := repairState.buildStatus(context.Background(), shard)
	tester.ErrorIs(buildErr, failure)
	tester.Greater(len(observed.requests), 1, "the build must have reached the failing page")

	_, statErr := os.Stat(repairState.statePath)
	tester.True(os.IsNotExist(statErr), "a failed build must record no generation state: %v", statErr)
	_, treeErr := os.Stat(repairState.composeTreeFilePath)
	tester.True(os.IsNotExist(treeErr), "a failed build must publish no repair tree: %v", treeErr)
}

// TestE2EPropertyRepairNativeKeepsNoRetiredReader parses the repair build's own
// source.
//
// Producing the right tree is not enough on its own: the point of the milestone
// is that the repair read stops going through the retired reader, and a tree
// that is right for the wrong reason would hide that.
//
// Requirement proved here:
//
//	R7 -- the Property repair build opens no retired index reader and builds no
//	      retired ranked searcher. Its third-party dependency set holds nothing
//	      that reads an index, so the only way it reaches a shard's bytes is the
//	      BanyanDB-owned pinned generation.
func TestE2EPropertyRepairNativeKeepsNoRetiredReader(t *testing.T) {
	tester := require.New(t)
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, repairSourceFile, nil, parser.SkipObjectResolution)
	tester.NoError(err)

	for _, spec := range file.Imports {
		importPath, unquoteErr := strconv.Unquote(spec.Path.Value)
		tester.NoError(unquoteErr)
		if isStandardImport(importPath) || strings.HasPrefix(importPath, banyanDBModulePrefix) {
			continue
		}
		_, budgeted := repairExternalImports[importPath]
		tester.True(budgeted, "%s imports %s, which is outside the repair build's dependency budget",
			repairSourceFile, importPath)
	}

	for _, called := range selectorCallsIn(file) {
		tester.NotEqual(retiredReaderEntryPoint, called, "%s still opens a retired index reader", repairSourceFile)
		tester.NotEqual(retiredSearcherEntryPoint, called, "%s still builds a retired ranked searcher", repairSourceFile)
	}
}

// nidx01eShardCopy copies the checked-in corpus into a writable shard directory
// so a test may publish over it without touching the pinned bytes.
func nidx01eShardCopy(t *testing.T) string {
	t.Helper()
	shard := filepath.Join(t.TempDir(), "shard-0")
	require.NoError(t, os.MkdirAll(shard, 0o755))
	require.NoError(t, copyDirRecursive(nidx01eCorpus, shard))
	return shard
}

// nidx01eRepair builds a repair state over a shard directory at the declared
// page size, and reports every page its build asks the pinned generation for.
// The optional hook runs after each page, which is where a test publishes a
// competing generation or fails the build part way through.
func nidx01eRepair(t *testing.T, shard string, hook nidx01ePageHook) (*repair, *nidx01eObservedGeneration) {
	t.Helper()
	factory := observability.BypassRegistry.With(observability.RootScope.SubScope("nidx01e"))
	repairState := newRepair(shard, t.TempDir(), logger.GetLogger("nidx01e"), factory,
		nidx01ePageSize, nidx01eTreeSlotCount, &repairScheduler{})
	observed := &nidx01eObservedGeneration{hook: hook}
	repairState.openGeneration = func(shardPath string) (repairGeneration, error) {
		generation, err := openNativeRepairGeneration(shardPath)
		if err != nil {
			return nil, err
		}
		observed.repairGeneration = generation
		return observed, nil
	}
	return repairState, observed
}

// nidx01ePageHook observes one page a build read, and may fail the build.
type nidx01ePageHook func(pageIndex int, rows []inverted.RepairRow, pageErr error) error

// nidx01eObservedGeneration wraps the pinned generation a build pages so a test
// can see which pages the build asked for and act between them.
type nidx01eObservedGeneration struct {
	repairGeneration
	hook     nidx01ePageHook
	requests []inverted.RepairPageRequest
}

func (g *nidx01eObservedGeneration) RepairTuplePage(
	ctx context.Context, request inverted.RepairPageRequest,
) ([]inverted.RepairRow, error) {
	rows, err := g.repairGeneration.RepairTuplePage(ctx, request)
	g.requests = append(g.requests, request)
	if g.hook == nil {
		return rows, err
	}
	if hookErr := g.hook(len(g.requests), rows, err); hookErr != nil {
		return nil, hookErr
	}
	return rows, err
}

// nidx01eTreeLeaves reads the repair tree a build persisted and returns its
// leaves as entity to SHA.
func nidx01eTreeLeaves(t *testing.T, repairState *repair) map[string]string {
	t.Helper()
	tree := newRepairData(repairState, nil).readTree(t, defaultGroupName)
	require.NotNil(t, tree, "the build must have persisted a repair tree")
	leaves := map[string]string{}
	for _, slot := range tree.root.children {
		for _, leaf := range slot.children {
			_, duplicate := leaves[leaf.id]
			require.False(t, duplicate, "entity %s appears in the tree more than once", leaf.id)
			leaves[leaf.id] = leaf.shaValue
		}
	}
	return leaves
}

// nidx01ePublishRevision commits one more Property revision into a shard
// directory, which publishes a new generation over it. The revision is shaped
// the way the Property writer shapes one, so a build that read it would gain a
// leaf for it.
func nidx01ePublishRevision(t *testing.T, shard, group, name, entity string, revision int64, sha string) {
	t.Helper()
	writer, err := inverted.NewStore(inverted.StoreOpts{Path: shard})
	require.NoError(t, err)
	document := index.Document{
		EntityValues: []byte(group + "/" + name + "/" + entity + "/" + strconv.FormatInt(revision, 10)),
		Timestamp:    revision,
		Fields: []index.Field{
			nidx01eIndexedField(entityID, entity),
			nidx01eIndexedField(groupField, group),
			nidx01eIndexedField(nameField, name),
			nidx01eStoredField(sourceField, "source-"+entity),
			nidx01eStoredField(shaValueField, sha),
		},
	}
	require.NoError(t, writer.UpdateSeriesBatch(index.Batch{Documents: index.Documents{document}}))
	require.NoError(t, writer.Close())
}

func nidx01eIndexedField(name, value string) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: name}, []byte(value))
	field.Index = true
	return field
}

func nidx01eStoredField(name, value string) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: name}, []byte(value))
	field.Store = true
	field.NoSort = true
	return field
}

// nidx01eDeclaredSnapshotID reads the generation the corpus commits out of the
// manifest checked in beside its bytes, so the expected value comes from the
// fixture's own declaration rather than from anything the build computes.
func nidx01eDeclaredSnapshotID(t *testing.T) uint64 {
	t.Helper()
	raw, err := os.ReadFile(nidx01eProvenanceFile)
	require.NoError(t, err)
	var manifest struct {
		SnapshotID uint64 `json:"snapshot_id"`
	}
	require.NoError(t, json.Unmarshal(raw, &manifest))
	require.NotZero(t, manifest.SnapshotID, "the corpus manifest must declare the generation its bytes commit")
	return manifest.SnapshotID
}

// isStandardImport reports whether an import path names a standard library
// package, which has no dot in its first path element.
func isStandardImport(importPath string) bool {
	return !strings.Contains(strings.SplitN(importPath, "/", 2)[0], ".")
}

// selectorCallsIn lists the names of every package-qualified call the file
// makes, so a call the build must no longer contain can be named without naming
// the package it came from.
func selectorCallsIn(file *ast.File) []string {
	var called []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		if selector, isSelector := call.Fun.(*ast.SelectorExpr); isSelector {
			called = append(called, selector.Sel.Name)
		}
		return true
	})
	return called
}
