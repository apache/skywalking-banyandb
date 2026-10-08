// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

func encodeSegmentFile(t *testing.T, directory, name string, documents int) string {
	t.Helper()
	generation := Generation{}
	for index := 0; index < documents; index++ {
		generation.Documents = append(generation.Documents, EncodeDocument{
			Identifier: []byte(fmt.Sprintf("series-%06d", index)),
			Fields:     []EncodeField{{Name: "tag", Value: []byte(fmt.Sprintf("v%d", index%7)), Index: true, Store: true}},
		})
	}
	payload, encodeErr := EncodeSegment(generation)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	path := filepath.Join(directory, name)
	if writeErr := os.WriteFile(path, payload, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	return path
}

func requireIdentifier(t *testing.T, reader *Reader, identifier string) {
	t.Helper()
	_, found, postingErr := reader.TermPosting(identifierField, []byte(identifier))
	if postingErr != nil || !found {
		t.Fatalf("lookup %q: found %v, err %v", identifier, found, postingErr)
	}
}

// TestPersistedSegmentServesFromItsFile runs lookups, walks and term filters
// against a file-backed segment whose dictionaries are read in place.
func TestPersistedSegmentServesFromItsFile(t *testing.T) {
	directory := t.TempDir()
	path := encodeSegmentFile(t, directory, "segment.seg", 3000)
	reader, openErr := OpenSegmentFile(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	if _, fileBacked := reader.segments[0].file.(*fsSegmentFile); !fileBacked {
		t.Fatal("a persisted segment must be read through its file")
	}
	if prepareErr := reader.PrepareTermFilter(identifierField); prepareErr != nil {
		t.Fatal(prepareErr)
	}
	for _, identifier := range []string{"series-000000", "series-001234", "series-002999"} {
		requireIdentifier(t, reader, identifier)
	}
	if _, found, postingErr := reader.TermPosting(identifierField, []byte("series-999999")); postingErr != nil || found {
		t.Fatalf("absent identifier: found %v, err %v", found, postingErr)
	}
	visited := 0
	if walkErr := reader.VisitLiveDocuments(context.Background(), func(StoredDocument) error {
		visited++
		return nil
	}); walkErr != nil || visited != 3000 {
		t.Fatalf("walk visited %d documents, err %v", visited, walkErr)
	}
	storedReader, readerErr := reader.storedReader(0)
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	if _, filtered := storedReader.termFilters.Load(identifierField); !filtered {
		t.Fatal("the identifier term filter must be kept for the segment's lifetime")
	}
	cached, _ := storedReader.dictionaries.Load(identifierField)
	if _, paged := cached.(*pagedFST); !paged {
		t.Fatalf("a persisted segment's dictionary is %T, want it read in place", cached)
	}
}

// TestCloseLetsAdmittedOperationsFinish closes a Reader while an operation
// is reading: Close returns at once, the operation reads on to its end, and
// the files are released when it returns, which WhenReleased reports.
// Afterwards every operation, handle and iterator reports ErrReaderClosed.
func TestCloseLetsAdmittedOperationsFinish(t *testing.T) {
	directory := t.TempDir()
	reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, "segment.seg", 50))
	if openErr != nil {
		t.Fatal(openErr)
	}
	dictionary, dictionaryErr := reader.Dictionary(identifierField)
	if dictionaryErr != nil {
		t.Fatal(dictionaryErr)
	}
	iterator, iteratorErr := reader.NewDictionaryTermIterator(identifierField, nil, nil, nil)
	if iteratorErr != nil {
		t.Fatal(iteratorErr)
	}
	file := reader.segments[0].file.(*fsSegmentFile)
	inside := make(chan struct{})
	release := make(chan struct{})
	visitDone := make(chan error, 1)
	visited := 0
	go func() {
		visitDone <- reader.VisitPhysicalDocuments(context.Background(), func(StoredDocument, bool) error {
			if visited == 0 {
				close(inside)
				<-release
			}
			visited++
			return nil
		})
	}()
	<-inside
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	released := make(chan error, 1)
	reader.WhenReleased(func(releaseErr error) { released <- releaseErr })
	if done, _ := reader.releaseState(); done || fileClosed(file) {
		t.Fatal("Close released the files while an operation was reading them")
	}
	close(release)
	if visitErr := <-visitDone; visitErr != nil || visited != 50 {
		t.Fatalf("an operation admitted before Close must finish: visited %d, err %v", visited, visitErr)
	}
	if releaseErr := <-released; releaseErr != nil {
		t.Fatal(releaseErr)
	}
	if !fileClosed(file) {
		t.Fatal("the last admitted operation must release the files")
	}
	requireClosedEverywhere(t, reader, dictionary, iterator)
}

func requireClosedEverywhere(t *testing.T, reader *Reader, dictionary *Dictionary, iterator *DictionaryTermIterator) {
	t.Helper()
	for name, err := range map[string]error{
		"posting": func() error { _, _, err := reader.TermPosting(identifierField, []byte("series-000001")); return err }(),
		"walk":    reader.VisitLiveDocuments(context.Background(), func(StoredDocument) error { return nil }),
		"fields":  func() error { _, err := reader.Fields(); return err }(),
		"values":  func() error { _, err := reader.DocumentValues("tag", 1); return err }(),
		"stored": func() error {
			_, err := reader.storedReader(0)
			return err
		}(),
		"dictionary": func() error { _, _, err := dictionary.TermPosting([]byte("series-000001")); return err }(),
		"iterator":   func() error { _, err := iterator.NextTerm(); return err }(),
	} {
		if !errors.Is(err, ErrReaderClosed) {
			t.Errorf("%s after Close = %v, want ErrReaderClosed", name, err)
		}
	}
	_ = iterator.Close()
}

// TestCloseFromWithinAnOperation closes a Reader from an operation's own
// callback, which must neither deadlock nor cut the operation short.
func TestCloseFromWithinAnOperation(t *testing.T) {
	directory := t.TempDir()
	reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, "segment.seg", 3000))
	if openErr != nil {
		t.Fatal(openErr)
	}
	dictionary, dictionaryErr := reader.Dictionary(identifierField)
	if dictionaryErr != nil {
		t.Fatal(dictionaryErr)
	}
	iterator, iteratorErr := reader.NewDictionaryTermIterator(identifierField, nil, nil, nil)
	if iteratorErr != nil {
		t.Fatal(iteratorErr)
	}
	file := reader.segments[0].file.(*fsSegmentFile)
	var releasedDuringWalk atomic.Bool
	visited := 0
	done := make(chan error, 1)
	go func() {
		done <- reader.VisitLiveDocuments(context.Background(), func(StoredDocument) error {
			if visited == 0 {
				if closeErr := reader.Close(); closeErr != nil {
					return closeErr
				}
			}
			if fileClosed(file) {
				releasedDuringWalk.Store(true)
			}
			visited++
			return nil
		})
	}()
	select {
	case visitErr := <-done:
		if visitErr != nil || visited != 3000 {
			t.Fatalf("visited %d documents, err %v", visited, visitErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close from within an operation deadlocked")
	}
	if releasedDuringWalk.Load() {
		t.Fatal("a Close from within the walk released the files under it")
	}
	if released, releaseErr := reader.releaseState(); !released || releaseErr != nil {
		t.Fatalf("the walk must release the files when it ends: released %v, err %v", released, releaseErr)
	}
	requireClosedEverywhere(t, reader, dictionary, iterator)
}

// TestCloseFromAnotherGoroutineFedByACallback closes a Reader from a second
// goroutine a callback hands the work to and waits for: Close never waits
// for the callback, so this cannot deadlock.
func TestCloseFromAnotherGoroutineFedByACallback(t *testing.T) {
	directory := t.TempDir()
	reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, "segment.seg", 300))
	if openErr != nil {
		t.Fatal(openErr)
	}
	requests := make(chan chan error)
	go func() {
		for reply := range requests {
			reply <- reader.Close()
		}
	}()
	defer close(requests)
	done := make(chan error, 1)
	go func() {
		first := true
		done <- reader.VisitLiveDocuments(context.Background(), func(StoredDocument) error {
			if first {
				first = false
				reply := make(chan error)
				requests <- reply
				return <-reply
			}
			return nil
		})
	}()
	select {
	case visitErr := <-done:
		if visitErr != nil {
			t.Fatal(visitErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a Close requested by a callback and awaited by it deadlocked")
	}
	if released, _ := reader.releaseState(); !released {
		t.Fatal("the walk must release the files when it ends")
	}
}

// TestCloseDuringVisitDocumentWhileAMergeWaits closes a Reader from the
// callback of a VisitDocument, which holds the stored reader's walk lock,
// while a merge of the same Reader waits for that lock: the merge, admitted
// before Close, completes, and the Reader is released after it.
func TestCloseDuringVisitDocumentWhileAMergeWaits(t *testing.T) {
	directory := t.TempDir()
	reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, "segment.seg", 2000))
	if openErr != nil {
		t.Fatal(openErr)
	}
	inside := make(chan struct{})
	release := make(chan struct{})
	visited := make(chan error, 1)
	go func() {
		visited <- reader.VisitDocument(5, func(StoredDocument) error {
			close(inside)
			<-release
			return reader.Close()
		})
	}()
	<-inside
	merged := make(chan error, 1)
	go func() {
		_, mergeErr := MergeSegmentsTo(context.Background(), []MergeInput{{Reader: reader}}, &countingBuffer{}, "")
		merged <- mergeErr
	}()
	// Let the merge be admitted and reach the walk lock VisitDocument holds.
	for reader.active.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	for _, name := range []string{"VisitDocument", "merge"} {
		result := visited
		if name == "merge" {
			result = merged
		}
		select {
		case resultErr := <-result:
			if resultErr != nil {
				t.Fatalf("%s = %v", name, resultErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s deadlocked", name)
		}
	}
	if released, _ := reader.releaseState(); !released {
		t.Fatal("the merge must release the closed Reader when it ends")
	}
}

// TestLookupsRacingCloseReportOnlyClosed runs point lookups and document
// value reads, which are not admitted operations, against a Reader being
// closed: each either succeeds or reports ErrReaderClosed, never corruption.
func TestLookupsRacingCloseReportOnlyClosed(t *testing.T) {
	directory := t.TempDir()
	for round := 0; round < 20; round++ {
		reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, fmt.Sprintf("segment-%d.seg", round), 500))
		if openErr != nil {
			t.Fatal(openErr)
		}
		var group sync.WaitGroup
		failures := make(chan error, 8)
		for worker := 0; worker < 4; worker++ {
			group.Add(1)
			go func(worker int) {
				defer group.Done()
				for attempt := 0; ; attempt++ {
					identifier := []byte(fmt.Sprintf("series-%06d", (worker*131+attempt)%500))
					_, _, postingErr := reader.TermPosting(identifierField, identifier)
					_, valuesErr := reader.DocumentValues("tag", uint64(attempt%500))
					for _, err := range []error{postingErr, valuesErr} {
						if err != nil && !errors.Is(err, ErrReaderClosed) {
							failures <- err
							return
						}
					}
					if errors.Is(postingErr, ErrReaderClosed) && errors.Is(valuesErr, ErrReaderClosed) {
						return
					}
				}
			}(worker)
		}
		time.Sleep(time.Millisecond)
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		group.Wait()
		close(failures)
		for err := range failures {
			t.Fatalf("a lookup racing Close = %v, want success or ErrReaderClosed", err)
		}
	}
}

// TestRenameSegmentFileClosesAndReopensWhereRequired runs the rename path
// platforms that cannot rename an open file take, on any platform.
func TestRenameSegmentFileClosesAndReopensWhereRequired(t *testing.T) {
	for _, needsClose := range []bool{false, true} {
		t.Run(fmt.Sprintf("needsClose=%v", needsClose), func(t *testing.T) {
			previous := renameNeedsClose
			renameNeedsClose = needsClose
			t.Cleanup(func() { renameNeedsClose = previous })
			directory := t.TempDir()
			staged := encodeSegmentFile(t, directory, ".native-merge-test", 100)
			reader, openErr := OpenSegmentFile(staged)
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer func() { _ = reader.Close() }()
			final := filepath.Join(directory, "000000000007.seg")
			if renameErr := reader.RenameSegmentFile(final); renameErr != nil {
				t.Fatal(renameErr)
			}
			if _, statErr := os.Stat(staged); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("staged name still exists: %v", statErr)
			}
			requireIdentifier(t, reader, "series-000042")
			var copied countingBuffer
			if copyErr := reader.copySegmentTo(&copied); copyErr != nil {
				t.Fatal(copyErr)
			}
			if info, _ := os.Stat(final); copied.n != info.Size() {
				t.Fatalf("copied %d bytes, file holds %d", copied.n, info.Size())
			}
		})
	}
}

type countingBuffer struct{ n int64 }

func (b *countingBuffer) Write(data []byte) (int, error) {
	b.n += int64(len(data))
	return len(data), nil
}

// TestCloseDuringMergeLetsItFinish closes a merge input while the merge
// runs: the merge was admitted on its inputs, so it finishes, and its result
// is not the input's -- an error closing the input reaches WhenReleased, not
// the merge. A merge started afterwards reports ErrReaderClosed.
func TestCloseDuringMergeLetsItFinish(t *testing.T) {
	directory := t.TempDir()
	reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, "input.seg", 2000))
	if openErr != nil {
		t.Fatal(openErr)
	}
	file := reader.segments[0].file.(*fsSegmentFile)
	file.file = failingCloseFile{File: file.file}
	blocked := make(chan struct{})
	release := make(chan struct{})
	output := &blockingWriter{blocked: blocked, release: release}
	merged := make(chan error, 1)
	go func() {
		_, mergeErr := MergeSegmentsTo(context.Background(), []MergeInput{{Reader: reader}}, output, "")
		merged <- mergeErr
	}()
	<-blocked
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatalf("Close with a merge in flight = %v, want nil", closeErr)
	}
	releaseErrs := make(chan error, 1)
	reader.WhenReleased(func(releaseErr error) { releaseErrs <- releaseErr })
	close(release)
	if mergeErr := <-merged; mergeErr != nil {
		t.Fatalf("a merge whose input closed with an error = %v, want success", mergeErr)
	}
	if releaseErr := <-releaseErrs; releaseErr == nil {
		t.Fatal("the input's close error must reach WhenReleased")
	}
	if _, mergeErr := MergeSegmentsTo(context.Background(), []MergeInput{{Reader: reader}}, &countingBuffer{}, ""); !errors.Is(mergeErr, ErrReaderClosed) {
		t.Fatalf("merging a closed input = %v, want ErrReaderClosed", mergeErr)
	}
}

// TestRenameWaitsForReadsInFlight renames a segment, on the path platforms
// that cannot rename an open file take, while lookups and a snapshot copy
// read it: none of them fails.
func TestRenameWaitsForReadsInFlight(t *testing.T) {
	previous := renameNeedsClose
	renameNeedsClose = true
	t.Cleanup(func() { renameNeedsClose = previous })
	directory := t.TempDir()
	path := encodeSegmentFile(t, directory, ".native-merge-test", 2000)
	reader, openErr := OpenSegmentFile(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	stop := make(chan struct{})
	failures := make(chan error, 16)
	var lookups atomic.Int64
	var group sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for attempt := 0; ; attempt++ {
				select {
				case <-stop:
					return
				default:
				}
				identifier := fmt.Sprintf("series-%06d", (worker*977+attempt)%2000)
				if _, found, postingErr := reader.TermPosting(identifierField, []byte(identifier)); postingErr != nil || !found {
					failures <- fmt.Errorf("lookup %s: found %v, err %w", identifier, found, postingErr)
					return
				}
				lookups.Add(1)
				if attempt%50 == 0 {
					if copyErr := reader.copySegmentTo(&countingBuffer{}); copyErr != nil {
						failures <- fmt.Errorf("copy: %w", copyErr)
						return
					}
				}
			}
		}(worker)
	}
	// Keep renaming until the readers have overlapped many renames.
	for rename := 0; rename < 50 || lookups.Load() < 20000; rename++ {
		next := filepath.Join(directory, fmt.Sprintf("%012x.seg", rename+1))
		if renameErr := reader.RenameSegmentFile(next); renameErr != nil {
			t.Fatal(renameErr)
		}
		if len(failures) > 0 {
			break
		}
	}
	close(stop)
	group.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
}

// fileClosed reports whether a segment file's descriptor is closed.
func fileClosed(file *fsSegmentFile) bool {
	file.mu.RLock()
	defer file.mu.RUnlock()
	return file.closed
}

// TestReaderHoldsOneDescriptorForItsLifetime opens many segment readers:
// each holds exactly one descriptor, from open until its release.
func TestReaderHoldsOneDescriptorForItsLifetime(t *testing.T) {
	directory := t.TempDir()
	before := openFileDescriptorCount(t)
	readers := make([]*Reader, 0, 40)
	for index := 0; index < 40; index++ {
		reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, fmt.Sprintf("%012x.seg", index+1), 100))
		if openErr != nil {
			t.Fatal(openErr)
		}
		readers = append(readers, reader)
	}
	for round := 0; round < 3; round++ {
		for index, reader := range readers {
			if visitErr := reader.VisitDocument(uint64((index+round)%100), func(StoredDocument) error { return nil }); visitErr != nil {
				t.Fatal(visitErr)
			}
		}
	}
	if open := openFileDescriptorCount(t) - before; open != len(readers) {
		t.Fatalf("%d descriptors open for %d segment readers", open, len(readers))
	}
	for _, reader := range readers {
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if open := openFileDescriptorCount(t) - before; open != 0 {
		t.Fatalf("%d descriptors left open after every reader closed", open)
	}
}

type failingCloseFile struct {
	fs.File
}

func (f failingCloseFile) Close() error {
	return errors.Join(f.File.Close(), errors.New("injected close failure"))
}

// TestReleaseHooksAllRunAndMayClose registers release hooks that panic and
// that call Close: every hook runs, with the release's error, and none
// deadlocks.
func TestReleaseHooksAllRunAndMayClose(t *testing.T) {
	directory := t.TempDir()
	reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, "segment.seg", 100))
	if openErr != nil {
		t.Fatal(openErr)
	}
	file := reader.segments[0].file.(*fsSegmentFile)
	file.file = failingCloseFile{File: file.file}
	var ran atomic.Int32
	reader.WhenReleased(func(error) { panic("hook failure") })
	reader.WhenReleased(func(releaseErr error) {
		if releaseErr == nil {
			t.Error("the release hook must receive the close error")
		}
		_ = reader.Close()
		ran.Add(1)
	})
	reader.WhenReleased(func(error) { ran.Add(1) })
	done := make(chan error, 1)
	go func() { done <- reader.Close() }()
	select {
	case closeErr := <-done:
		if closeErr == nil {
			t.Fatal("Close performing the release must return its error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a release hook calling Close deadlocked")
	}
	if ran.Load() != 2 {
		t.Fatalf("%d of the 2 non-panicking hooks ran", ran.Load())
	}
	late := make(chan error, 1)
	reader.WhenReleased(func(releaseErr error) { late <- releaseErr })
	if releaseErr := <-late; releaseErr == nil {
		t.Fatal("a hook registered after the release runs at once with its error")
	}
}

// TestPointReadAfterReleaseKeepsNothing builds a Reader's structures, releases
// it, and checks that point reads racing the release leave nothing stored on
// it.
func TestPointReadAfterReleaseKeepsNothing(t *testing.T) {
	directory := t.TempDir()
	reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, "segment.seg", 3000))
	if openErr != nil {
		t.Fatal(openErr)
	}
	storedReader, readerErr := reader.storedReader(0)
	if readerErr != nil {
		t.Fatal(readerErr)
	}
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if _, dictionaryErr := storedReader.dictionary(identifierField); !errors.Is(dictionaryErr, ErrReaderClosed) {
		t.Fatalf("building a dictionary on a released Reader = %v, want ErrReaderClosed", dictionaryErr)
	}
	if _, filterErr := storedReader.buildTermBloom(identifierField); !errors.Is(filterErr, ErrReaderClosed) {
		t.Fatalf("building a filter on a released Reader = %v, want ErrReaderClosed", filterErr)
	}
	kept := 0
	for _, stored := range []*sync.Map{&storedReader.dictionaries, &storedReader.termFilters, &storedReader.termSets} {
		stored.Range(func(any, any) bool { kept++; return true })
	}
	if kept != 0 {
		t.Fatalf("%d structures kept on a released Reader", kept)
	}
	if _, rebuildErr := reader.storedReader(0); !errors.Is(rebuildErr, ErrReaderClosed) {
		t.Fatalf("rebuilding a stored reader after release = %v, want ErrReaderClosed", rebuildErr)
	}
}

// TestOpenPromotedSegmentAdoptsFromTheFileItWrote promotes an admitted
// segment to the file it was written to, adopting the filters it built, and
// adopts nothing when the identity given is another file's.
func TestOpenPromotedSegmentAdoptsFromTheFileItWrote(t *testing.T) {
	directory := t.TempDir()
	path := encodeSegmentFile(t, directory, "segment.seg", 3000)
	payload, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	admitted, admitErr := OpenSegment(payload)
	if admitErr != nil {
		t.Fatal(admitErr)
	}
	defer func() { _ = admitted.Close() }()
	if prepareErr := admitted.PrepareTermFilter(identifierField); prepareErr != nil {
		t.Fatal(prepareErr)
	}
	metadata := admitted.SnapshotMetadata().Segments[0]
	written, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	from, _ := admitted.storedReader(0)
	built, _ := from.termFilters.Load(identifierField)
	promoted, openErr := OpenPromotedSegment(path, metadata, &Promotion{source: admitted, written: written})
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = promoted.Close() }()
	to, _ := promoted.storedReader(0)
	if adopted, found := to.termFilters.Load(identifierField); !found || adopted != built {
		t.Fatal("the promoted reader must adopt the admitted reader's identifier filter")
	}
	requireIdentifier(t, promoted, "series-001234")
	copyPath := filepath.Join(directory, "copy.seg")
	if writeErr := os.WriteFile(copyPath, payload, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	other, _ := os.Stat(copyPath)
	unrelated, openErr := OpenPromotedSegment(path, metadata, &Promotion{source: admitted, written: other})
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = unrelated.Close() }()
	unrelatedReader, _ := unrelated.storedReader(0)
	if _, adopted := unrelatedReader.termFilters.Load(identifierField); adopted {
		t.Fatal("a reader of a file other than the one written must adopt nothing")
	}
	requireIdentifier(t, unrelated, "series-001234")
}

// TestOpenFailsWhenTheFileCannotBeDescribed makes describing a just-opened
// segment file fail: the open fails, leaving no descriptor behind.
func TestOpenFailsWhenTheFileCannotBeDescribed(t *testing.T) {
	path := encodeSegmentFile(t, t.TempDir(), "segment.seg", 100)
	previous := statSegmentFile
	statSegmentFile = func(fs.File) (os.FileInfo, error) { return nil, errors.New("injected stat failure") }
	t.Cleanup(func() { statSegmentFile = previous })
	before := openFileDescriptorCount(t)
	if _, openErr := OpenSegmentFile(path); openErr == nil {
		t.Fatal("opening a segment whose file cannot be described must fail")
	}
	if open := openFileDescriptorCount(t) - before; open != 0 {
		t.Fatalf("a failed open left %d descriptors open", open)
	}
}

// TestPublishRefusesAPromotionOfAnotherPayload publishes a payload with a
// promotion that follows a different segment.
func TestPublishRefusesAPromotionOfAnotherPayload(t *testing.T) {
	payload, readErr := os.ReadFile(encodeSegmentFile(t, t.TempDir(), "segment.seg", 100))
	if readErr != nil {
		t.Fatal(readErr)
	}
	other, otherErr := os.ReadFile(encodeSegmentFile(t, t.TempDir(), "other.seg", 101))
	if otherErr != nil {
		t.Fatal(otherErr)
	}
	admitted, admitErr := OpenSegment(other)
	if admitErr != nil {
		t.Fatal(admitErr)
	}
	defer func() { _ = admitted.Close() }()
	source, sourceErr := OpenSegment(payload)
	if sourceErr != nil {
		t.Fatal(sourceErr)
	}
	metadata := source.SnapshotMetadata().Segments[0]
	_ = source.Close()
	metadata.ID = 1
	if publishErr := PublishSnapshot(t.TempDir(), 1, []SnapshotSegmentPayload{
		{SnapshotSegment: metadata, Payload: payload, Promotion: NewPromotion(admitted)},
	}); publishErr == nil {
		t.Fatal("a promotion must only follow its own segment's payload")
	}
}

// TestReadStrictSnapshotMetadataChecksHeldSegments reads a directory's
// manifest metadata, checking held segments by identity and validating the
// others, without taking descriptors from the budget.
func TestReadStrictSnapshotMetadataChecksHeldSegments(t *testing.T) {
	directory := t.TempDir()
	source := encodeSegmentFile(t, t.TempDir(), "source.seg", 20)
	payload, readErr := os.ReadFile(source)
	if readErr != nil {
		t.Fatal(readErr)
	}
	reader, openErr := OpenSegment(payload)
	if openErr != nil {
		t.Fatal(openErr)
	}
	segment := reader.SnapshotMetadata().Segments[0]
	_ = reader.Close()
	first, second := segment, segment
	first.ID, second.ID = 1, 2
	if publishErr := PublishSnapshot(directory, 1, []SnapshotSegmentPayload{
		{SnapshotSegment: first, Payload: payload}, {SnapshotSegment: second, Payload: payload},
	}); publishErr != nil {
		t.Fatal(publishErr)
	}
	firstPath := filepath.Join(directory, "000000000001.seg")
	heldReader, heldErr := OpenSnapshotSegment(firstPath, first)
	if heldErr != nil {
		t.Fatal(heldErr)
	}
	defer func() { _ = heldReader.Close() }()
	held := func(id uint64) *Reader {
		if id == 1 {
			return heldReader
		}
		return nil
	}
	baseline := openFileDescriptorCount(t)
	metadata, metadataErr := ReadStrictSnapshotMetadata(directory, held)
	if metadataErr != nil || len(metadata.Segments) != 2 {
		t.Fatalf("metadata %+v, err %v", metadata, metadataErr)
	}
	if open := openFileDescriptorCount(t); open != baseline {
		t.Fatalf("reading metadata left %d descriptors open", open-baseline)
	}
	// Corrupt the segment that is not held: validation must catch it.
	secondPath := filepath.Join(directory, "000000000002.seg")
	if truncateErr := os.Truncate(secondPath, 8); truncateErr != nil {
		t.Fatal(truncateErr)
	}
	if _, corruptErr := ReadStrictSnapshotMetadata(directory, held); !errors.Is(corruptErr, ErrCorrupt) {
		t.Fatalf("a damaged segment not held = %v, want ErrCorrupt", corruptErr)
	}
}

type blockingWriter struct {
	blocked chan struct{}
	release chan struct{}
	once    bool
}

func (w *blockingWriter) Write(data []byte) (int, error) {
	if !w.once {
		w.once = true
		close(w.blocked)
		<-w.release
	}
	return len(data), nil
}

func TestSnapshotSourceReaderMustMatchItsMetadata(t *testing.T) {
	directory := t.TempDir()
	reader, openErr := OpenSegmentFile(encodeSegmentFile(t, directory, "source.seg", 10))
	if openErr != nil {
		t.Fatal(openErr)
	}
	metadata := reader.SnapshotMetadata().Segments[0]
	metadata.ID = 3
	wrongCount := metadata
	wrongCount.DocumentCount++
	if publishErr := PublishSnapshot(t.TempDir(), 1, []SnapshotSegmentPayload{{SnapshotSegment: wrongCount, Source: reader}}); !errors.Is(publishErr, ErrCorrupt) {
		t.Fatalf("publishing a source whose document count differs = %v, want ErrCorrupt", publishErr)
	}
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	publishErr := PublishSnapshot(t.TempDir(), 1, []SnapshotSegmentPayload{{SnapshotSegment: metadata, Source: reader}})
	if !errors.Is(publishErr, ErrReaderClosed) || errors.Is(publishErr, ErrCorrupt) {
		t.Fatalf("copying from a closed source = %v, want ErrReaderClosed and not ErrCorrupt", publishErr)
	}
}
