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

// Package resume holds what the resumable export and import share: the process lock, the
// atomic progress-file and side-file writers and the persister goroutine that owns the
// progress file, the binding digests and their differences, the resume summary and the
// operator's confirmation.
package resume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// Header is the part every progress file starts with: what wrote it, in which version.
type Header struct {
	Kind    string `json:"kind"`
	Version int    `json:"version"`
}

// header lets Read find the Header of a progress state that embeds it.
func (h Header) header() Header { return h }

// Exists reports whether a progress file is at path. Any error but "does not exist" counts as
// present, so the caller reads it and reports the error instead of starting over it.
func Exists(path string) bool {
	_, err := os.Lstat(path)
	return !errors.Is(err, os.ErrNotExist)
}

// VersionError is a progress file of the kind a command resumes that another version of bydbctl
// wrote: Found is the version of its format, Want the one this command reads. Read wraps it in
// an exitcode.ExitResume error, so that the command can tell the file from any other unusable
// one and say how to go on.
type VersionError struct {
	Path, Kind  string
	Found, Want int
}

func (e *VersionError) Error() string {
	return fmt.Sprintf("progress file %s is a %q version %d file; this command resumes %q version %d", e.Path, e.Kind, e.Found, e.Kind, e.Want)
}

// Read decodes the progress file at path into v after checking its kind and version. A file
// that cannot be read or parsed, or that another command or version wrote, exits
// exitcode.ExitResume: nothing is deleted. A file of another version is a *VersionError.
func Read(path, kind string, version int, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return exitcode.Exit(exitcode.ExitResume, "progress file %s: %w", path, err)
	}
	// A v that embeds the Header is decoded once; the Header alone is decoded only when v has none
	// or does not decode, so that a file of another kind or version is named as such.
	decodeErr := json.Unmarshal(data, v)
	h, ok := v.(interface{ header() Header })
	var hdr Header
	if decodeErr == nil && ok {
		hdr = h.header()
	} else if err = json.Unmarshal(data, &hdr); err != nil {
		return exitcode.Exit(exitcode.ExitResume, "progress file %s is unusable: %w", path, err)
	}
	if hdr.Kind == kind && hdr.Version != version {
		return exitcode.Exit(exitcode.ExitResume, "%w", &VersionError{Path: path, Kind: kind, Found: hdr.Version, Want: version})
	}
	if hdr.Kind != kind {
		return exitcode.Exit(exitcode.ExitResume, "progress file %s is a %q version %d file; this command resumes %q version %d",
			path, hdr.Kind, hdr.Version, kind, version)
	}
	if decodeErr != nil {
		return exitcode.Exit(exitcode.ExitResume, "progress file %s is unusable: %w", path, decodeErr)
	}
	return nil
}

// WriteFile writes v as indented JSON to path atomically: a temporary file with a unique name
// next to it (TempPattern) is written with mode 0600 and fsynced, renamed over path, and the
// directory is fsynced, so a crash leaves either the old or the new file, never a torn one, and
// two writers never share a temporary file.
func WriteFile(path string, v any) error {
	_, err := writeJSON(path, v)
	return err
}

// writeJSON is WriteFile returning the bytes written.
func writeJSON(path string, v any) ([]byte, error) {
	data, err := EncodeJSON(v)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", path, err)
	}
	if err = writeAtomic(path, data, true); err != nil {
		return nil, fmt.Errorf("write %s: %w", path, err)
	}
	return data, nil
}

// EncodeJSON is v as JSON indented by two spaces and ending in a newline, what WriteFile and
// WriteSide write.
func EncodeJSON(v any) ([]byte, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// ReplaceFileUnsynced replaces the file at path with data like WriteFile, through a temporary
// file named as one of path's (IsTemp), but fsyncs neither the file nor the directory: a process
// killed meanwhile leaves the old file or the new one, never a torn one, while a power loss may
// lose it.
func ReplaceFileUnsynced(path string, data []byte) error {
	return writeAtomic(path, data, false)
}

// RemoveFile removes the file at path; on Windows it tries again for a moment while another
// process has the file open.
func RemoveFile(path string) error { return removeFile(path) }

func writeAtomic(path string, data []byte, sync bool) error {
	f, err := os.CreateTemp(filepath.Dir(path), TempPattern(path))
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, err = f.Write(data)
	if err == nil && sync {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = renameFile(tmp, path)
	}
	if err == nil && sync {
		err = SyncDir(filepath.Dir(path))
	}
	if err != nil {
		_ = os.Remove(tmp)
	}
	return err
}

// retryInUse runs op, trying again for up to inUseRetryFor while retryable says the error is one
// that goes away: on Windows another process that has the file open for a moment (a dry run
// reading the progress file, a virus scanner) makes a rename over it or its removal fail until
// that process closes it.
func retryInUse(op func() error, retryable func(error) bool) error {
	deadline := time.Now().Add(inUseRetryFor)
	wait := time.Millisecond
	for {
		err := op()
		if err == nil || !retryable(err) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(wait)
		wait = min(2*wait, 50*time.Millisecond)
	}
}

// inUseRetryFor bounds how long retryInUse tries again; a test shortens it.
var inUseRetryFor = 2 * time.Second

// MkdirAll is os.MkdirAll that also fsyncs the parent of every directory it creates, so a new
// directory does not vanish in a crash while a file below it, already fsynced, is recorded as
// written.
func MkdirAll(dir string, perm os.FileMode) error {
	info, err := os.Stat(dir)
	if err == nil {
		if !info.IsDir() {
			return &os.PathError{Op: "mkdir", Path: dir, Err: syscall.ENOTDIR}
		}
		return nil
	}
	if parent := filepath.Dir(dir); parent != dir {
		if err = MkdirAll(parent, perm); err != nil {
			return err
		}
	}
	if err = os.Mkdir(dir, perm); err != nil {
		// Another goroutine or process created it meanwhile: fsync its entry all the same, the
		// caller may record a file below it before that one did.
		if info, statErr := os.Stat(dir); statErr != nil || !info.IsDir() {
			return err
		}
	}
	return SyncDir(filepath.Dir(dir))
}

// TempPattern is the os.CreateTemp pattern of the temporary files WriteFile writes path
// through: ".<base>.*.tmp" in the directory of path.
func TempPattern(path string) string {
	return "." + filepath.Base(path) + ".*.tmp"
}

// IsTemp reports whether name, a base name in the directory of path, is a temporary file of
// WriteFile for path: ".<base>.<digits>.tmp", the digits being what os.CreateTemp puts in. The
// temporary file of another path whose name starts with the same base, such as "<base>.v2", is
// not one of path's.
func IsTemp(path, name string) bool {
	prefix := "." + filepath.Base(path) + "."
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".tmp") || len(name) <= len(prefix)+len(".tmp") {
		return false
	}
	for _, c := range name[len(prefix) : len(name)-len(".tmp")] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// RemoveTemps deletes the temporary files a crashed WriteFile left of the progress file at path
// and of its side files named by sides (SidePath), scanning the directory once.
func RemoveTemps(path string, sides ...string) error {
	return removeTemps(filepath.Dir(path), withSides(path, sides))
}

// withSides is path followed by the paths of its side files named by sides.
func withSides(path string, sides []string) []string {
	paths := []string{path}
	for _, s := range sides {
		paths = append(paths, SidePath(path, s))
	}
	return paths
}

// removeTemps deletes the temporary files of every one of paths, all in dir, scanning it once.
func removeTemps(dir string, paths []string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove the temporary files of %s: %w", paths[0], err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() || !slices.ContainsFunc(paths, func(p string) bool { return IsTemp(p, e.Name()) }) {
			continue
		}
		if err = os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", e.Name(), err)
		}
	}
	return nil
}

// SidePath is the side file of the progress file at path: "<path without .json>-<name>.json", so
// "x/.export-progress.json" has "x/.export-progress-plan.json"; a path that does not end in
// ".json" just gets "-<name>.json".
func SidePath(path, name string) string {
	return strings.TrimSuffix(path, ".json") + "-" + name + ".json"
}

// sideSuffixes are the endings of the side files of the progress files of this package's users.
var sideSuffixes = []string{"-plan.json", "-cursor.json"}

// CheckSideCollision refuses a progress file path that ends in the name of a side file
// ("-plan.json", "-cursor.json"): its own side file's path or that of another progress file would
// be it, and one would overwrite the other.
func CheckSideCollision(path string) error {
	for _, suffix := range sideSuffixes {
		if strings.HasSuffix(path, suffix) {
			return fmt.Errorf("%s ends in %s, the name of a side file of a progress file; give it another name", path, suffix)
		}
	}
	return nil
}

// WriteSide writes v once as indented JSON to the side file at side, atomically like WriteFile,
// and returns the digest (DigestBytes) of what it wrote, which the progress file records so
// that ReadSide finds out a side file that is not the one written with it.
func WriteSide(side string, v any) (string, error) {
	data, err := writeJSON(side, v)
	if err != nil {
		return "", err
	}
	return DigestBytes(data), nil
}

// EncodeSide is what WriteSide writes for v and its digest, computed without writing anything:
// what a progress file announces before it rewrites a side file with WriteSideBytes, so that a
// crash in between leaves a side file one of its digests names.
func EncodeSide(v any) (data []byte, digest string, err error) {
	if data, err = EncodeJSON(v); err != nil {
		return nil, "", fmt.Errorf("encode: %w", err)
	}
	return data, DigestBytes(data), nil
}

// WriteSideBytes writes data, from EncodeSide, to the side file at side atomically like WriteSide.
func WriteSideBytes(side string, data []byte) error {
	if err := writeAtomic(side, data, true); err != nil {
		return fmt.Errorf("write %s: %w", side, err)
	}
	return nil
}

// ReadSide decodes the side file at side into v after checking it against digest, what
// WriteSide returned. A side file that is missing, cannot be read or parsed, or has another
// digest exits exitcode.ExitResume: nothing is deleted.
func ReadSide(side, digest string, v any) error {
	data, err := os.ReadFile(side)
	if err != nil {
		return exitcode.Exit(exitcode.ExitResume, "side file %s of the progress file: %w", side, err)
	}
	if got := DigestBytes(data); got != digest {
		if !json.Valid(data) {
			return exitcode.Exit(exitcode.ExitResume, "side file %s is empty or incomplete (%d bytes, not valid JSON; a crash or a full disk cut it short)", side, len(data))
		}
		return exitcode.Exit(exitcode.ExitResume, "side file %s is not the one its progress file was written with: recorded %s, now %s", side, digest, got)
	}
	if err = json.Unmarshal(data, v); err != nil {
		return exitcode.Exit(exitcode.ExitResume, "side file %s is unusable: %w", side, err)
	}
	return nil
}

// Remove deletes the progress file, the side files named by sides (SidePath), the temporary
// files a crash left of any of them, and fsyncs the directory. A file or directory that is
// already gone is not an error. The lock file is removed by its unlock.
func Remove(path string, sides ...string) error {
	paths := withSides(path, sides)
	if err := removeTemps(filepath.Dir(path), paths); err != nil {
		return err
	}
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", p, err)
		}
	}
	if err := SyncDir(filepath.Dir(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", path, err)
	}
	return nil
}

// ConfirmResume decides whether a run resumes the progress file at path, after its
// summary was printed. resumeFlag (--resume, or its environment variable) continues without asking;
// otherwise ask, set only when the operator is at a terminal, asks "Resume? [y/N]"; without it
// the run stops. startOver tells the operator how to begin again instead of resuming ("" leaves
// it out) and ends both refusals. A refusal exits exitcode.ExitUsage and leaves everything in
// place; an interrupt (ctx ending) while asked exits exitcode.ExitRuntime.
func ConfirmResume(ctx context.Context, resumeFlag bool, ask func(context.Context, string) bool, path, startOver string) error {
	if resumeFlag {
		return nil
	}
	way := ""
	if startOver != "" {
		way = "; " + startOver
	}
	if ask == nil {
		return exitcode.Exit(exitcode.ExitUsage, "a previous run left %s; add --resume to continue it (needed when not at a terminal)%s", path, way)
	}
	if ask(ctx, "Resume? [y/N] ") {
		return nil
	}
	if ctx.Err() != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "interrupted at the resume question: %w; %s and the files it records are left in place", context.Cause(ctx), path)
	}
	return exitcode.Exit(exitcode.ExitUsage, "not resumed; %s and the files it records are left in place%s", path, way)
}
