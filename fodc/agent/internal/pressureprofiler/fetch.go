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

package pressureprofiler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
)

// fetchProfile streams a pprof endpoint's body straight to dest, never buffering the
// whole profile in memory. It returns the number of bytes written.
func fetchProfile(ctx context.Context, client *http.Client, url, dest string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to build pprof request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch pprof %q: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("pprof %q returned status %d", url, resp.StatusCode)
	}

	f, err := os.Create(dest) //nolint:gosec // dest is built under the store's private directory
	if err != nil {
		return 0, fmt.Errorf("failed to create profile file %q: %w", dest, err)
	}
	n, copyErr := io.Copy(f, resp.Body)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(dest)
		return 0, fmt.Errorf("failed to write profile %q: %w", dest, copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(dest)
		return 0, fmt.Errorf("failed to flush profile %q: %w", dest, closeErr)
	}
	return n, nil
}
