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

package trace

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// computeConfigHash returns the sha256 hex of the protojson-marshaled
// SamplerPlugin.config (or of "{}" when config is nil). It mirrors the hash
// used inside loadSamplerPlugin for the pluginCacheKey so callers can build
// namedSampler entries without re-computing the hash.
func computeConfigHash(sp *commonv1.SamplerPlugin) string {
	var cfgJSON []byte
	if cfg := sp.GetConfig(); cfg != nil {
		var marshalErr error
		cfgJSON, marshalErr = protojson.Marshal(cfg)
		if marshalErr != nil {
			cfgJSON = []byte("{}")
		}
	} else {
		cfgJSON = []byte("{}")
	}
	hashBytes := sha256.Sum256(cfgJSON)
	return hex.EncodeToString(hashBytes[:])
}

// pathWithin reports whether target is base itself or a descendant of base.
// It treats only a bare ".." or a ".."+separator prefix as an escape, so a
// legitimate child whose first segment merely starts with ".." (e.g. "..foo")
// is not falsely rejected.
func pathWithin(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// pluginCache caches loaded .so plugins by (path, symbol, configHash) to avoid
// re-opening the same .so with the same config. Go plugins cannot be unloaded;
// we re-use cached instances. A different config yields a different key and
// triggers a fresh ABI-checked construction.
var pluginCache = &loadedPluginCache{
	m: make(map[pluginCacheKey]sdk.Sampler),
}

type pluginCacheKey struct {
	path       string
	symbol     string
	configHash string // sha256 hex of the protojson-marshaled config (or of "{}" when nil)
	group      string // "" for a shared (non-HostAware) instance; group name for a per-group HostAware instance
}

type loadedPluginCache struct {
	m  map[pluginCacheKey]sdk.Sampler
	mu sync.Mutex
}

// loadSamplerPlugin loads (or returns cached) a sampler plugin from the given SamplerPlugin config.
// trustedDir is the only directory .so paths may resolve within; empty trustedDir disables loading.
// group scopes a HostAware instance to one group so its injected telemetry is per-group; a
// non-HostAware sampler is cached under the shared key (group=="") and re-used across groups.
// bindHost, when non-nil, is invoked EXACTLY ONCE on a freshly constructed HostAware sampler,
// under pluginCache.mu and BEFORE the instance is stored, so no concurrent cache-hit can hand a
// not-yet-hosted instance to a merge. It is never called on a cache hit. bindHost may be nil.
func loadSamplerPlugin(sp *commonv1.SamplerPlugin, trustedDir, group string, bindHost func(sdk.Sampler)) (_ sdk.Sampler, retErr error) {
	if trustedDir == "" {
		return nil, fmt.Errorf("trusted plugin dir not configured")
	}
	rawPath := sp.GetPath()
	if rawPath == "" {
		return nil, fmt.Errorf("plugin path is empty")
	}
	resolvedPath := filepath.Clean(filepath.Join(trustedDir, rawPath))
	cleanTrusted := filepath.Clean(trustedDir)
	if !pathWithin(cleanTrusted, resolvedPath) {
		return nil, fmt.Errorf("plugin path %q escapes trusted directory %q", rawPath, trustedDir)
	}
	// Defense-in-depth: the lexical check above cannot see through symlinks, so a
	// symlink inside the trusted dir could otherwise redirect the load outside it.
	// Resolve symlinks and re-check containment. EvalSymlinks requires the targets
	// to exist; if either cannot be resolved we keep the lexical result and let the
	// subsequent plugin.Open surface any real error.
	if realTrusted, trustedErr := filepath.EvalSymlinks(cleanTrusted); trustedErr == nil {
		if realPath, pathErr := filepath.EvalSymlinks(resolvedPath); pathErr == nil && !pathWithin(realTrusted, realPath) {
			return nil, fmt.Errorf("plugin path %q escapes trusted directory %q after symlink resolution", rawPath, trustedDir)
		}
	}

	symbol := sp.GetSymbol()
	if symbol == "" {
		symbol = "NewSampler"
	}

	// Marshal config before the cache lookup so the hash is part of the key.
	// protojson map-key ordering is deterministic for google.protobuf.Struct
	// (keys are sorted lexicographically by the protojson encoder), so hashing
	// the raw output is stable across calls with the same logical config.
	// A nil config hashes the canonical empty JSON object "{}".
	var cfgJSON []byte
	if cfg := sp.GetConfig(); cfg != nil {
		var marshalErr error
		cfgJSON, marshalErr = protojson.Marshal(cfg)
		if marshalErr != nil {
			return nil, fmt.Errorf("cannot marshal plugin config: %w", marshalErr)
		}
	} else {
		cfgJSON = []byte("{}")
	}
	hashBytes := sha256.Sum256(cfgJSON)
	configHash := hex.EncodeToString(hashBytes[:])

	key := pluginCacheKey{path: resolvedPath, symbol: symbol, configHash: configHash}
	pluginCache.mu.Lock()
	defer pluginCache.mu.Unlock()
	// 3-step lookup, all under pluginCache.mu:
	//  1. shared key (group=="") — a non-HostAware instance re-used across groups.
	//  2. group key — an already-hosted HostAware instance; return WITHOUT calling bindHost.
	//  3. miss — fall through to construct (ABI check, plugin.Open, ctor) under the lock.
	if cached, ok := pluginCache.m[key]; ok {
		return cached, nil
	}
	groupKey := key
	groupKey.group = group
	if cached, ok := pluginCache.m[groupKey]; ok {
		return cached, nil
	}

	if sp.GetAbiVersion() != uint32(sdk.ABIVersion) {
		return nil, fmt.Errorf("plugin ABI version %d does not match engine ABI version %d",
			sp.GetAbiVersion(), sdk.ABIVersion)
	}

	sampler, err := newSamplerFromPlugin(resolvedPath, symbol, cfgJSON)
	if err != nil {
		return nil, err
	}

	// Classify the freshly constructed instance and key it accordingly. For a
	// HostAware sampler, inject the host via bindHost while still holding
	// pluginCache.mu and BEFORE storing, so no concurrent cache-hit can observe a
	// not-yet-hosted instance. UseHost/telemetry setup thus runs under the same
	// lock that already guards plugin.Open and the constructor; keep it fast.
	if _, isHostAware := sampler.(sdk.HostAware); isHostAware {
		key.group = group
		if bindHost != nil {
			bindHost(sampler)
		}
	}
	pluginCache.m[key] = sampler
	return sampler, nil
}

// newSamplerFromPlugin opens the .so, verifies its ABIVersion symbol, and calls
// the constructor. It is a package var so tests can substitute a Go sampler and
// exercise the cache/keying/bindHost logic under -race without a real .so. The
// default implementation delegates to sdk.OpenSampler — the single shared
// loader-contract implementation also used by sdktest.LoadSO — so the host and
// the offline dev toolkit never drift.
var newSamplerFromPlugin = sdk.OpenSampler
