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
	"fmt"
	"path/filepath"
	"plugin"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"

	pipelinev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/pipeline/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

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

// pluginCache caches loaded .so plugins by (path, symbol) to avoid re-opening.
// Go plugins cannot be unloaded; we re-use cached ones.
var pluginCache = &loadedPluginCache{
	m: make(map[pluginCacheKey]sdk.Sampler),
}

type pluginCacheKey struct {
	path   string
	symbol string
}

type loadedPluginCache struct {
	m  map[pluginCacheKey]sdk.Sampler
	mu sync.Mutex
}

// loadSamplerPlugin loads (or returns cached) a sampler plugin from the given SamplerPlugin config.
// trustedDir is the only directory .so paths may resolve within; empty trustedDir disables loading.
func loadSamplerPlugin(sp *pipelinev1.SamplerPlugin, trustedDir string) (_ sdk.Sampler, retErr error) {
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

	key := pluginCacheKey{path: resolvedPath, symbol: symbol}
	pluginCache.mu.Lock()
	defer pluginCache.mu.Unlock()
	if cached, ok := pluginCache.m[key]; ok {
		return cached, nil
	}

	if sp.GetAbiVersion() != uint32(sdk.ABIVersion) {
		return nil, fmt.Errorf("plugin ABI version %d does not match engine ABI version %d",
			sp.GetAbiVersion(), sdk.ABIVersion)
	}

	var p *plugin.Plugin
	var openErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				openErr = fmt.Errorf("panic opening plugin %q: %v", resolvedPath, r)
			}
		}()
		p, openErr = plugin.Open(resolvedPath)
	}()
	if openErr != nil {
		return nil, fmt.Errorf("cannot open plugin %q: %w", resolvedPath, openErr)
	}

	abiSym, lookupErr := p.Lookup("ABIVersion")
	if lookupErr != nil {
		return nil, fmt.Errorf("plugin %q missing ABIVersion symbol: %w", resolvedPath, lookupErr)
	}
	pluginABI, ok := abiSym.(*int)
	if !ok {
		return nil, fmt.Errorf("plugin %q ABIVersion has wrong type", resolvedPath)
	}
	if *pluginABI != sdk.ABIVersion {
		return nil, fmt.Errorf("plugin %q ABI version %d does not match engine %d", resolvedPath, *pluginABI, sdk.ABIVersion)
	}

	ctorSym, lookupErr := p.Lookup(symbol)
	if lookupErr != nil {
		return nil, fmt.Errorf("plugin %q missing symbol %q: %w", resolvedPath, symbol, lookupErr)
	}
	ctor, ok := ctorSym.(func([]byte) (sdk.Sampler, error))
	if !ok {
		return nil, fmt.Errorf("plugin %q symbol %q has wrong type", resolvedPath, symbol)
	}

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

	var sampler sdk.Sampler
	var ctorErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				ctorErr = fmt.Errorf("panic in plugin %q constructor: %v", resolvedPath, r)
			}
		}()
		sampler, ctorErr = ctor(cfgJSON)
	}()
	if ctorErr != nil {
		return nil, fmt.Errorf("plugin %q constructor failed: %w", resolvedPath, ctorErr)
	}
	if sampler == nil {
		return nil, fmt.Errorf("plugin %q constructor returned nil sampler", resolvedPath)
	}

	pluginCache.m[key] = sampler
	return sampler, nil
}
