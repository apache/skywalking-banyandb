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

package sdk

import (
	"fmt"
	"plugin"
)

// OpenSampler opens the .so at path, verifies its exported ABIVersion symbol
// matches this package's ABIVersion, looks up the constructor symbol, and
// calls it with cfg (the canonical JSON encoding of the plugin's config).
//
// This is the ONE loader-contract implementation: banyand/trace's host loader
// delegates here, pkg/pipeline/sdk/sdktest.LoadSO calls it directly to drive a
// real .so offline, and any future toolchain-authoritative CLI validate/decide
// command would call it too — so every caller runs the exact same code path,
// never a hand-rolled mirror that can silently drift from the host.
//
// A panic while opening the plugin or while running its constructor is
// recovered and returned as an error; OpenSampler itself never panics.
func OpenSampler(path, symbol string, cfg []byte) (Sampler, error) {
	var p *plugin.Plugin
	var openErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				openErr = fmt.Errorf("panic opening plugin %q: %v", path, r)
			}
		}()
		p, openErr = plugin.Open(path)
	}()
	if openErr != nil {
		return nil, fmt.Errorf("cannot open plugin %q: %w", path, openErr)
	}

	abiSym, lookupErr := p.Lookup("ABIVersion")
	if lookupErr != nil {
		return nil, fmt.Errorf("plugin %q missing ABIVersion symbol: %w", path, lookupErr)
	}
	pluginABI, ok := abiSym.(*int)
	if !ok {
		return nil, fmt.Errorf("plugin %q ABIVersion has wrong type", path)
	}
	if *pluginABI != ABIVersion {
		return nil, fmt.Errorf("plugin %q ABI version %d does not match engine %d", path, *pluginABI, ABIVersion)
	}

	ctorSym, lookupErr := p.Lookup(symbol)
	if lookupErr != nil {
		return nil, fmt.Errorf("plugin %q missing symbol %q: %w", path, symbol, lookupErr)
	}
	ctor, ok := ctorSym.(func([]byte) (Sampler, error))
	if !ok {
		return nil, fmt.Errorf("plugin %q symbol %q has wrong type", path, symbol)
	}

	var sampler Sampler
	var ctorErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				ctorErr = fmt.Errorf("panic in plugin %q constructor: %v", path, r)
			}
		}()
		sampler, ctorErr = ctor(cfg)
	}()
	if ctorErr != nil {
		return nil, fmt.Errorf("plugin %q constructor failed: %w", path, ctorErr)
	}
	if sampler == nil {
		return nil, fmt.Errorf("plugin %q constructor returned nil sampler", path)
	}
	return sampler, nil
}
