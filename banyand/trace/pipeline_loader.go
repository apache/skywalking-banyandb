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
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	runtime "github.com/apache/skywalking-banyandb/banyand/internal/pipeline"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

var pluginCache = func() *runtime.Loader {
	loader := runtime.NewLoader()
	loader.OpenSampler = func(path, symbol string, config []byte) (sdk.Sampler, error) {
		return newSamplerFromPlugin(path, symbol, config)
	}
	return loader
}()
var newSamplerFromPlugin = sdk.OpenSampler

func computeConfigHash(sp *commonv1.SamplerPlugin) string { return runtime.ConfigHash(sp) }

func loadSamplerPlugin(sp *commonv1.SamplerPlugin, trustedDir, group string, bindHost func(sdk.Sampler)) (sdk.Sampler, error) {
	return pluginCache.LoadSamplerPlugin(sp, trustedDir, group, bindHost)
}
