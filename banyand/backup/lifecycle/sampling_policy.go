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

package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/pipeline"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

type samplingHostConfig struct {
	TrustedDirectory string
	ArtifactRoot     string
	Timeout          time.Duration
	CircuitBreakN    int
	Enabled          bool
}

type pinnedSamplingPolicy struct {
	schema        *databasev1.Trace
	config        *commonv1.TracePipelineConfig
	session       *pipeline.Session
	digest        string
	plugins       []*commonv1.Plugin
	binaryDigests []string
	samplers      []sdk.Sampler
	host          samplingHostConfig
	closeOnce     sync.Once
}

func sourceSamplingChain(config *commonv1.TracePipelineConfig, source, name string) ([]*commonv1.Plugin, error) {
	if config == nil || !config.Enabled {
		return nil, nil
	}
	if len(config.SchemaNames) > 0 || config.SchemaNameRegex != "" {
		matches := false
		for _, selected := range config.SchemaNames {
			matches = matches || selected == name
		}
		if config.SchemaNameRegex != "" {
			expression, err := regexp.Compile(config.SchemaNameRegex)
			if err != nil {
				return nil, fmt.Errorf("invalid sampling schema selector: %w", err)
			}
			matches = matches || expression.MatchString(name)
		}
		if !matches {
			return nil, nil
		}
	}
	if source == "" {
		return nil, fmt.Errorf("sampling source stage identity is missing")
	}
	var result []*commonv1.Plugin
	found := false
	for _, stage := range config.Stages {
		if stage.Stage != source {
			continue
		}
		if found {
			return nil, fmt.Errorf("multiple sampling rules for source stage %q", source)
		}
		found = true
		result = stage.Plugins
	}
	return result, nil
}

func resolveSamplingPolicy(ctx context.Context, group *GroupConfig, md metadata.Repo) (*pinnedSamplingPolicy, error) {
	config := group.GetPipeline()
	if config == nil || !config.Enabled {
		return nil, nil
	}
	// Resolve only when a nonempty source chain exists, keeping legacy migrations untouched.
	applicable := false
	for _, stage := range config.Stages {
		applicable = applicable || stage.Stage == group.SourceStage && len(stage.Plugins) > 0
	}
	if group.SourceStage == "" && len(config.Stages) > 0 {
		return nil, fmt.Errorf("sampling source stage identity is missing")
	}
	if !applicable {
		return nil, nil
	}
	traces, err := md.TraceRegistry().ListTrace(ctx, schema.ListOpt{Group: group.Metadata.Name})
	if err != nil {
		return nil, fmt.Errorf("resolve sampling trace schema: %w", err)
	}
	if len(traces) != 1 {
		return nil, fmt.Errorf("sampling group %s has %d trace schemas; exactly one is required", group.Metadata.Name, len(traces))
	}
	plugins, err := sourceSamplingChain(config, group.SourceStage, traces[0].Metadata.Name)
	if err != nil || len(plugins) == 0 {
		return nil, err
	}
	if !group.SamplingHost.Enabled {
		return nil, fmt.Errorf("source-stage sampling configured but lifecycle plugin host is disabled")
	}
	policy := &pinnedSamplingPolicy{
		schema: proto.Clone(traces[0]).(*databasev1.Trace),
		config: proto.Clone(config).(*commonv1.TracePipelineConfig), host: group.SamplingHost,
	}
	if err = config.ValidateAll(); err != nil {
		return nil, fmt.Errorf("invalid migration sampling chain: %w", err)
	}
	for _, plugin := range plugins {
		samplerConfig := plugin.GetSampler()
		if samplerConfig == nil {
			policy.close()
			return nil, fmt.Errorf("unsupported migration plugin %s", plugin.Name)
		}
		digest, digestErr := samplingFileDigest(filepath.Join(policy.host.TrustedDirectory, samplerConfig.Path))
		if digestErr != nil {
			policy.close()
			return nil, fmt.Errorf("digest plugin %s: %w", plugin.Name, digestErr)
		}
		sampler, loadErr := pipeline.NewLoader().LoadSamplerPlugin(samplerConfig, policy.host.TrustedDirectory, group.Metadata.Name, nil)
		if loadErr != nil {
			policy.close()
			return nil, fmt.Errorf("load migration plugin %s: %w", plugin.Name, loadErr)
		}
		loadedDigest, loadedDigestErr := samplingFileDigest(filepath.Join(policy.host.TrustedDirectory, samplerConfig.Path))
		if loadedDigestErr != nil || loadedDigest != digest {
			_ = sampler.Close()
			policy.close()
			return nil, fmt.Errorf("plugin binary changed during migration preflight: %s", plugin.Name)
		}
		policy.samplers = append(policy.samplers, sampler)
		policy.plugins = append(policy.plugins, proto.Clone(plugin).(*commonv1.Plugin))
		policy.binaryDigests = append(policy.binaryDigests, digest)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(policy.config)
	if err != nil {
		policy.close()
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	policy.digest = hex.EncodeToString(digest[:])
	policy.session, err = pipeline.NewSessionChecked(policy.samplers, policy.host.CircuitBreakN)
	if err != nil {
		policy.close()
		return nil, err
	}
	return policy, nil
}

func (p *pinnedSamplingPolicy) close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		if p.session != nil {
			p.session.ClosePluginsWhenIdle()
			return
		}
		for _, sampler := range p.samplers {
			_ = sampler.Close()
		}
	})
}

// newShardSession starts native state at the preparation restart boundary.
func (p *pinnedSamplingPolicy) newShardSession(group string) (*pinnedSamplingPolicy, error) {
	return p.newShardSessionWithLoader(group, pipeline.NewLoader)
}

func (p *pinnedSamplingPolicy) newShardSessionWithLoader(group string, newLoader func() *pipeline.Loader) (*pinnedSamplingPolicy, error) {
	child := &pinnedSamplingPolicy{schema: p.schema, config: p.config, plugins: p.plugins, host: p.host, digest: p.digest, binaryDigests: p.binaryDigests}
	for index, plugin := range p.plugins {
		samplerConfig := plugin.GetSampler()
		digest, err := samplingFileDigest(filepath.Join(p.host.TrustedDirectory, samplerConfig.Path))
		if err != nil {
			child.close()
			return nil, err
		}
		if digest != p.binaryDigests[index] {
			child.close()
			return nil, fmt.Errorf("pinned sampling plugin binary changed: %s", plugin.Name)
		}
		sampler, err := newLoader().LoadSamplerPlugin(samplerConfig, p.host.TrustedDirectory, group, nil)
		if err != nil {
			child.close()
			return nil, err
		}
		child.samplers = append(child.samplers, sampler)
		loadedDigest, err := samplingFileDigest(filepath.Join(p.host.TrustedDirectory, samplerConfig.Path))
		if err != nil || loadedDigest != digest {
			child.close()
			return nil, fmt.Errorf("pinned sampling plugin changed during load: %s", plugin.Name)
		}
	}
	var err error
	child.session, err = pipeline.NewSessionChecked(child.samplers, p.host.CircuitBreakN)
	if err != nil {
		child.close()
		return nil, err
	}
	return child, nil
}
