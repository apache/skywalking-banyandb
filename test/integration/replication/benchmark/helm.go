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

package benchmark

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	benchmarkRelease         = "banyandb-bench"
	benchmarkImageRepository = "apache/skywalking-banyandb"
	builtImageRef            = benchmarkImageRepository + ":latest"
)

var benchmarkImageTag = "latest"

func setBenchmarkImageTag(tag string) {
	if tag != "" {
		benchmarkImageTag = tag
	}
}

func benchmarkImageRef() string {
	return fmt.Sprintf("%s:%s", benchmarkImageRepository, benchmarkImageTag)
}

func benchmarkSlimImageRef() string {
	return fmt.Sprintf("%s:%s-slim", benchmarkImageRepository, benchmarkImageTag)
}

func resolveBenchmarkImageTag(ctx context.Context) (string, error) {
	out, err := runCommand(ctx, "docker", "image", "inspect", "--format", "{{.ID}}", builtImageRef)
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(out)
	id = strings.TrimPrefix(id, "sha256:")
	if len(id) < 12 {
		return "", fmt.Errorf("invalid image digest %q", out)
	}
	return "bench-" + id[:12], nil
}

func buildLocalImage(ctx context.Context, repoRoot string) error {
	env := map[string]string{
		"RELEASE_VERSION": "local",
	}
	if strings.EqualFold(getEnvString("BANYANDB_BENCH_BUILD_UI", "false"), "true") {
		if _, err := runCommandEnv(ctx, env, "make", "-C", filepath.Join(repoRoot, "ui"), "build"); err != nil {
			return err
		}
	}
	if _, err := runCommandEnv(ctx, env, "make", "-C", filepath.Join(repoRoot, "banyand"), "release"); err != nil {
		return err
	}
	if _, err := runCommandEnv(ctx, env, "make", "-C", filepath.Join(repoRoot, "banyand"), "docker"); err != nil {
		return err
	}
	imageTag, err := resolveBenchmarkImageTag(ctx)
	if err != nil {
		return err
	}
	setBenchmarkImageTag(imageTag)
	if _, err := runCommand(ctx, "docker", "tag", builtImageRef, benchmarkImageRef()); err != nil {
		return err
	}
	if _, err := runCommand(ctx, "docker", "tag", builtImageRef, benchmarkSlimImageRef()); err != nil {
		return err
	}
	return nil
}

func installChart(ctx context.Context, repoRoot, namespace string, cfg Config) error {
	valuesPath := filepath.Join(repoRoot, "test", "integration", "replication", "benchmark", "values.yaml")
	args := []string{
		"upgrade",
		"--install",
		benchmarkRelease,
		cfg.ChartRef,
		"--namespace",
		namespace,
		"--create-namespace",
		"--values",
		valuesPath,
		"--set-string",
		"image.tag=" + benchmarkImageTag,
	}
	if cfg.ChartVersion != "" && strings.HasPrefix(cfg.ChartRef, "oci://") {
		args = append(args, "--version", cfg.ChartVersion)
	}
	_, err := runCommand(ctx, "helm", args...)
	return err
}

func uninstallChart(ctx context.Context, namespace string) error {
	_, err := runCommand(ctx, "helm", "uninstall", benchmarkRelease, "--namespace", namespace)
	return err
}

func deleteNamespace(ctx context.Context, namespace string) error {
	_, err := runCommand(ctx, "kubectl", "delete", "namespace", namespace, "--wait=true")
	return err
}
