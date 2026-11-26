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

package config

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sourcesOf is the Sources of the option struct opts points to with the given labels, keyed by
// flag (by plan path for a plan-only setting); a setting absent from labels is at its default.
func sourcesOf(opts any, labels map[string]string) Sources {
	v, fields := fieldsOf(opts)
	r := resolvedStruct{v: v, fields: fields, labels: make([]string, len(fields))}
	for i, f := range fields {
		key := f.flag
		if key == planOnly {
			key = f.plan
		}
		r.labels[i] = labels[key]
	}
	return Sources{structs: []resolvedStruct{r}}
}

// labelsOf lists the settings of s not at their default, keyed as sourcesOf keys them.
func labelsOf(s Sources) map[string]string {
	out := map[string]string{}
	for _, r := range s.structs {
		for i, f := range r.fields {
			if r.labels[i] == "" {
				continue
			}
			key := f.flag
			if key == planOnly {
				key = f.plan
			}
			out[key] = r.labels[i]
		}
	}
	return out
}

// Export and Import share flag names such as checkpoint and resume; the field address tells the
// settings apart, so the source of one never answers for the other.
func TestSources_SameFlagOfTwoStructs(t *testing.T) {
	exp, imp := &Export{}, &Import{}
	expSources := sourcesOf(exp, map[string]string{"checkpoint": "--checkpoint"})
	impSources := sourcesOf(imp, map[string]string{"checkpoint": "BYDBCTL_IMPORT_CHECKPOINT", "resume": "plan.yaml import.resume"})
	sources := Sources{structs: append(expSources.structs, impSources.structs...)}

	assert.True(t, sources.FromFlag(&exp.Checkpoint))
	assert.False(t, sources.FromEnv(&exp.Checkpoint))
	assert.False(t, sources.FromFlag(&imp.Checkpoint))
	assert.True(t, sources.FromEnv(&imp.Checkpoint))
	assert.Equal(t, "--checkpoint", sources.Label(&exp.Checkpoint))
	assert.Equal(t, "BYDBCTL_IMPORT_CHECKPOINT", sources.Label(&imp.Checkpoint))
	assert.False(t, sources.Explicit(&exp.Resume))
	assert.True(t, sources.FromPlan(&imp.Resume))

	// Two resolutions keep their structs apart too.
	assert.Panics(t, func() { expSources.FromFlag(&imp.Checkpoint) }, "the import is not one of the export's structs")
	assert.Panics(t, func() { impSources.Label(&exp.Resume) })
}

// A pointer to anything but a field of a resolved option struct is a programming error.
func TestSources_UnknownSettingPanics(t *testing.T) {
	exp := &Export{}
	sources := sourcesOf(exp, nil)
	copied := *exp
	other := ""
	for name, setting := range map[string]any{
		"a field of a copy":    &copied.Output,
		"an unrelated string":  &other,
		"the struct itself":    exp,
		"a value":              exp.Output,
		"a nil pointer":        (*string)(nil),
		"a field of no struct": new(bool),
	} {
		assert.Panics(t, func() { sources.Label(setting) }, name)
	}
	assert.Panics(t, func() { Sources{}.FromFlag(&exp.Output) }, "zero Sources know no setting")
	assert.NotPanics(t, func() { sources.Label(&exp.Output) })
	// The first field shares the address of its struct; only the field's type matches it.
	assert.Equal(t, "--output", sources.Label(&exp.Output))
}

// Each setting is told to come from its flag, the environment, plan.yaml or its default.
func TestSources_FourSources(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	conn, imp := &Connection{}, &Import{}
	Bind(fs, conn, imp)
	require.NoError(t, fs.Parse([]string{"--write-batch-size", "10", "--plan", writePlan(t, "import:\n  input: /plan\n")}))
	sources, err := Resolve(fs, envOf(map[string]string{"BYDBCTL_IMPORT_PARALLELISM": "3"}), conn, imp)
	require.NoError(t, err)

	for _, c := range []struct {
		setting                   any
		label                     string
		flag, env, plan, explicit bool
	}{
		{setting: &imp.WriteBatchSize, label: "--write-batch-size", flag: true, explicit: true},
		{setting: &imp.Parallelism, label: "BYDBCTL_IMPORT_PARALLELISM", env: true},
		{setting: &imp.Input, label: "plan.yaml import.input", plan: true, explicit: true},
		{setting: &imp.Verify, label: "--verify"},
		{setting: &imp.ConflictRules, label: "plan.yaml import.schemaConflictPolicy.rules"},
		{setting: &conn.Nodes, label: "--nodes"},
	} {
		assert.Equal(t, c.label, sources.Label(c.setting))
		assert.Equal(t, c.flag, sources.FromFlag(c.setting), c.label)
		assert.Equal(t, c.env, sources.FromEnv(c.setting), c.label)
		assert.Equal(t, c.plan, sources.FromPlan(c.setting), c.label)
		assert.Equal(t, c.explicit, sources.Explicit(c.setting), c.label)
	}
}
