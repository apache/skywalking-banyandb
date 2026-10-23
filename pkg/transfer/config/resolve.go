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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"sigs.k8s.io/yaml"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// EnvPrefix is prepended to every env tag.
const EnvPrefix = "BYDBCTL_"

type kind int

const (
	kindString kind = iota
	kindBool
	kindStrings
	kindDuration
	kindSize
	kindRateBytes
	kindRateRows
	kindParallelism
	kindSelector
	kindFormat
	kindCompress
)

var (
	parseKinds = map[string]kind{
		"size": kindSize, "rate-bytes": kindRateBytes, "rate-rows": kindRateRows, "parallelism": kindParallelism,
		"selector": kindSelector, "format": kindFormat, "compress": kindCompress,
	}
	kindTypes = map[kind]reflect.Type{
		kindString:      reflect.TypeOf(""),
		kindBool:        reflect.TypeOf(false),
		kindStrings:     reflect.TypeOf([]string(nil)),
		kindDuration:    reflect.TypeOf(time.Duration(0)),
		kindSize:        reflect.TypeOf(uint64(0)),
		kindRateBytes:   reflect.TypeOf(float64(0)),
		kindRateRows:    reflect.TypeOf(float64(0)),
		kindParallelism: reflect.TypeOf(0),
		kindSelector:    reflect.TypeOf([]*transferv1.Selector(nil)),
		kindFormat:      reflect.TypeOf(transferv1.Format(0)),
		kindCompress:    reflect.TypeOf(transferv1.Compress(0)),
	}
)

// field is one tagged option.
type field struct {
	flag, short, plan, env, def, usage string
	index                              int
	kind                               kind
}

// fieldsOf reads the tags of the struct opts points to (or is). A malformed declaration is a
// programming error and panics.
func fieldsOf(opts any) (reflect.Value, []field) {
	v := reflect.ValueOf(opts)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic(fmt.Sprintf("config: %T is not a struct", opts))
	}
	t := v.Type()
	out := make([]field, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		f := field{
			index: i, flag: sf.Tag.Get("flag"), short: sf.Tag.Get("short"), plan: sf.Tag.Get("plan"),
			env: sf.Tag.Get("env"), def: sf.Tag.Get("default"), usage: sf.Tag.Get("usage"),
		}
		if f.flag == "" {
			panic(fmt.Sprintf("config: %s.%s has no flag tag", t.Name(), sf.Name))
		}
		if p := sf.Tag.Get("parse"); p != "" {
			k, ok := parseKinds[p]
			if !ok {
				panic(fmt.Sprintf("config: %s.%s has an unknown parse grammar %q", t.Name(), sf.Name, p))
			}
			f.kind = k
		} else {
			switch sf.Type {
			case kindTypes[kindString]:
				f.kind = kindString
			case kindTypes[kindBool]:
				f.kind = kindBool
			case kindTypes[kindStrings]:
				f.kind = kindStrings
			case kindTypes[kindDuration]:
				f.kind = kindDuration
			default:
				panic(fmt.Sprintf("config: %s.%s of type %s needs a parse tag", t.Name(), sf.Name, sf.Type))
			}
		}
		if sf.Type != kindTypes[f.kind] {
			panic(fmt.Sprintf("config: %s.%s is a %s, its grammar needs a %s", t.Name(), sf.Name, sf.Type, kindTypes[f.kind]))
		}
		if f.kind == kindSelector && (f.env != "" || f.def != "") {
			panic(fmt.Sprintf("config: %s.%s: selectors take no env or default tag", t.Name(), sf.Name))
		}
		if f.def == "" && f.kind == kindBool {
			f.def = "false"
		}
		if f.def == "" && f.kind == kindDuration {
			f.def = "0s"
		}
		out = append(out, f)
	}
	return v, out
}

// PlanFlag is the flag naming the plan.yaml every data command reads its settings from.
const PlanFlag = "plan"

// Bind registers --plan and one flag per field of every option struct in opts (pointers to
// structs). The flags hold the raw text; Resolve parses it. The usage text names the
// plan.yaml key and the variable.
func Bind(fs *pflag.FlagSet, opts ...any) {
	fs.String(PlanFlag, "", "plan.yaml describing connection, scope and export options")
	for _, o := range opts {
		bindFields(fs, o)
	}
}

func bindFields(fs *pflag.FlagSet, opts any) {
	_, fields := fieldsOf(opts)
	for _, f := range fields {
		usage := f.usage
		var also []string
		if f.plan != "" {
			also = append(also, "plan.yaml "+f.plan)
		}
		if f.env != "" {
			also = append(also, EnvPrefix+f.env)
		}
		if len(also) > 0 {
			usage += " (" + strings.Join(also, ", ") + ")"
		}
		switch f.kind {
		case kindBool:
			fs.BoolP(f.flag, f.short, f.def == "true", usage)
		case kindDuration:
			d, err := time.ParseDuration(f.def)
			if err != nil {
				panic(fmt.Sprintf("config: default of --%s: %v", f.flag, err))
			}
			fs.DurationP(f.flag, f.short, d, usage)
		case kindStrings:
			fs.StringSliceP(f.flag, f.short, nil, usage)
		case kindSelector:
			fs.StringArrayP(f.flag, f.short, nil, usage)
		default:
			fs.StringP(f.flag, f.short, f.def, usage)
		}
	}
}

// Sources records where each resolved setting came from, by flag name: "--<flag>",
// "plan.yaml <path>" or "BYDBCTL_<NAME>"; a setting left at its default is absent.
type Sources map[string]string

// FromFlag reports whether the setting was given as an explicit flag.
func (s Sources) FromFlag(flag string) bool { return s[flag] == "--"+flag }

// FromPlan reports whether the setting came from plan.yaml.
func (s Sources) FromPlan(flag string) bool { return strings.HasPrefix(s[flag], "plan.yaml ") }

// Label names the source of a setting for messages; "--<flag>" for a default.
func (s Sources) Label(flag string) string {
	if l := s[flag]; l != "" {
		return l
	}
	return "--" + flag
}

// Resolve fills every option struct in opts (pointers to structs bound with Bind on fs) with
// typed values. Each setting comes from its explicit flag, else the plan.yaml --plan names
// (when set), else the BYDBCTL_* variable lookupEnv finds (an empty value counts as unset),
// else the default. plan.yaml is strict: a key no option struct of this package declares is
// rejected. Errors name the source of the bad value.
func Resolve(fs *pflag.FlagSet, lookupEnv func(string) (string, bool), opts ...any) (Sources, error) {
	planFile, err := fs.GetString(PlanFlag)
	if err != nil {
		return nil, err
	}
	plan, err := LoadPlan(planFile)
	if err != nil {
		return nil, err
	}
	if lookupEnv == nil {
		lookupEnv = func(string) (string, bool) { return "", false }
	}
	sources := Sources{}
	for _, o := range opts {
		v, fields := fieldsOf(o)
		for _, f := range fields {
			dst := v.Field(f.index)
			source, resolveErr := resolveField(fs, plan, lookupEnv, f, dst)
			if source != "" {
				sources[f.flag] = source
			}
			if resolveErr != nil {
				return nil, fmt.Errorf("%s: %w", sources.Label(f.flag), resolveErr)
			}
		}
	}
	return sources, nil
}

// resolveField picks the source of one setting and parses its value into dst. It returns
// the source label, "" for the default.
func resolveField(fs *pflag.FlagSet, plan map[string]any, lookupEnv func(string) (string, bool), f field, dst reflect.Value) (string, error) {
	if fl := fs.Lookup(f.flag); fl != nil && fl.Changed {
		source := "--" + f.flag
		switch f.kind {
		case kindStrings:
			values, err := fs.GetStringSlice(f.flag)
			if err != nil {
				return source, err
			}
			dst.Set(reflect.ValueOf(splitList(strings.Join(values, ","))))
			return source, nil
		case kindSelector:
			raws, err := fs.GetStringArray(f.flag)
			if err != nil {
				return source, err
			}
			cfgs := make([]SelectorConfig, 0, len(raws))
			for _, raw := range raws {
				s, parseErr := ParseSelector(raw)
				if parseErr != nil {
					return source, parseErr
				}
				cfgs = append(cfgs, s)
			}
			return source, setSelectors(dst, cfgs)
		default:
			return source, setText(dst, f.kind, fl.Value.String())
		}
	}
	if raw, ok := plan[f.plan]; ok && f.plan != "" {
		source := "plan.yaml " + f.plan
		return source, setPlanValue(dst, f.kind, raw)
	}
	if f.env != "" {
		if text, ok := lookupEnv(EnvPrefix + f.env); ok && text != "" {
			source := EnvPrefix + f.env
			if f.kind == kindStrings {
				dst.Set(reflect.ValueOf(splitList(text)))
				return source, nil
			}
			return source, setText(dst, f.kind, text)
		}
	}
	if f.kind == kindStrings || f.kind == kindSelector {
		dst.Set(reflect.Zero(dst.Type()))
		return "", nil
	}
	return "", setText(dst, f.kind, f.def)
}

// splitList splits a comma separated variable, dropping blanks.
func splitList(text string) []string {
	var out []string
	for _, item := range strings.Split(text, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// setText parses the text of a scalar setting with its grammar.
func setText(dst reflect.Value, k kind, text string) error {
	switch k {
	case kindString:
		dst.SetString(text)
	case kindBool:
		b, err := strconv.ParseBool(strings.TrimSpace(text))
		if err != nil {
			return fmt.Errorf("%q is not a boolean", text)
		}
		dst.SetBool(b)
	case kindDuration:
		d, err := time.ParseDuration(strings.TrimSpace(text))
		if err != nil {
			return err
		}
		if d < 0 {
			return fmt.Errorf("duration %q must not be negative", text)
		}
		dst.SetInt(int64(d))
	case kindSize:
		n, err := ParseSize(text)
		if err != nil {
			return err
		}
		dst.SetUint(n)
	case kindRateBytes, kindRateRows:
		rk := RateBytes
		if k == kindRateRows {
			rk = RateRows
		}
		r, err := ParseRate(text, rk)
		if err != nil {
			return err
		}
		dst.SetFloat(r)
	case kindParallelism:
		n, err := ParseParallelism(strings.TrimSpace(text))
		if err != nil {
			return err
		}
		dst.SetInt(int64(n))
	case kindFormat:
		f, err := ParseFormat(text)
		if err != nil {
			return err
		}
		dst.SetInt(int64(f))
	case kindCompress:
		c, err := ParseCompress(text)
		if err != nil {
			return err
		}
		dst.SetInt(int64(c))
	default:
		return fmt.Errorf("grammar %d takes no text", k)
	}
	return nil
}

// setPlanValue converts a decoded plan.yaml value. Sizes, rates and the parallelism take a
// YAML number or a string, booleans a YAML boolean, lists a YAML sequence, and every other
// scalar a string.
func setPlanValue(dst reflect.Value, k kind, raw any) error {
	switch k {
	case kindBool:
		b, ok := raw.(bool)
		if !ok {
			return fmt.Errorf("must be true or false, got %s", describe(raw))
		}
		dst.SetBool(b)
		return nil
	case kindStrings:
		list, ok := raw.([]any)
		if !ok {
			return fmt.Errorf("must be a list of strings, got %s", describe(raw))
		}
		values := make([]string, 0, len(list))
		for i, item := range list {
			s, isString := item.(string)
			if !isString {
				return fmt.Errorf("item %d must be a string, got %s", i, describe(item))
			}
			if s = strings.TrimSpace(s); s != "" {
				values = append(values, s)
			}
		}
		dst.Set(reflect.ValueOf(values))
		return nil
	case kindSelector:
		body, err := json.Marshal(raw)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		var cfgs []SelectorConfig
		if err = dec.Decode(&cfgs); err != nil {
			return fmt.Errorf("must be a list of {catalog, groups}: %w", err)
		}
		return setSelectors(dst, cfgs)
	case kindSize, kindRateBytes, kindRateRows, kindParallelism, kindDuration:
		// A bare number is only meaningful for a duration when it is 0, which
		// time.ParseDuration accepts.
		switch v := raw.(type) {
		case string:
			return setText(dst, k, v)
		case json.Number:
			return setText(dst, k, v.String())
		default:
			return fmt.Errorf("must be a number or a string, got %s", describe(raw))
		}
	default:
		s, ok := raw.(string)
		if !ok {
			return fmt.Errorf("must be a string, got %s", describe(raw))
		}
		return setText(dst, k, s)
	}
}

// setSelectors validates the selectors and stores their wire form.
func setSelectors(dst reflect.Value, cfgs []SelectorConfig) error {
	var out []*transferv1.Selector
	for i := range cfgs {
		s, err := cfgs[i].ToProto()
		if err != nil {
			return fmt.Errorf("[%d]: %w", i, err)
		}
		out = append(out, s)
	}
	dst.Set(reflect.ValueOf(out))
	return nil
}

func describe(raw any) string {
	switch v := raw.(type) {
	case bool:
		return "a boolean"
	case json.Number:
		return "the number " + v.String()
	case string:
		return strconv.Quote(v)
	case []any:
		return "a list"
	case map[string]any:
		return "a mapping"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ParseFormat parses an artifact format name: csv or native, in any case.
func ParseFormat(raw string) (transferv1.Format, error) {
	switch f := strings.ToLower(strings.TrimSpace(raw)); f {
	case "csv":
		return transferv1.Format_FORMAT_CSV, nil
	case "native":
		return transferv1.Format_FORMAT_NATIVE, nil
	default:
		return 0, fmt.Errorf("unsupported format %q (csv|native)", raw)
	}
}

// ParseCompress parses a file compression name: none or gzip, in any case.
func ParseCompress(raw string) (transferv1.Compress, error) {
	switch c := strings.ToLower(strings.TrimSpace(raw)); c {
	case "gzip":
		return transferv1.Compress_COMPRESS_GZIP, nil
	case "none":
		return transferv1.Compress_COMPRESS_NONE, nil
	default:
		return 0, fmt.Errorf("unsupported compress %q (none|gzip)", raw)
	}
}

// LoadPlan reads plan.yaml into its leaf values keyed by dotted path; "" yields no values.
// Every key must be declared by a plan tag of an option struct of this package, so a typo
// never silently falls back to a default. Numbers keep their text (json.Number).
func LoadPlan(path string) (map[string]any, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read plan file: %w", err)
	}
	// Strict: a key given twice is an error, not a silent override of the first.
	body, err := yaml.YAMLToJSONStrict(raw)
	if err != nil {
		return nil, fmt.Errorf("parse plan file %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc any
	if err = dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse plan file %s: %w", path, err)
	}
	leaves := map[string]any{}
	if doc == nil {
		return leaves, nil
	}
	top, ok := doc.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("parse plan file %s: the top level must be a mapping", path)
	}
	known, prefixes := planKeys()
	if err = walkPlan("", top, known, prefixes, leaves); err != nil {
		return nil, fmt.Errorf("parse plan file %s: %w", path, err)
	}
	return leaves, nil
}

// planKeys returns the declared plan paths and every section above them.
func planKeys() (known, prefixes map[string]bool) {
	known, prefixes = map[string]bool{}, map[string]bool{}
	for _, o := range planSchema {
		_, fields := fieldsOf(o)
		for _, f := range fields {
			if f.plan == "" {
				continue
			}
			known[f.plan] = true
			parts := strings.Split(f.plan, ".")
			for i := 1; i < len(parts); i++ {
				prefixes[strings.Join(parts[:i], ".")] = true
			}
		}
	}
	return known, prefixes
}

func walkPlan(prefix string, m map[string]any, known, prefixes map[string]bool, leaves map[string]any) error {
	for k, v := range m {
		p := k
		if prefix != "" {
			p = prefix + "." + k
		}
		switch {
		case known[p]:
			if v != nil {
				leaves[p] = v
			}
		case prefixes[p]:
			if v == nil {
				continue
			}
			sub, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("%s must be a mapping, got %s", p, describe(v))
			}
			if err := walkPlan(p, sub, known, prefixes, leaves); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown field %q", p)
		}
	}
	return nil
}
