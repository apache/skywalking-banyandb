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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func TestCoverageContractRequiresIndependentInventory(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	require.NoError(t, qlgen.ValidateCoverageContract(suite.Features))
	for _, status := range []string{"supported", "rejection", "pending"} {
		t.Run("missing_"+status, func(t *testing.T) {
			features := append([]qlgen.Feature(nil), suite.Features...)
			for index, feature := range features {
				if feature.Status == status {
					features = append(features[:index], features[index+1:]...)
					require.ErrorContains(t, qlgen.ValidateCoverageContract(features), "required QL capability")
					return
				}
			}
			t.Fatalf("no capability has status %s", status)
		})
	}
	features := append([]qlgen.Feature(nil), suite.Features...)
	features[0].Status = "pending"
	require.ErrorContains(t, qlgen.ValidateCoverageContract(features), "audited contract requires")
	features = append([]qlgen.Feature(nil), suite.Features...)
	features = append(features, qlgen.Feature{ID: "unaudited", Status: "supported"})
	require.ErrorContains(t, qlgen.ValidateCoverageContract(features), "has not been audited")
}

// This is an audit alarm, not an oracle. Runtime generation never imports the
// production parser. A grammar change must update both the capability inventory
// and this fingerprint after checking which generated witnesses/gaps it affects.
func TestMeasureGrammarCoverageAudit(t *testing.T) {
	const audited = "b138407428351d27fc35127f8e1f984add4d8d281e97bfea1bcf5fc40722e983"
	var contract bytes.Buffer
	writeGrammarShape(&contract, reflect.TypeFor[bydbql.Grammar](), make(map[reflect.Type]bool))
	writeLexerAndSyntaxGuards(t, &contract)
	writeExpressionGrammar(t, &contract)
	actual := fmt.Sprintf("%x", sha256.Sum256(contract.Bytes()))
	require.Equal(t, audited, actual,
		"BydbQL grammar changed: audit supported/rejection/pending QL coverage before updating this fingerprint")
}

func writeExpressionGrammar(t *testing.T, output *bytes.Buffer) {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	// Expression syntax has a custom lexer and Pratt parser, so parser tags in
	// Grammar do not expose its operators, precedence or conditional productions.
	for _, name := range []string{
		"grammar_expression.go", "grammar_expression_lexer.go", "grammar_expression_conditional.go",
		"grammar_expression_where.go", "grammar_expression_having.go",
	} {
		file := filepath.Join(filepath.Dir(source), "../../../../pkg/bydbql", name)
		files := token.NewFileSet()
		parsed, err := parser.ParseFile(files, file, nil, 0)
		require.NoError(t, err)
		for _, declaration := range parsed.Decls {
			var formatted bytes.Buffer
			require.NoError(t, format.Node(&formatted, files, declaration))
			for _, line := range strings.Split(formatted.String(), "\n") {
				if strings.TrimSpace(line) != "" {
					fmt.Fprintln(output, line)
				}
			}
		}
	}
}

func writeGrammarShape(output *bytes.Buffer, kind reflect.Type, seen map[reflect.Type]bool) {
	for kind.Kind() == reflect.Pointer || kind.Kind() == reflect.Slice {
		kind = kind.Elem()
	}
	if kind.Kind() != reflect.Struct || seen[kind] {
		return
	}
	seen[kind] = true
	fmt.Fprintln(output, kind.Name())
	for index := range kind.NumField() {
		field := kind.Field(index)
		grammar, exists := field.Tag.Lookup("parser")
		if !exists {
			continue
		}
		fmt.Fprintf(output, "%s %s %s\n", field.Name, field.Type, grammar)
		writeGrammarShape(output, field.Type, seen)
	}
}

func writeLexerAndSyntaxGuards(t *testing.T, output *bytes.Buffer) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	source := filepath.Join(filepath.Dir(file), "../../../../pkg/bydbql/parser.go")
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, source, nil, 0)
	require.NoError(t, err)
	for _, declaration := range parsed.Decls {
		include := false
		switch node := declaration.(type) {
		case *ast.FuncDecl:
			include = node.Name.Name == "init" || node.Name.Name == "checkAggregateAndGroupByShape" || node.Name.Name == "checkParenDepth"
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				value, valid := spec.(*ast.ValueSpec)
				if !valid {
					continue
				}
				for _, name := range value.Names {
					include = include || name.Name == "bydbqlKeywords" || name.Name == "maxParenDepth"
				}
			}
		}
		if include {
			var formatted bytes.Buffer
			require.NoError(t, format.Node(&formatted, files, declaration))
			// Blank lines reflect original positions even without comments.
			for _, line := range strings.Split(formatted.String(), "\n") {
				if strings.TrimSpace(line) != "" {
					fmt.Fprintln(output, line)
				}
			}
		}
	}
}
