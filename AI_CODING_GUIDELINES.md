# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# AI Assistant Rules for SkyWalking BanyanDB Go Project
# This file provides coding standards and guidelines for AI assistants
# Compatible with Claude, Cursor, GitHub Copilot, and other LLMs

## PROJECT OVERVIEW
This is the SkyWalking BanyanDB project - a distributed time-series database written in Go.
Follow these strict coding standards when generating or modifying Go code.

## FORMATTING RULES
1. Maximum line length: 170 characters

## LINTING RULES
1. Variable shadowing prevention (govet shadow enabled)
2. Import aliases for specific packages (importas settings)
3. Error handling patterns (errcheck, errorlint, errname)
4. Code style and conventions (gosimple, staticcheck, stylecheck)
5. Security considerations (gosec)
6. Documentation standards (godot scope: toplevel)

## IMPORT ORGANIZATION
Follow the sections order:
1. Standard library imports
2. Default imports
3. github.com/apache/skywalking-banyandb/ prefix imports

## IMPORT ALIASES
Use these specific aliases for protobuf packages:
- github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1 → commonv1
- github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1 → databasev1
- github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1 → modelv1
- github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1 → propertyv1
- github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1 → measurev1
- github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1 → streamv1
- github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1 → clusterv1
- github.com/apache/skywalking-banyandb/pkg/pb/v1 → pbv1

## ERROR HANDLING PATTERNS
1. Always check errors immediately after function calls
2. Use descriptive error variable names to avoid shadowing
3. Follow the errname convention for error types
4. Use errorlint for consistent error handling
5. Wrap errors with context using fmt.Errorf and %w verb

## VARIABLE SHADOWING PREVENTION
1. NEVER shadow variables in the same scope
2. Use unique variable names in nested scopes
3. Be especially careful with common variable names like 'err', 'ctx', 'v', 'i', 'n', etc.
4. When using range loops, use descriptive variable names instead of single letters
5. In if statements with variable declarations, ensure the variable name doesn't conflict with outer scope

## CODE STYLE
1. Use gocritic enabled checks for code quality
2. Follow exhaustive switch/map patterns
3. Use proper field alignment (govet fieldalignment)
4. Avoid unnecessary conversions (unconvert)
5. Use standard library variables when available (usestdlibvars)
6. Follow whitespace rules (whitespace linter)

## SECURITY
1. Follow gosec security guidelines
2. Be aware of integer overflow conversions (G115 excluded in config)

## DOCUMENTATION
1. Add top-level comments for exported functions and types (godot scope: toplevel)
2. Follow docStub patterns for documentation
3. Use proper sentence structure and punctuation
4. Describe what the function does, not how it does it

## CONSTANTS AND MAGIC NUMBERS
1. Use goconst for repeated string literals (min-occurrences: 4)
2. Avoid magic numbers, use named constants

## COMPLEXITY
1. Keep cyclomatic complexity low (gocyclo)
2. Avoid deeply nested structures
3. Use proper abstraction levels

## NAMING CONVENTIONS
1. Use descriptive names for variables, especially in nested scopes
2. Follow Go naming conventions
3. Use proper package naming
4. Follow the project's specific naming patterns

## TESTING
1. Follow proper test naming conventions
2. Use appropriate test helpers
3. Follow the project's testing patterns

## EXCLUDED PATTERNS
The following file patterns are excluded from linting:
- *.pb.go (protobuf generated files)
- *.pb.gw.go (protobuf gateway files)
- *.gen.go (generated files)
- *_mock.go (mock files)
- *_test.go files have relaxed errcheck rules

## PATTERNS TO AVOID

### Variable Shadowing Patterns (BAD):
```go
if err := someFunc(); err != nil {
if v := someValue(); v != nil {
for _, v := range items {
for i, v := range items {
switch v := someValue(); v {
defer func() { if err := cleanup(); err != nil {
```

### Import Organization Violations (BAD):
```go
import (
	"fmt"
	"github.com/apache/skywalking-banyandb/pkg/something"
	"os"
)
```

### Error Handling Violations (BAD):
```go
someFunc() // ignoring error
if err != nil {
	return
}
```

### Style Violations (BAD):
```go
if x == true {
if x == false {
if x != nil && x.y == true {
```

### Documentation Violations (BAD):
```go
// Function does something
// This function does something
```

## PREFERRED PATTERNS

### Proper Variable Naming to Avoid Shadowing (GOOD):
```go
if resultErr := someFunc(); resultErr != nil {
if item := someValue(); item != nil {
for _, item := range items {
for idx, item := range items {
switch value := someValue(); value {
defer func() { if cleanupErr := cleanup(); cleanupErr != nil {
```

### Proper Import Organization (GOOD):
```go
import (
	"fmt"
	"os"

	"github.com/apache/skywalking-banyandb/pkg/something"
)
```

### Proper Error Handling (GOOD):
```go
if err := someFunc(); err != nil {
	return fmt.Errorf("failed to do something: %w", err)
}
```

### Proper Style (GOOD):
```go
if x {
if !x {
if x != nil && x.y {
```

### Proper Documentation (GOOD):
```go
// DoSomething performs a specific action.
// DoSomething performs a specific action and returns an error if it fails.
```

## CODE GENERATION GUIDELINES

### IMPORTANT: Always follow these steps:
1. Check for existing variable names in the current scope before declaring new ones
2. Use descriptive variable names to avoid shadowing
3. Organize imports according to "IMPORT ORGANIZATION" sections
4. Use proper import aliases for the project's protobuf packages
5. Follow error handling patterns with proper error wrapping
6. Add appropriate documentation for exported functions and types
7. Keep line length under 170 characters
8. Use gofumpt formatting style

### Example of Good Code Structure:
```go
package example

import (
	"context"
	"fmt"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// ProcessData processes the given data and returns the result.
func ProcessData(ctx context.Context, data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	// Use descriptive variable names to avoid shadowing
	for idx, item := range data {
		if processErr := processItem(ctx, item); processErr != nil {
			return nil, fmt.Errorf("failed to process item at index %d: %w", idx, processErr)
		}
	}

	result, err := finalizeProcessing(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("failed to finalize processing: %w", err)
	}

	return result, nil
}
```

### Example of Bad Code (violates multiple rules):
```go
package example

import (
	"fmt"
	"github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1" // wrong alias
	"os"
)

func ProcessData(ctx context.Context, data []byte) ([]byte, error) {
	for _, v := range data {
		if err := processItem(ctx, v); err != nil { // 'err' shadows outer scope
			return err
		}
		
		for i, v := range v.SubItems { // 'v' shadows outer 'v'
			if err := processSubItem(v); err != nil { // 'err' shadows outer 'err'
				return err
			}
		}
	}
	return nil
}
```

## COMMON VARIABLE NAMES TO BE EXTRA CAREFUL WITH
err, error, ctx, context, v, value, i, idx, index,
n, num, count, size, len, length, k, key, val,
result, res, data, item, obj, object, msg, message,
resp, response, req, request, client, conn, connection

## NAMING CONVENTIONS FOR NESTED SCOPES
When dealing with nested scopes, use these naming patterns:
- Outer scope: 'err', 'ctx', 'v', 'i'
- Inner scope: 'innerErr', 'innerCtx', 'item', 'idx'
- Deep nested: 'deepErr', 'deepCtx', 'subItem', 'subIdx'
- Or use descriptive prefixes: 'processErr', 'validateErr', 'parseErr'

## USAGE INSTRUCTIONS FOR AI ASSISTANTS

### For Claude:
- Use this file as a reference when generating Go code
- Follow all formatting, naming, and error handling patterns
- Pay special attention to variable shadowing prevention
- Use the provided import aliases and organization rules

### For Cursor:
- This file can be used as .cursorrules
- Follow all the patterns and guidelines specified

### For GitHub Copilot:
- Use this as a reference for code generation
- Follow the coding standards and patterns

### For Other LLMs:
- Use this as a comprehensive coding guide
- Follow all specified patterns and conventions
- Pay attention to the project-specific requirements 