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

// Package exitcode defines the process exit codes of data export and import and maps errors to them.
package exitcode

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exit codes of data export and import; codes 1 to 3 are shared, 4 and 5 are import only.
const (
	// ExitUsage is a parameter or usage error.
	ExitUsage = 1
	// ExitPreflight is a read-only preflight rejection: nothing was written anywhere.
	ExitPreflight = 2
	// ExitRuntime is a failure during execution, after the preflight passed.
	ExitRuntime = 3
	// ExitVerify is an import whose --verify found the target's row counts inconsistent with
	// the rows sent, or could not complete the check.
	ExitVerify = 4
	// ExitPlanChanged is an import stopped by --abort-on-plan-change: the plan re-checked after
	// writing the schema differed, so the schema is written but no data was sent.
	ExitPlanChanged = 5
)

// ExitError carries a process exit code alongside the error.
type ExitError struct {
	Err  error
	Code int
}

func (e *ExitError) Error() string { return e.Err.Error() }

// Unwrap exposes the wrapped error to errors.Is / errors.As.
func (e *ExitError) Unwrap() error { return e.Err }

// Exit wraps a formatted error with an exit code.
func Exit(code int, format string, args ...any) *ExitError {
	return &ExitError{Code: code, Err: fmt.Errorf(format, args...)}
}

// For maps an error to the exit code of its cause: an ExitError keeps its code,
// INVALID_ARGUMENT is a usage error, a refusal the server made before writing anything
// (permission, authentication, precondition, not found, or UNIMPLEMENTED from a server
// without data export) is a preflight rejection, and everything else failed at run time.
func For(err error) int {
	var exitErr *ExitError
	if errors.As(err, &exitErr) {
		return exitErr.Code
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return ExitUsage
	case codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.NotFound, codes.Unimplemented:
		return ExitPreflight
	default:
		return ExitRuntime
	}
}
