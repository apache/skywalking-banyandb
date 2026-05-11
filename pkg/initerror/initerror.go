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

// Package initerror exposes a Permanent error contract for initialization
// failures that must not be retried. The package is a leaf: it imports nothing
// project-internal so it can be referenced by both the storage layer (which
// produces permanent errors) and the metadata-schema retry classifier (which
// reads them) without introducing a layering edge between those packages.
package initerror

import "errors"

// Permanent marks an error as a permanent (non-retriable) initialization failure.
type Permanent interface {
	error
	Permanent() bool
}

type permanentError struct {
	err error
}

// Error implements error.
func (p *permanentError) Error() string { return p.err.Error() }

// Unwrap exposes the wrapped error so errors.Is/As can traverse the chain.
func (p *permanentError) Unwrap() error { return p.err }

// Permanent reports that this error must not be retried.
func (p *permanentError) Permanent() bool { return true }

// AsPermanent wraps err as a Permanent error. Returns nil if err is nil.
func AsPermanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err's chain contains a Permanent error.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	var p Permanent
	return errors.As(err, &p) && p.Permanent()
}
