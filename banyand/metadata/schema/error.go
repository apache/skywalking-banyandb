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

package schema

import (
	"github.com/pkg/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrGRPCResourceNotFound indicates the resource doesn't exist.
	ErrGRPCResourceNotFound = statusGRPCResourceNotFound.Err()
	// ErrClosed indicates the registry is closed.
	ErrClosed = errors.New("metadata registry is closed")

	statusGRPCInvalidArgument  = status.New(codes.InvalidArgument, "banyandb: input is invalid")
	statusGRPCResourceNotFound = status.New(codes.NotFound, "banyandb: resource not found")
	statusGRPCAlreadyExists    = status.New(codes.AlreadyExists, "banyandb: resource already exists")
	errGRPCAlreadyExists       = statusGRPCAlreadyExists.Err()
	statusDataLoss             = status.New(codes.DataLoss, "banyandb: resource corrupts.")
	errGRPCDataLoss            = statusDataLoss.Err()
)

// BadRequest creates a gRPC error with error details with type BadRequest,
// which describes violations in a client request.
func BadRequest(field, desc string) error {
	v := &errdetails.BadRequest_FieldViolation{
		Field:       field,
		Description: desc,
	}
	br := &errdetails.BadRequest{}
	br.FieldViolations = append(br.FieldViolations, v)
	st, _ := statusGRPCInvalidArgument.WithDetails(br)
	return st.Err()
}
