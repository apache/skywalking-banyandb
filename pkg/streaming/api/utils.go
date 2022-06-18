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

package api

import (
	"context"
	"reflect"

	"github.com/pkg/errors"
)

var (
	ErrNotFunction              = errors.New("not a function")
	ErrMultipleReturnValue      = errors.New("malformed func: multiple return value is not supported")
	ErrInvalidReturnType        = errors.New("malformed func: invalid return type")
	ErrUnexpectedArgumentLength = errors.New("malformed func: unexpected argument len")
)

// Transmit provides a helper function to connect the current component with the downstream component.
// It should be run in another goroutine.
func Transmit(downstream Inlet, current Outlet) {
	for elem := range current.Out() {
		downstream.In() <- elem
	}
	close(downstream.In())
}

func transformToUnaryFunc(typ reflect.Type, fnVal reflect.Value) (reflectedUnaryFunc, error) {
	if typ.Kind() != reflect.Func {
		return nil, ErrNotFunction
	}

	if typ.NumOut() != 1 {
		return nil, ErrMultipleReturnValue
	}

	switch typ.NumIn() {
	case 1: // single argument
		return func(ctx context.Context, i interface{}) reflect.Value {
			return fnVal.Call([]reflect.Value{reflect.ValueOf(i)})[0]
		}, nil
	case 2: // binary arguments
		return func(ctx context.Context, i interface{}) reflect.Value {
			arg0 := reflect.ValueOf(ctx)
			if !arg0.IsValid() {
				arg0 = reflect.ValueOf(context.Background())
			}
			return fnVal.Call([]reflect.Value{arg0, reflect.ValueOf(i)})[0]
		}, nil
	default:
		return nil, errors.Wrap(ErrUnexpectedArgumentLength, "expect func(T)->R or func(context.Context,T)->R")
	}
}

// FilterFunc transform a function to an UnaryOperation
func FilterFunc(f interface{}) (UnaryOperation, error) {
	funcTyp := reflect.TypeOf(f)

	fun, err := transformToUnaryFunc(funcTyp, reflect.ValueOf(f))
	if err != nil {
		return nil, err
	}

	if funcTyp.Out(0).Kind() != reflect.Bool {
		return nil, errors.Wrap(ErrInvalidReturnType, "expected <bool>")
	}

	return UnaryFunc(func(ctx context.Context, payload interface{}) interface{} {
		predicate := fun(ctx, payload).Bool()
		if !predicate {
			return nil
		}
		return payload
	}), nil
}

func MapperFunc(f interface{}) (UnaryOperation, error) {
	funcTyp := reflect.TypeOf(f)

	fun, err := transformToUnaryFunc(funcTyp, reflect.ValueOf(f))
	if err != nil {
		return nil, err
	}

	return UnaryFunc(func(ctx context.Context, payload interface{}) interface{} {
		return fun(ctx, payload).Interface()
	}), nil
}
