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

package logical

import (
	"fmt"
	"strconv"
)

var _ Expr = (*int64Literal)(nil)

type int64Literal struct {
	int64
}

func (i *int64Literal) String() string {
	return strconv.FormatInt(i.int64, 10)
}

var _ Expr = (*int64Literal)(nil)

type int64ArrayLiteral struct {
	arr []int64
}

func (i *int64ArrayLiteral) String() string {
	return fmt.Sprintf("%v", i.arr)
}

var _ Expr = (*strLiteral)(nil)

type strLiteral struct {
	string
}

func (s *strLiteral) String() string {
	return s.string
}

var _ Expr = (*strArrLiteral)(nil)

type strArrLiteral struct {
	arr []string
}

func (s *strArrLiteral) String() string {
	return fmt.Sprintf("%v", s.arr)
}
