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

// Package measure implements a time-series-based storage which is consists of a sequence of data points.
// Each data point contains tags and fields. They arrive in a fixed interval. A data point could be updated
// by one with the identical entity(series_id) and timestamp.
package measure

import vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"

// VectorizedConfig exposes query memory policy before decoding a native scan request.
func (s *standalone) VectorizedConfig() vmeasure.VectorizedConfig { return s.option.vectorized }

// VectorizedConfig exposes query memory policy before decoding a native scan request.
func (s *dataSVC) VectorizedConfig() vmeasure.VectorizedConfig { return s.option.vectorized }
