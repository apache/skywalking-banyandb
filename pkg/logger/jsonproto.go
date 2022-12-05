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

package logger

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MarshalError is the error raised by marshaling a JSON object.
type MarshalError struct {
	Msg string `json:"msg"`
}

// Proto converts proto.Message to JSON raw data.
func Proto(message proto.Message) []byte {
	b, err := protojson.Marshal(message)
	if err != nil {
		var errJSON error
		b, errJSON = json.Marshal(MarshalError{Msg: err.Error()})
		if errJSON != nil {
			return nil
		}
	}
	return b
}
