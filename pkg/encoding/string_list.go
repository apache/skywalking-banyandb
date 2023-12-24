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

package encoding

import (
	"bytes"
	"encoding/binary"
)

func EncodeElementIDs(elementIDs []string) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, uint32(len(elementIDs)))
	if err != nil {
		return nil, err
	}
	for _, elementID := range elementIDs {
		err = binary.Write(&buf, binary.BigEndian, uint32(len(elementID)))
		if err != nil {
			return nil, err
		}
		err = binary.Write(&buf, binary.BigEndian, []byte(elementID))
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func DecodeElementIDs(data []byte) ([]string, error) {
	var elementIDs []string
	buf := bytes.NewReader(data)

	var count uint32
	err := binary.Read(buf, binary.BigEndian, &count)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(count); i++ {
		var length uint32
		err := binary.Read(buf, binary.BigEndian, &length)
		if err != nil {
			return nil, err
		}
		content := make([]byte, length)
		err = binary.Read(buf, binary.BigEndian, &content)
		if err != nil {
			return nil, err
		}
		elementIDs = append(elementIDs, string(content))
	}

	return elementIDs, nil
}
