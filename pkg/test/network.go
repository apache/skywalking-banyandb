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

package test

import (
	"fmt"
	"net"
)

// AllocateFreePorts finds n available ports
// Copied from https://github.com/tendermint/tendermint/issues/3682
func AllocateFreePorts(n int) ([]int, error) {
	ports := make([]int, n)

	for k := range ports {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return ports, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return ports, err
		}
		// This is done on purpose - we want to keep ports
		// busy to avoid collisions when getting the next one
		defer func() { _ = l.Close() }()
		ports[k] = l.Addr().(*net.TCPAddr).Port
	}

	return ports, nil
}

// NewEtcdListenUrls returns two urls with different ports.
func NewEtcdListenUrls() (string, string, error) {
	ports, err := AllocateFreePorts(2)
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("http://127.0.0.1:%d", ports[0]), fmt.Sprintf("http://127.0.0.1:%d", ports[1]), nil
}
