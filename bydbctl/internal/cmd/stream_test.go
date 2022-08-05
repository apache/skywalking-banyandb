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

package cmd_test

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("Stream", func() {
	It("exec bydbctl without any parameter", func() {
		// todo: start banyand-server

		cmd := exec.Command("../../build/bin/bydbctl")
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s", out.String())
	})

	It("exec bydbctl stream list operation", func() {
		// todo: start banyand-server
		cmd := exec.Command("../../build/bin/bydbctl", "stream list", "-j \"{\\\"group\\\":\\\"mxm\\\",\\\"name\\\":\\\"naonao\\\"}\"")
		fmt.Println(cmd)
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s", out.String())
	})
})
