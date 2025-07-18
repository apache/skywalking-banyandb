// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package auth contains authentication helper functions.
package auth

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

// GenerateBasicAuthHeader creates a Basic Auth header from username and password.
func GenerateBasicAuthHeader(username, password string) string {
	credentials := fmt.Sprintf("%s:%s", username, password)
	encoded := base64.StdEncoding.EncodeToString([]byte(credentials))
	return "Basic " + encoded
}

// PromptForPassword is set to PromptForPasswordFunc by default.
// It can be replaced (mocked) in tests.
var PromptForPassword = PromptForPasswordFunc

// PromptForPasswordFunc reads password input silently from the terminal.
func PromptForPasswordFunc() (string, error) {
	fmt.Print("Enter password: ")
	bytePassword, err := term.ReadPassword(int(os.Stdin.Fd())) // Read password without showing it
	if err != nil {
		return "", fmt.Errorf("failed to read password: %w", err)
	}
	return strings.TrimSpace(string(bytePassword)), nil
}
