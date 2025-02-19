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

package http

import (
	"context"
	"encoding/base64"
	"google.golang.org/grpc/metadata"
	"net/http"
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !auth.Cfg.Enabled {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "Authorization header is missing", http.StatusUnauthorized)
			return
		}

		if !strings.HasPrefix(authHeader, "Basic ") {
			http.Error(w, "Invalid authorization header format", http.StatusBadRequest)
			return
		}

		encodedCredentials := strings.TrimPrefix(authHeader, "Basic ")

		decodedBytes, err := base64.StdEncoding.DecodeString(encodedCredentials)
		if err != nil {
			http.Error(w, "Failed to decode authorization header", http.StatusBadRequest)
			return
		}

		decodedCredentials := string(decodedBytes)

		parts := strings.SplitN(decodedCredentials, ":", 2)
		if len(parts) != 2 {
			http.Error(w, "Invalid authorization header format", http.StatusBadRequest)
			return
		}

		username := parts[0]
		password := parts[1]
		for _, user := range auth.Cfg.Users {
			if strings.TrimSpace(username) == strings.TrimSpace(user.Username) &&
				strings.TrimSpace(password) == strings.TrimSpace(user.Password) {
				next.ServeHTTP(w, r)
				return
			}
		}
		http.Error(w, `{"error": "invalid credentials"}`, http.StatusUnauthorized)
	})
}

func metadataAnnotator() func(context.Context, *http.Request) metadata.MD {
	return func(_ context.Context, r *http.Request) metadata.MD {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			return nil
		}

		if !strings.HasPrefix(authHeader, "Basic ") {
			return nil
		}

		encodedCredentials := strings.TrimPrefix(authHeader, "Basic ")

		decodedBytes, err := base64.StdEncoding.DecodeString(encodedCredentials)
		if err != nil {
			return nil
		}

		decodedCredentials := string(decodedBytes)

		parts := strings.SplitN(decodedCredentials, ":", 2)
		if len(parts) != 2 {
			return nil
		}

		username := parts[0]
		password := parts[1]

		md := metadata.Pairs("username", username, "password", password)
		return md
	}
}
