//go:build !slim
// +build !slim

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
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/apache/skywalking-banyandb/ui"
)

func (p *server) setRootPath(mux *chi.Mux) error {
	fSys, err := fs.Sub(ui.DistContent, "dist")
	if err != nil {
		return err
	}
	httpFS := http.FS(fSys)
	fileServer := http.FileServer(http.FS(fSys))
	serveIndex := serveFileContents("index.html", httpFS)
	mux.Mount("/", intercept404(fileServer, serveIndex))
	return nil
}
