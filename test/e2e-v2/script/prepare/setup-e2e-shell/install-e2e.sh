#!/usr/bin/env bash

# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

set -ex

BASE_DIR=$1
BIN_DIR=$2

install_e2e() {
  mkdir -p "$BASE_DIR/e2e"
  mkdir -p "$BIN_DIR"

  if ! command -v go &> /dev/null; then
    echo "go is required to install e2e"
    exit 1
  fi

  E2E_REVISION=${SW_INFRA_E2E_COMMIT:-8c21e43e241a32a54bdf8eeceb9099eb27e5e9b4}

  GO111MODULE=on go install "github.com/apache/skywalking-infra-e2e/cmd/e2e@${E2E_REVISION}"

  GOPATH_BIN="$(go env GOPATH)/bin"
  if [ ! -x "${GOPATH_BIN}/e2e" ]; then
    echo "failed to find e2e binary in ${GOPATH_BIN}"
    exit 1
  fi

  cp "${GOPATH_BIN}/e2e" "${BIN_DIR}/"
}

install_e2e

