#!/usr/bin/env bash

# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

BASE_DIR=$1
BIN_DIR=$2
ISTIO_VERSION=${ISTIO_VERSION:-1.20.0}

if [ ! -x "$BIN_DIR/istioctl" ]; then
  mkdir -p "$BASE_DIR/istioctl" && cd "$BASE_DIR/istioctl"
  curl -sL "https://github.com/istio/istio/releases/download/${ISTIO_VERSION}/istio-${ISTIO_VERSION}-linux-amd64.tar.gz" | tar xz
  cp "istio-${ISTIO_VERSION}/bin/istioctl" "$BIN_DIR/istioctl"
  chmod +x "$BIN_DIR/istioctl"
fi
