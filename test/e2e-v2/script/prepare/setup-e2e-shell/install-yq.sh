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

BASE_DIR=$1
BIN_DIR=$2

# The ai-agent case parses OAP's JSON with yq `tonumber`, which yq v4.33.3 (the version
# SkyWalking's own install-yq.sh pins) and earlier lack. Pin a release that has it.
YQ_VERSION=${YQ_VERSION:-v4.54.1}

# Install when yq is absent or too old to evaluate `tonumber`; a newer yq that ships with
# the runner is kept.
if ! command -v yq &> /dev/null || ! echo '"1"' | yq 'tonumber' &> /dev/null; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) YQ_BINARY=yq_linux_amd64 ;;
    Linux-aarch64 | Linux-arm64) YQ_BINARY=yq_linux_arm64 ;;
    Darwin-x86_64) YQ_BINARY=yq_darwin_amd64 ;;
    Darwin-arm64) YQ_BINARY=yq_darwin_arm64 ;;
    *) echo "unsupported platform for yq: $(uname -s)-$(uname -m)"; exit 1 ;;
  esac
  mkdir -p "$BIN_DIR"
  curl -sSLf -o "$BIN_DIR/yq" "https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/${YQ_BINARY}"
  chmod +x "$BIN_DIR/yq"
fi
