#!/usr/bin/env bash

# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

FB_VERSION=${FB_VERSION:=v2.0.0}

for CMD in curl cmake g++ make; do
  command -v $CMD > /dev/null || \
    { echo "[ERROR]: '$CMD' command not not found. Exiting" 1>&2; exit 1; }
done

## Create Temp Build Directory
BUILD_DIR=$(mktemp -d)
pushd $BUILD_DIR

## Fetch Tarball
curl -sLO   https://github.com/google/flatbuffers/archive/$FB_VERSION.tar.gz
tar xf $FB_VERSION.tar.gz

## Build Binaries
cd flatbuffers-${FB_VERSION#v}
cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
make
./flattests
sudo cp flatc /usr/local/bin/flatc
sudo chmod +x /usr/local/bin/flatc

## Cleanup Temp Build Directory
popd
rm -rf $BUILD_DIR
