#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -ex

if [ "$VERSION" == "" ]; then
  echo "VERSION environment variable not found, Please setting the VERSION."
  echo "For example: export VERSION=1.0.0"
  exit 1
fi

VERSION=${VERSION}
TAG_NAME=v${VERSION}
PRODUCT_NAME="skywalking-banyandb-${VERSION}"

echo "Release version "${VERSION}
echo "Source tag "${TAG_NAME}

SCRIPTDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOTDIR=${SCRIPTDIR}/..
BUILDDIR=${ROOTDIR}/build

pushd ${BUILDDIR}
trap 'popd' EXIT

rm -rf skywalking

svn co https://dist.apache.org/repos/dist/dev/skywalking/
mkdir -p skywalking/banyandb/"$VERSION"
cp ${PRODUCT_NAME}-*.tgz skywalking/banyandb/"$VERSION"
cp ${PRODUCT_NAME}-*.tgz.asc skywalking/banyandb/"$VERSION"
cp ${PRODUCT_NAME}-*.tgz.sha512 skywalking/banyandb/"$VERSION"

cd skywalking/banyandb && svn add "$VERSION" && svn commit -m "Draft Apache SkyWalking BanyanDB release $VERSION"
cd "$VERSION"

cat << EOF
=========================================================================
Subject: [VOTE] Release Apache SkyWalking BanyanDB version $VERSION

Content:

Hi the SkyWalking Community:
This is a call for vote to release Apache SkyWalking BanyanDB version $VERSION.

Release notes:

 * https://github.com/apache/skywalking-banyandb/blob/v$VERSION/CHANGES.md

Release Candidate:

 * https://dist.apache.org/repos/dist/dev/skywalking/banyandb/$VERSION
 * sha512 checksums
   - $(cat ${PRODUCT_NAME}-src.tgz.sha512)
   - $(cat ${PRODUCT_NAME}-banyand.tgz.sha512)
   - $(cat ${PRODUCT_NAME}-bydbctl.tgz.sha512)

Release Tag :

 * (Git Tag) $TAG_NAME

Release Commit Hash :

 * https://github.com/apache/skywalking-banyandb/tree/$(git rev-list -n 1 "$TAG_NAME")

Keys to verify the Release Candidate :

 * https://dist.apache.org/repos/dist/release/skywalking/KEYS

Guide to build the release from source :

 * https://github.com/apache/skywalking-banyandb/blob/v$VERSION/docs/installation/binaries.md#Build-From-Source

Voting will start now and will remain open for at least 72 hours, all PMC members are required to give their votes.

[ ] +1 Release this package.
[ ] +0 No opinion.
[ ] -1 Do not release this package because....

Thanks.

[1] https://github.com/apache/skywalking/blob/master/docs/en/guides/How-to-release.md#vote-check
EOF
