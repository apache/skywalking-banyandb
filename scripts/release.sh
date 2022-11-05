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
#

set -ex
SCRIPTDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOTDIR=${SCRIPTDIR}/..
BUILDDIR=${ROOTDIR}/build

RELEASE_TAG=$(git describe --tags $(git rev-list --tags --max-count=1))
RELEASE_VERSION=${RELEASE_TAG#"v"}

SOURCE_FILE=${BUILDDIR}/skywalking-banyandb-${RELEASE_VERSION}-src.tgz

binary(){
    if [ ! -f "${SOURCE_FILE}" ]; then
        echo "$FILE exists."
        exit 1
    fi
    tmpdir=`mktemp -d`
    trap "rm -rf ${tmpdir}" EXIT
    pushd ${tmpdir}
    tar -xvf ${SOURCE_FILE}
    make generate
    make release
    bindir=./build
    mkdir -p ${bindir}/bin
    # Copy relevant files
    for module in "banyand" "bydbctl"
    do
        cp -Rfv ./${module}/build/bin/* ${bindir}/bin
    done
    cp -Rfv ./CHANGES.md ${bindir}
    cp -Rfv ./README.md ${bindir}
    cp -Rfv ./dist/* ${bindir}
    # Package
    tar -czf ${BUILDDIR}/skywalking-banyandb-${RELEASE_VERSION}-bin.tgz -C ${bindir} .
    popd
}

source(){
    # Package
    mkdir -p ${BUILDDIR}
    rm -rf ${SOURCE_FILE}
    pushd ${ROOTDIR}
    echo "RELEASE_VERSION=${RELEASE_VERSION}" > .env
    tar \
    --exclude=".DS_Store" \
    --exclude=".git" \
    --exclude=".github" \
    --exclude=".gitignore" \
    --exclude=".asf.yaml" \
    --exclude=".idea" \
    --exclude=".vscode" \
    --exclude="./build" \
    --exclude="bin" \
    -czf ${SOURCE_FILE} \
    .
    popd
}

sign(){
    type=$1
    pushd ${BUILDDIR}
    gpg --batch --yes --armor --detach-sig skywalking-banyandb-${RELEASE_VERSION}-${type}.tgz
    shasum -a 512 skywalking-banyandb-${RELEASE_VERSION}-${type}.tgz > skywalking-banyandb-${RELEASE_VERSION}-${type}.tgz.sha512
    popd
}

parseCmdLine(){
    ARGS=$1
    if [ $# -eq 0 ]; then
        echo "Exactly one argument required."
        usage
    fi
    while getopts  "bsk:h" FLAG; do
        case "${FLAG}" in
            b) binary ;;
            s) source ;;
            k) sign ${OPTARG} ;;
            h) usage ;;
            \?) usage ;;
        esac
    done
    return 0
}



usage() {
cat <<EOF
Usage:
    ${0} -[bsh]

Parameters:
    -b  Build and assemble the binary package
    -s  Assemble the source package
    -h  Show this help.
EOF
    exit 1
}

#
# main
#

ret=0

parseCmdLine "$@"
ret=$?
[ $ret -ne 0 ] && exit $ret
echo "Done release ${RELEASE_VERSION} (exit $ret)"
