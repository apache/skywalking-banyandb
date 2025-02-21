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

SOURCE_FILE_NAME=skywalking-banyandb-${RELEASE_VERSION}-src.tgz
SOURCE_FILE=${BUILDDIR}/${SOURCE_FILE_NAME}

binary(){
    if [ ! -f "${SOURCE_FILE}" ]; then
        echo "$FILE exists."
        exit 1
    fi
    tmpdir=`mktemp -d`
    trap "rm -rf ${tmpdir}" EXIT
    pushd ${tmpdir}
    trap 'popd' EXIT
    tar -xvf ${SOURCE_FILE}
    make generate && make -C ui build
    TARGET_OS=linux PLATFORMS=linux/amd64,linux/arm64 make -C banyand release
    bindir=./build
    mkdir -p ${bindir}/bin
    # Copy relevant files
    copy_binaries banyand
    cp -Rfv ./CHANGES.md ${bindir}
    cp -Rfv ./README.md ${bindir}
    cp -Rfv ./dist/* ${bindir}
    # Package
    tar -czf ${BUILDDIR}/skywalking-banyandb-${RELEASE_VERSION}-banyand.tgz -C ${bindir} .

    # Cross compile bydbctl
    TARGET_OS=linux PLATFORMS=linux/amd64,linux/arm64,linux/386 make -C bydbctl release
    TARGET_OS=windows PLATFORMS=windows/amd64,windows/386 make -C bydbctl release
    TARGET_OS=darwin PLATFORMS=darwin/amd64,darwin/arm64 make -C bydbctl release
    rm -rf ${bindir}/bin
    mkdir -p ${bindir}/bin
    # Copy relevant files
    copy_binaries bydbctl
    # Package
    tar -czf ${BUILDDIR}/skywalking-banyandb-${RELEASE_VERSION}-bydbctl.tgz -C ${bindir} .
}

copy_binaries() {
    local module=$1
    find ./${module}/build/bin -type f -not -name "*.lock" | while read -r binary
    do
        # Extract os and arch from the path
        os_arch=$(echo ${binary} | awk -F'/' '{print $(NF-2)"/"$(NF-1)}')
        binary_name=$(basename ${binary})
        cp -Rfv ${binary} ${bindir}/bin/${binary_name}-${os_arch//\//-}
    done
}

source(){
    # Package
    tmpdir=`mktemp -d`
    trap "rm -rf ${tmpdir}" EXIT
    rm -rf ${SOURCE_FILE}
    pushd ${ROOTDIR}
    echo "RELEASE_VERSION=${RELEASE_VERSION}" > .env
    tar \
    --exclude=".DS_Store" \
    --exclude=".github" \
    --exclude=".gitignore" \
    --exclude=".asf.yaml" \
    --exclude=".idea" \
    --exclude=".vscode" \
    --exclude="bin" \
    -czf ${tmpdir}/${SOURCE_FILE_NAME} \
    .

    mkdir -p ${BUILDDIR}
    mv ${tmpdir}/${SOURCE_FILE_NAME} ${BUILDDIR}
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
