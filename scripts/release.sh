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
# Prevent `tar` from writing macOS AppleDouble (._*) and __MACOSX metadata into
# release archives. Without this, downstream users running `make generate`
# (which invokes `buf generate`) hit "invalid control character" errors when
# the macOS-only resource-fork files are picked up by protoc.
COPYFILE_DISABLE=1
export COPYFILE_DISABLE
SCRIPTDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOTDIR=${SCRIPTDIR}/..
BUILDDIR=${ROOTDIR}/build

RELEASE_TAG=$(git describe --tags $(git rev-list --tags --max-count=1))
RELEASE_VERSION=${RELEASE_TAG#"v"}
# Component Makefiles stamp pkg/version.build from RELEASE_VERSION. The binary()
# extract is not a git checkout, so this must be exported or every official
# executable is linked with build=-.
export RELEASE_VERSION

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
    RELEASE_VERSION="${RELEASE_VERSION}" make -C mcp release
    TARGET_OS=linux PLATFORMS=linux/amd64,linux/arm64 RELEASE_VERSION="${RELEASE_VERSION}" make -C banyand release
    TARGET_OS=linux PLATFORMS=linux/amd64,linux/arm64 RELEASE_VERSION="${RELEASE_VERSION}" make -C fodc/agent release
    TARGET_OS=linux PLATFORMS=linux/amd64,linux/arm64 RELEASE_VERSION="${RELEASE_VERSION}" make -C fodc/proxy release
    bindir=./build
    stage_binary_package banyand banyand --with-mcp
    tar -czf ${BUILDDIR}/skywalking-banyandb-${RELEASE_VERSION}-banyand.tgz \
      --exclude="._*" --exclude="__MACOSX" \
      -C ${bindir} .

    # Cross compile bydbctl
    TARGET_OS=linux PLATFORMS=linux/amd64,linux/arm64,linux/386 RELEASE_VERSION="${RELEASE_VERSION}" make -C bydbctl release
    TARGET_OS=windows PLATFORMS=windows/amd64,windows/386 RELEASE_VERSION="${RELEASE_VERSION}" make -C bydbctl release
    TARGET_OS=darwin PLATFORMS=darwin/amd64,darwin/arm64 RELEASE_VERSION="${RELEASE_VERSION}" make -C bydbctl release
    stage_binary_package bydbctl bydbctl
    tar -czf ${BUILDDIR}/skywalking-banyandb-${RELEASE_VERSION}-bydbctl.tgz \
      --exclude="._*" --exclude="__MACOSX" \
      -C ${bindir} .

    stage_binary_package fodc/agent fodc-agent
    tar -czf ${BUILDDIR}/skywalking-banyandb-${RELEASE_VERSION}-fodc-agent.tgz \
      --exclude="._*" --exclude="__MACOSX" \
      -C ${bindir} .

    stage_binary_package fodc/proxy fodc-proxy
    tar -czf ${BUILDDIR}/skywalking-banyandb-${RELEASE_VERSION}-fodc-proxy.tgz \
      --exclude="._*" --exclude="__MACOSX" \
      -C ${bindir} .
}

stage_binary_package() {
    local module=$1
    local pkg=$2
    local with_mcp=0
    local extra_args=()
    if [ "${3:-}" = "--with-mcp" ]; then
        with_mcp=1
    fi
    echo "Staging ${pkg} package"
    rm -rf "${bindir}"
    mkdir -p "${bindir}/bin"
    copy_binaries "${module}"
    cp -Rfv ./CHANGES.md "${bindir}"
    cp -Rfv ./README.md "${bindir}"
    cp -Rfv ./dist/NOTICE "${bindir}"
    if [ "${with_mcp}" -eq 1 ]; then
        mkdir -p "${bindir}/mcp"
        cp -Rfv ./mcp/dist "${bindir}/mcp/"
        cp -Rfv ./mcp/package.json "${bindir}/mcp/"
        extra_args+=(
            --extra-license ./ui/LICENSE
            --extra-licenses-dir ./dist/licenses/ui-licenses
            --extra-license ./mcp/LICENSE
            --extra-licenses-dir ./dist/licenses/mcp-licenses
        )
    fi
    python3 ./scripts/package-licenses.py \
        --license ./dist/LICENSE \
        --licenses-dir ./dist/licenses \
        --bins "${bindir}/bin" \
        --out "${bindir}" \
        "${extra_args[@]}"
}

copy_binaries() {
    local module=$1
    # Filter out lock files AND macOS AppleDouble resource-fork files (._*) that
    # bsdtar emits when run on macOS; otherwise they leak into release tarballs
    # and break `buf generate` in downstream rebuilds.
    find ./${module}/build/bin \
        -type f \
        -not -name "*.lock" \
        -not -name "._*" \
        -not -path "*/__MACOSX/*" | while read -r binary
    do
        # Extract os and arch from the path
        os_arch=$(echo ${binary} | awk -F'/' '{print $(NF-2)"/"$(NF-1)}')
        binary_name=$(basename ${binary})
        cp -Rfv ${binary} ${bindir}/bin/${binary_name}-${os_arch//\//-}
    done
}

source(){
    # Package only the git tree (plus .env) so untracked/local binaries cannot leak
    # into the Apache source archive.
    tmpdir=`mktemp -d`
    trap "rm -rf ${tmpdir}" EXIT
    rm -rf ${SOURCE_FILE}
    srcdir=${tmpdir}/src
    mkdir -p "${srcdir}"
    pushd ${ROOTDIR}
    git archive --format=tar HEAD | tar -x -C "${srcdir}"
    echo "RELEASE_VERSION=${RELEASE_VERSION}" > "${srcdir}/.env"
    tar \
    --exclude=".DS_Store" \
    --exclude="._*" \
    --exclude="__MACOSX" \
    --exclude=".github" \
    --exclude=".gitignore" \
    --exclude=".asf.yaml" \
    --exclude=".idea" \
    --exclude=".vscode" \
    --exclude="bin" \
    -czf ${tmpdir}/${SOURCE_FILE_NAME} \
    -C "${srcdir}" .

    checkdir=${tmpdir}/check
    mkdir -p "${checkdir}"
    tar -xzf ${tmpdir}/${SOURCE_FILE_NAME} -C "${checkdir}"
    if find "${checkdir}" -type f -print0 | xargs -0 file | grep -E 'ELF |Mach-O '; then
        echo "ERROR: source archive contains compiled binaries" >&2
        exit 1
    fi

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
