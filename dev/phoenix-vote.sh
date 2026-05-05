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

set -e -o pipefail

usage() {
  SCRIPT=$(basename "${BASH_SOURCE[@]}")

  cat << __EOF
phoenix-vote. A script for standard vote which verifies the following items
1. Checksum of sources and binaries
2. Signature of sources and binaries
3. Rat check
4. Built from source
5. Unit tests

Usage: ${SCRIPT} -s | --source <url> [-k | --key <signature>] [-f | --keys-file-url <url>] [-o | --output-dir </path/to/use>] [-D property[=value]]
       ${SCRIPT} -h | --help

  -h | --help                   Show this screen.
  -s | --source '<url>'         A URL pointing to the release candidate sources and binaries
                                e.g. https://dist.apache.org/repos/dist/dev/phoenix/phoenix-<version>RC0/
  -k | --key '<signature>'      A signature of the public key, e.g. 9AD2AE49
  -f | --keys-file-url '<url>'   the URL of the key file, default is
                                https://downloads.apache.org/phoenix/KEYS
  -o | --output-dir '</path>'   directory which has the stdout and stderr of each verification target
  -D |                          list of maven properties to set for the mvn invocations, i.e. <-D hbase.profile=2.4 -DskipTests> Defaults to unset
__EOF
}

MVN_PROPERTIES=()

while ((${#})); do
  case "${1}" in
    -h | --help )
      usage; exit 0 ;;
    -s | --source )
      SOURCE_URL="${2}"; shift 2 ;;
    -k | --key )
      SIGNING_KEY="${2}"; shift 2 ;;
    -f | --keys-file-url )
      KEY_FILE_URL="${2}"; shift 2 ;;
    -o | --output-dir )
      OUTPUT_DIR="${2}"; shift 2 ;;
    -D )
      MVN_PROPERTIES+=("-D ${2}"); shift 2 ;;
    * )
      usage >&2; exit 1             ;;
  esac
done

# Source url must be provided
if [ -z "${SOURCE_URL}" ]; then
  usage;
  exit 1
fi

cat << __EOF
Although This tool helps verifying Phoenix RC build and unit tests,
operator may still consider to verify the following manually
1. Verify the CHANGES.md and RELEASENOTES.md
2. Any on cluster Integration test or performance test
3. Other concerns if any
__EOF

if ! command -v wget >/dev/null 2>&1; then
    echo "ERROR: wget is required but was not found on PATH."
    echo "       On macOS install with: brew install wget"
    exit 1
fi

[[ "${SOURCE_URL}" != */ ]] && SOURCE_URL="${SOURCE_URL}/"
# Extract the trailing path component without depending on a fixed number of path segments.
PHOENIX_RC_VERSION=$(echo "${SOURCE_URL}" | sed -E 's|/+$||' | awk -F/ '{ print $NF }')
PHOENIX_VERSION=$(echo "${PHOENIX_RC_VERSION}" | sed -E 's/RC[0-9]+//' | sed -e 's/phoenix-//')
# Number of path components above the RC directory; wget --cut-dirs needs this
# to drop the leading host-relative path so files land under ./${PHOENIX_RC_VERSION}/.
URL_PATH=$(echo "${SOURCE_URL}" | sed -E 's|^https?://[^/]+/||; s|/+$||')
WGET_CUT_DIRS=$(echo "${URL_PATH%/*}" | tr -cd '/' | wc -c | tr -d ' ')
WGET_CUT_DIRS=$((WGET_CUT_DIRS + 1))
JAVA_VERSION=$(java -version 2>&1 | cut -f3 -d' ' | head -n1 | sed -e 's/"//g')
OUTPUT_DIR="${OUTPUT_DIR:-$(pwd)}"

# Create the output dir if it doesn't exist, then resolve to an absolute path so
# logs and downloaded artifacts stay together regardless of CWD.
mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"
cd "${OUTPUT_DIR}"

OUTPUT_PATH_PREFIX="${OUTPUT_DIR}"/"${PHOENIX_RC_VERSION}"

# default value for verification targets, 0 = failed
SIGNATURE_PASSED=0
CHECKSUM_PASSED=0
RAT_CHECK_PASSED=0
BUILD_FROM_SOURCE_PASSED=0
UNIT_TEST_PASSED=0

function download_and_import_keys() {
    KEY_FILE_URL="${KEY_FILE_URL:-https://downloads.apache.org/phoenix/KEYS}"
    echo "Obtain and import the publisher key(s) from ${KEY_FILE_URL}"
    # download the keys file into file KEYS
    wget -O KEYS "${KEY_FILE_URL}"
    gpg --import KEYS
    if [ -n "${SIGNING_KEY}" ]; then
        gpg --list-keys "${SIGNING_KEY}"
    fi
}

function download_release_candidate () {
    # dist.apache.org's robots.txt disallows /repos/, so wget would otherwise
    # stop after fetching the directory index. -e robots=off lets it follow the
    # links to the artifacts. -A scopes the recursion to release files only.
    wget -r -np -N -nH \
        -e robots=off \
        -A "*.tar.gz,*.tar.gz.asc,*.tar.gz.sha512,*.tar.gz.sha256,CHANGES.md,RELEASENOTES.md" \
        --cut-dirs "${WGET_CUT_DIRS}" "${SOURCE_URL}"
    if ! ls "${PHOENIX_RC_VERSION}"/*.tar.gz >/dev/null 2>&1; then
        echo "ERROR: no .tar.gz files were downloaded under ${OUTPUT_DIR}/${PHOENIX_RC_VERSION}/."
        echo "       Check that ${SOURCE_URL} is a valid release candidate URL."
        exit 1
    fi
}

function verify_signatures() {
    rm -f "${OUTPUT_PATH_PREFIX}"_verify_signatures
    SIGNATURE_PASSED=1
    for file in *.tar.gz; do
        if ! gpg --verify "${file}".asc "${file}" 2>&1 | tee -a "${OUTPUT_PATH_PREFIX}"_verify_signatures; then
            SIGNATURE_PASSED=0
        fi
    done
}

function verify_checksums() {
    rm -f "${OUTPUT_PATH_PREFIX}"_verify_checksums
    SHA_EXT=$(find . -name "*.sha*" | awk -F '.' '{ print $NF }' | head -n 1)
    CHECKSUM_PASSED=1
    for file in *.tar.gz; do
        gpg --print-md SHA512 "${file}" > "${file}"."${SHA_EXT}".tmp
        if ! diff "${file}"."${SHA_EXT}".tmp "${file}"."${SHA_EXT}" 2>&1 | tee -a "${OUTPUT_PATH_PREFIX}"_verify_checksums; then
            CHECKSUM_PASSED=0
        fi
        rm -f "${file}"."${SHA_EXT}".tmp
    done
}

function unzip_from_source() {
    tar -zxvf phoenix-"${PHOENIX_VERSION}"-src.tar.gz
    cd phoenix-"${PHOENIX_VERSION}"
}

function rat_test() {
    rm -f "${OUTPUT_PATH_PREFIX}"_rat_test
    mvn clean apache-rat:check "${MVN_PROPERTIES[@]}" 2>&1 | tee "${OUTPUT_PATH_PREFIX}"_rat_test && RAT_CHECK_PASSED=1
}

function build_from_source() {
    rm -f "${OUTPUT_PATH_PREFIX}"_build_from_source
    # Hardcode skipTests for faster build. Testing is covered later.
    mvn clean install "${MVN_PROPERTIES[@]}" -DskipTests 2>&1 | tee "${OUTPUT_PATH_PREFIX}"_build_from_source && BUILD_FROM_SOURCE_PASSED=1
}

function run_tests() {
    rm -f "${OUTPUT_PATH_PREFIX}"_run_tests
    (mvn clean package "${MVN_PROPERTIES[@]}" && mvn verify "${MVN_PROPERTIES[@]}") 2>&1 | tee "${OUTPUT_PATH_PREFIX}"_run_tests && UNIT_TEST_PASSED=1
}

function execute() {
   ${1} || print_when_exit
}

function print_when_exit() {
  cat << __EOF
        * Signature: $( ((SIGNATURE_PASSED)) && echo "ok" || echo "failed" )
        * Checksum : $( ((CHECKSUM_PASSED)) && echo "ok" || echo "failed" )
        * Rat check (${JAVA_VERSION}): $( ((RAT_CHECK_PASSED)) && echo "ok" || echo "failed" )
         - mvn clean apache-rat:check ${MVN_PROPERTIES[@]}
        * Built from source (${JAVA_VERSION}): $( ((BUILD_FROM_SOURCE_PASSED)) && echo "ok" || echo "failed" )
         - mvn clean install ${MVN_PROPERTIES[@]} -DskipTests
        * Unit tests pass (${JAVA_VERSION}): $( ((UNIT_TEST_PASSED)) && echo "ok" || echo "failed" )
         - mvn clean package ${MVN_PROPERTIES[@]} && mvn verify ${MVN_PROPERTIES[@]}
__EOF
  if ((CHECKSUM_PASSED)) && ((SIGNATURE_PASSED)) && ((RAT_CHECK_PASSED)) && ((BUILD_FROM_SOURCE_PASSED)) && ((UNIT_TEST_PASSED)) ; then
    exit 0
  fi
  exit 1
}

download_and_import_keys
download_release_candidate
pushd "${PHOENIX_RC_VERSION}"

execute verify_signatures
execute verify_checksums
execute unzip_from_source
execute rat_test
execute build_from_source
execute run_tests

popd

print_when_exit
