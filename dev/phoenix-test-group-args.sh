#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Prints, one per line, the Maven arguments that limit a build to one test group, so that the
# test groups can run as separate CI jobs. The groups are the surefire unit tests and the
# failsafe executions defined in the root pom.xml.
#
# Usage: phoenix-test-group-args.sh <group> [<first letter>-<last letter>]
#
#   group   all | unit | ParallelStatsDisabledTest | ParallelStatsEnabledTest |
#           NeedsOwnMiniClusterTest
#   range   optional, e.g. A-G: only run the integration test classes whose name starts with a
#           letter in this range. Only valid for the integration test groups.
#
# "all" prints nothing, so the build runs every test, as it did before the split.

set -e

group=$1
range=$2

usage() {
  echo "Usage: $0 <all|unit|ParallelStatsDisabledTest|ParallelStatsEnabledTest|NeedsOwnMiniClusterTest> [A-Z range, e.g. A-G]" >&2
  exit 1
}

# Skips every unit test. surefire.failIfNoSpecifiedTests is false in the root pom.xml, so a test
# name that matches nothing runs no unit tests and does not fail the build.
SKIP_UNIT_TESTS="-Dtest=NoUnitTestsInThisTestGroup"

case "${group}" in
  all)
    ;;
  unit)
    echo "-DskipITs"
    ;;
  ParallelStatsDisabledTest)
    echo "${SKIP_UNIT_TESTS}"
    echo "-DskipParallelStatsEnabledTests=true"
    echo "-DskipNeedsOwnMiniClusterTests=true"
    ;;
  ParallelStatsEnabledTest)
    echo "${SKIP_UNIT_TESTS}"
    echo "-DskipParallelStatsDisabledTests=true"
    echo "-DskipNeedsOwnMiniClusterTests=true"
    ;;
  NeedsOwnMiniClusterTest)
    echo "${SKIP_UNIT_TESTS}"
    echo "-DskipParallelStatsEnabledTests=true"
    echo "-DskipParallelStatsDisabledTests=true"
    ;;
  *)
    usage
    ;;
esac

if [[ -n "${range}" ]]; then
  if [[ "${group}" == all ]] || [[ "${group}" == unit ]]; then
    echo "[ERROR] A class name range only applies to an integration test group." >&2
    usage
  fi
  if [[ ! "${range}" =~ ^[A-Z]-[A-Z]$ ]]; then
    echo "[ERROR] Invalid class name range '${range}'." >&2
    usage
  fi
  first=${range%-*}
  last=${range#*-}
  if [[ "${first}" > "${last}" ]]; then
    echo "[ERROR] Invalid class name range '${range}': '${first}' is after '${last}'." >&2
    usage
  fi
  patterns=""
  for letter in {A..Z}; do
    if [[ ! "${letter}" < "${first}" ]] && [[ ! "${letter}" > "${last}" ]]; then
      patterns="${patterns:+${patterns},}${letter}*IT"
    fi
  done
  # -Dit.test replaces the failsafe includes, so only classes ending in IT are selected. Every
  # integration test class in the repository ends in IT. it.failIfNoSpecifiedTests is false in
  # the root pom.xml, so modules without a matching integration test still pass.
  echo "-Dit.test=${patterns}"
fi
