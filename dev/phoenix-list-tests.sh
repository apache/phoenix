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

# Prints the test classes that a Yetus unit run executed, with their results, so that they can be
# seen in the CI console without downloading the build artifacts. Yetus writes the Maven output
# of the unit tests to <patch dir>/patch-unit-<module>.txt instead of the console.
#
# Usage: phoenix-list-tests.sh <Yetus patch dir> [<class list file>]
#
# If a class list file is given, the names of the test classes run are also written to it, one
# per line, for dev/phoenix_check_tests.py. If Yetus ran no unit tests, the file has the single
# line "# no tests run". If the Maven build failed, the file also has the line "# build failed".

set -euo pipefail
shopt -s nullglob

patchdir=$1
class_list=${2:-}
logs=("${patchdir}"/patch-unit-*.txt)
if [[ ${#logs[@]} -eq 0 ]]; then
  echo "Yetus ran no unit tests for this PR."
  if [[ -n "${class_list}" ]]; then
    echo "# no tests run" > "${class_list}"
  fi
  exit 0
fi

# Surefire and failsafe print one line per test class, for example:
#   Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.1 s -- in <class>
#   Tests run: 3, Failures: 1, ..., Time elapsed: 5 s <<< FAILURE! -- in <class>
results=$(grep -hE 'Tests run: [0-9]+, .* in [A-Za-z0-9_.$]+$' "${logs[@]}" \
  | sed -E 's/^\[[A-Z]+\] //' || true)
classes=$(grep -c . <<< "${results}" || true)

# On GitHub Actions the list is folded, so that the console stays short.
[[ "${GITHUB_ACTIONS:-}" == "true" ]] && echo "::group::Test classes run (${classes})"
[[ "${GITHUB_ACTIONS:-}" != "true" ]] && echo "Test classes run (${classes}):"
if [[ -n "${results}" ]]; then
  awk -F' -- in | in ' 'NF > 1 { printf "%-90s %s\n", $NF, $1 }' <<< "${results}" | sort
fi
[[ "${GITHUB_ACTIONS:-}" == "true" ]] && echo "::endgroup::"

if [[ -n "${class_list}" ]]; then
  if [[ -n "${results}" ]]; then
    awk -F' -- in | in ' 'NF > 1 { print $NF }' <<< "${results}" | sort -u > "${class_list}"
  else
    : > "${class_list}"
  fi
fi

failed=$(grep -E '<<< (FAILURE|ERROR)!' <<< "${results}" || true)
# Maven skips the modules after a module with failed tests, so their classes do not run.
if [[ -n "${class_list}" ]] && grep -qE '^\[[A-Z]+\] BUILD FAILURE' "${logs[@]}"; then
  echo "# build failed" >> "${class_list}"
fi
if [[ -n "${failed}" ]]; then
  echo
  echo "Test classes with failures or errors:"
  awk -F' -- in | in ' 'NF > 1 { printf "%-90s %s\n", $NF, $1 }' <<< "${failed}" | sort
fi

# The totals of each surefire and failsafe run.
echo
grep -hE '^\[[A-Z]+\] Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+$' \
  "${logs[@]}" | sed -E 's/^\[[A-Z]+\] /Total: /' || true
