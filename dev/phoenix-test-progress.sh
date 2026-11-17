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

# Prints the progress of the tests while a Yetus unit run is running. Yetus writes the Maven
# output of the unit tests to <patch dir>/patch-unit-<module>.txt instead of the console, so
# this follows that file and prints, with the time, the start and the result of each test class,
# the failed tests, and the build result. Run it in the background, and stop it with SIGTERM when
# Yetus is done.
#
# Usage: phoenix-test-progress.sh <Yetus patch dir>

set -uo pipefail

patchdir=$1

# Stop the tail and grep below too.
trap 'pkill -P $$; exit 0' TERM INT

# Yetus creates the file when it starts the unit tests, after the build.
while ! compgen -G "${patchdir}/patch-unit-*.txt" > /dev/null; do
  sleep 5
done

# For example:
#   [INFO] Running org.apache.phoenix.end2end.FooIT
#   [INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.1 s -- in <class>
#   [ERROR] org.apache.phoenix.end2end.FooIT.testBar -- Time elapsed: 3.2 s <<< FAILURE!
tail -F -n +1 "${patchdir}"/patch-unit-*.txt 2> /dev/null \
  | grep --line-buffered -E \
    '^\[[A-Z]+\] (Running [a-z]|Tests run: )|<<< (FAILURE|ERROR)!|BUILD (SUCCESS|FAILURE)' \
  | while IFS= read -r line; do
    echo "$(date -u +%H:%M:%S) ${line}"
  done &
wait
