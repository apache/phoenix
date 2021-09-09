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
# Based on hbase-personality.sh of the HBase project
#
# You'll need a local installation of
# [Apache Yetus' precommit checker](http://yetus.apache.org/documentation/0.12.0/#yetus-precommit)
# to use this personality.
#
# Download from: http://yetus.apache.org/downloads/ . You can either grab the source artifact and
# build from it, or use the convenience binaries provided on that download page.
#
# To run against, e.g. PHOENIX-5032 you'd then do
# ```bash
# test-patch --personality=dev-support/hbase-personality.sh HBASE-15074
# ```
#
# pass the `--sentinel` flag if you want to allow test-patch to destructively alter local working
# directory / branch in order to have things match what the issue patch requests.

personality_plugins "all"

if ! declare -f "yetus_info" >/dev/null; then

  function yetus_info
  {
    echo "[$(date) INFO]: $*" 1>&2
  }

fi

# work around yetus overwriting JAVA_HOME from our docker image
#function docker_do_env_adds
#{
#  declare k
#  for k in "${DOCKER_EXTRAENVS[@]}"; do
#    if [[ "JAVA_HOME" == "${k}" ]]; then
#      if [ -n "${JAVA_HOME}" ]; then
#        DOCKER_EXTRAARGS+=("--env=JAVA_HOME=${JAVA_HOME}")
#      fi
#    else
#      DOCKER_EXTRAARGS+=("--env=${k}=${!k}")
#    fi
#  done
#}


## @description  Globals specific to this personality
## @audience     private
## @stability    evolving
function personality_globals
{
  BUILDTOOL=maven
  #shellcheck disable=SC2034
  PROJECT_NAME=phoenix
  #shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=master
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^PHOENIX-[0-9]+$'
  #shellcheck disable=SC2034
  GITHUB_REPO="apache/phoenix"

  # TODO use PATCH_BRANCH to select jdk versions to use.

  # ASF Jenkins workers run up to two jobs per Agent. Docker doesn't do anything with nproc,
  # any setting is simply set on the docker daemon user, and shared between containers.
  # Thus, there is no way to protect containers from fork-bombing each other.
  # Set nprocs higher than than the combined process count of two jobs.
  # see https://docs.docker.com/engine/reference/commandline/run/#set-ulimits-in-container-ulimit
  # Note that this won't stop a container started after us from resetting the limit to a
  # lower value, but should help if we are started later.
  #shellcheck disable=SC2034
  PROC_LIMIT=30000

  # Set docker container to run with 20g. Default is 4g in yetus.
  # See HBASE-19902 for how we arrived at 20g.
  # TODO Doesn't seem to have effect in Yetus 0.12, set in cli instead
  #shellcheck disable=SC2034
  DOCKERMEMLIMIT=20g

  # Extremely evil in-memory patching of the maven module to use mvn verify instead of mvn test
  # This is likely to break on Yetus upgrade
  maven_modules_worker_definition=$(declare -f maven_modules_worker)
  maven_modules_worker_definition=${maven_modules_worker_definition//unit clean test/unit clean verify}
  eval "$maven_modules_worker_definition"

  yetus_debug "patched maven_modules_worker_definition. New function:\n${maven_modules_worker_definition}"
}

## @description  Parse extra arguments required by personalities, if any.
## @audience     private
## @stability    evolving
function personality_parse_args
{
  declare i

  for i in "$@"; do
    case ${i} in
      --exclude-tests-url=*)
        delete_parameter "${i}"
        EXCLUDE_TESTS_URL=${i#*=}
      ;;
      --include-tests-url=*)
        delete_parameter "${i}"
        INCLUDE_TESTS_URL=${i#*=}
      ;;
      --hbase-profile=*)
        delete_parameter "${i}"
        HBASE_PROFILE=${i#*=}
      ;;
      --skip-errorprone)
        delete_parameter "${i}"
        SKIP_ERRORPRONE=true
      ;;
    esac
  done
}

## @description  Queue up modules for this personality
## @audience     private
## @stability    evolving
## @param        repostatus
## @param        testtype
function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""
  local jdk8module=""
  local MODULES=("${CHANGED_MODULES[@]}")

  yetus_info "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  # Running with threads>1 seems to trigger some problem in the build, but since we
  # spend 80+% of the time in phoenix-core, it wouldn't help much anyway

  # The PhoenixPatchProcess property disables creating the shaded artifacts and assembly
  # We have no tests for those, but they add 45+ minutes to the Yetus runtime.

  # I have been unable to get Jacoco running reliably on ASF Jenkins, thus it is disabled.

  extra="--threads=1 -DPhoenixPatchProcess -Dskip.code-coverage "
  if [[ "${PATCH_BRANCH}" = 4* ]]; then
    extra="${extra} -Dhttps.protocols=TLSv1.2"
  fi

  # If we have HBASE_PROFILE specified pass along
  # the hbase.profile system property.
  if [[ -n "${HBASE_PROFILE}" ]] ; then
    extra="${extra} -Dhbase.profile=${HBASE_PROFILE}"
  fi

  # BUILDMODE value is 'full' when there is no patch to be tested, and we are running checks on
  # full source code instead. In this case, do full compiles, tests, etc instead of per
  # module.
  # Used in nightly runs.
  # If BUILDMODE is 'patch', for unit and compile testtypes, there is no need to run individual
  # modules if root is included. HBASE-18505
  if [[ "${BUILDMODE}" == "full" ]] || \
     { { [[ "${testtype}" == unit ]] || [[ "${testtype}" == compile ]] || [[ "${testtype}" == checkstyle ]]; } && \
     [[ "${MODULES[*]}" =~ \. ]]; }; then
    MODULES=(.)
  fi

  # If the checkstyle configs change, check everything.
  if [[ "${testtype}" == checkstyle ]] && [[ "${MODULES[*]}" =~ hbase-checkstyle ]]; then
    MODULES=(.)
  fi

  if [[ ${testtype} == mvninstall ]]; then
    # shellcheck disable=SC2086
    personality_enqueue_module . ${extra}
    return
  fi

  if [[ ${testtype} == spotbugs ]]; then
    # Run spotbugs on each module individually to diff pre-patch and post-patch results and
    # report new warnings for changed modules only.
    # For some reason, spotbugs on root is not working, but running on individual modules is
    # working. For time being, let it run on original list of CHANGED_MODULES. HBASE-19491
    for module in "${CHANGED_MODULES[@]}"; do
      # skip spotbugs on any module that lacks content in `src/main/java`
      if [[ "$(find "${BASEDIR}/${module}" -iname '*.java' -and -ipath '*/src/main/java/*' \
          -type f | wc -l | tr -d '[:space:]')" -eq 0 ]]; then
        yetus_debug "no java files found under ${module}/src/main/java. skipping."
        continue
      else
        # shellcheck disable=SC2086
        personality_enqueue_module ${module} ${extra}
      fi
    done
    return
  fi

  if [[ ${testtype} == compile ]] && [[ "${SKIP_ERRORPRONE}" != "true" ]] &&
      [[ "${PATCH_BRANCH}" != branch-1* ]] ; then
    extra="${extra} -PerrorProne"
  fi

  # If EXCLUDE_TESTS_URL/INCLUDE_TESTS_URL is set, fetches the url
  # and sets -Dtest.exclude.pattern/-Dtest to exclude/include the
  # tests respectively.
  if [[ ${testtype} == unit ]]; then
    local tests_arg=""
    get_include_exclude_tests_arg tests_arg
    # We have patched the maven module above to run mvn verify instead of mvn test
    extra="${extra} ${tests_arg}"

    # Inject the jenkins build-id for our surefire invocations
    # Used by zombie detection stuff, even though we're not including that yet.
    if [ -n "${BUILD_ID}" ]; then
      extra="${extra} -Dbuild.id=${BUILD_ID}"
    fi

  fi

  for module in "${MODULES[@]}"; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}

## @description places where we override the built in assumptions about what tests to run
## @audience    private
## @stability   evolving
## @param       filename of changed file
function personality_file_tests
{
  local filename=$1
  yetus_debug "Phoenix specific personality_file_tests"
  # If we change checkstyle configs, run checkstyle
  if [[ ${filename} =~ checkstyle.*\.xml ]]; then
    yetus_debug "tests/checkstyle: ${filename}"
    add_test checkstyle
  fi
  # fallback to checking which tests based on what yetus would do by default
  if declare -f "${BUILDTOOL}_builtin_personality_file_tests" >/dev/null; then
    "${BUILDTOOL}_builtin_personality_file_tests" "${filename}"
  elif declare -f builtin_personality_file_tests >/dev/null; then
    builtin_personality_file_tests "${filename}"
fi
}

## @description  Uses relevant include/exclude env variable to fetch list of included/excluded
#                tests and sets given variable to arguments to be passes to maven command.
## @audience     private
## @stability    evolving
## @param        name of variable to set with maven arguments
function get_include_exclude_tests_arg
{
  #Phoenix doesn't support this yet, but should
  return
  local  __resultvar=$1
  yetus_info "EXCLUDE_TESTS_URL=${EXCLUDE_TESTS_URL}"
  yetus_info "INCLUDE_TESTS_URL=${INCLUDE_TESTS_URL}"
  if [[ -n "${EXCLUDE_TESTS_URL}" ]]; then
      if wget "${EXCLUDE_TESTS_URL}" -O "excludes"; then
        excludes=$(cat excludes)
        yetus_debug "excludes=${excludes}"
        if [[ -n "${excludes}" ]]; then
          eval "${__resultvar}='-Dtest.exclude.pattern=${excludes}'"
        fi
        rm excludes
      else
        yetus_error "Wget error $? in fetching excludes file from url" \
             "${EXCLUDE_TESTS_URL}. Ignoring and proceeding."
      fi
  elif [[ -n "$INCLUDE_TESTS_URL" ]]; then
      if wget "$INCLUDE_TESTS_URL" -O "includes"; then
        includes=$(cat includes)
        yetus_debug "includes=${includes}"
        if [[ -n "${includes}" ]]; then
          eval "${__resultvar}='-Dtest=${includes}'"
        fi
        rm includes
      else
        yetus_error "Wget error $? in fetching includes file from url" \
             "${INCLUDE_TESTS_URL}. Ignoring and proceeding."
      fi
  else
    # Use branch specific exclude list when EXCLUDE_TESTS_URL and INCLUDE_TESTS_URL are empty
    FLAKY_URL="https://ci-hadoop.apache.org/job/HBase/job/HBase-Find-Flaky-Tests/job/${PATCH_BRANCH}/lastSuccessfulBuild/artifact/excludes/"
    if wget "${FLAKY_URL}" -O "excludes"; then
      excludes=$(cat excludes)
        yetus_debug "excludes=${excludes}"
        if [[ -n "${excludes}" ]]; then
          eval "${__resultvar}='-Dtest.exclude.pattern=${excludes}'"
        fi
        rm excludes
      else
        yetus_error "Wget error $? in fetching excludes file from url" \
             "${FLAKY_URL}. Ignoring and proceeding."
      fi
  fi
}

######################################

add_test_type hbaserebuild

## @description  hbaserebuild file filter
## @audience     private
## @stability    evolving
## @param        filename
function hbaserebuild_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test hbaserebuild
  fi
}

## @description  hbaserebuild precheck
## @audience     private
## @stability    evolving
## @param        none
function hbaserebuild_precompile
{
  big_console_header "Reinstalling HBase with Hadoop 3"

  if [[ -n "${HBASE_REBUILD_DONE}" ]]; then
    return
  fi

  MAVEN_LOCAL_REPO="$MAVEN_LOCAL_REPO" ${BASEDIR}/dev/rebuild_hbase.sh detect

  HBASE_REBUILD_DONE=1

  add_vote_table +0 hbaserecompile "" "HBase recompiled."
  return 0
}

######################################

add_test_type hbaseanti

## @description  hbaseanti file filter
## @audience     private
## @stability    evolving
## @param        filename
function hbaseanti_filefilter
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test hbaseanti
  fi
}

## @description  hbaseanti patch file check
## @audience     private
## @stability    evolving
## @param        filename
function hbaseanti_patchfile
{
  local patchfile=$1
  local warnings
  local result

  if [[ "${BUILDMODE}" = full ]]; then
    return 0
  fi

  if ! verify_needed_test hbaseanti; then
    return 0
  fi

  big_console_header "Checking for known anti-patterns"

  start_clock

  warnings=$(${GREP} -c 'new TreeMap<byte.*()' "${patchfile}")
  if [[ ${warnings} -gt 0 ]]; then
    add_vote_table -1 hbaseanti "" "The patch appears to have anti-pattern where BYTES_COMPARATOR was omitted."
    ((result=result+1))
  fi

  if [[ ${result} -gt 0 ]]; then
    return 1
  fi

  add_vote_table +1 hbaseanti "" "Patch does not have any anti-patterns."
  return 0
}

## @description  process the javac output for generating WARNING/ERROR
## @audience     private
## @stability    evolving
## @param        input filename
## @param        output filename
# Override the default javac_logfilter so that we can do a sort before outputing the WARNING/ERROR.
# This is because that the output order of the error prone warnings is not stable, so the diff
# method will report unexpected errors if we do not sort it. Notice that a simple sort will cause
# line number being sorted by lexicographical so the output maybe a bit strange to human but it is
# really hard to sort by file name first and then line number and column number in shell...
function hbase_javac_logfilter
{
  declare input=$1
  declare output=$2

  ${GREP} -E '\[(ERROR|WARNING)\] /.*\.java:' "${input}" | sort > "${output}"
}
