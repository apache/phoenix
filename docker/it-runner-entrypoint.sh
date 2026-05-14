#!/usr/bin/env bash
# Entrypoint executed inside the docker container by /run-it-tests-local.sh.
# Reads env vars set by the launcher and runs the right maven command.
#
# Env vars consumed:
#   PHOENIX_IT_PATTERN     glob for -Dit.test, e.g. 'BsonPathIndex*IT'
#   PHOENIX_IT_FORKS       integer, default 4
#   PHOENIX_HBASE_PROFILE  e.g. 2.5.4 (default)
#   PHOENIX_RUN_ALL        '1' to drop -Dit.test and run everything
#   PHOENIX_DO_CLEAN       '1' to run `mvn clean` first
#   PHOENIX_INSTALL_FIRST  '1' to run `mvn install -DskipTests` before failsafe
#   PHOENIX_EXTRA_ARGS     extra args appended to mvn (string)

set -euo pipefail

log() { printf '\n=== %s ===\n' "$*"; }

cd /work

if [[ -n "${PHOENIX_DO_CLEAN:-}" ]]; then
    log "mvn clean"
    mvn -B -q clean
fi

# Install dependencies once so the failsafe step doesn't have to rebuild upstream modules.
# Skip rat / spotbugs / enforcer / dependency-analyze — these are repo-hygiene checks unrelated
# to the test cluster and they can fail (e.g. bouncycastle declared/used drift) and abort the run.
if [[ -n "${PHOENIX_INSTALL_FIRST:-}" ]]; then
    log "mvn install -DskipTests (warm local repo, build all modules)"
    mvn -B install -DskipTests \
        -Dhbase.profile="${PHOENIX_HBASE_PROFILE:-2.5.4}" \
        -Dmaven.javadoc.skip=true \
        -Dspotbugs.skip=true \
        -Drat.skip=true \
        -Denforcer.skip=true \
        -Dmaven.dependency.plugin.skip=true \
        -DskipDependencyAnalyze=true \
        -Dmdep.analyze.skip=true \
        -Ddependency-check.skip=true
fi

VERIFY_ARGS=(
    -B -e
    -pl phoenix-core -am
    verify
    -DfailIfNoTests=false
    -DskipTests=false
    -Dhbase.profile="${PHOENIX_HBASE_PROFILE:-2.5.4}"
    -DnumForkedIT="${PHOENIX_IT_FORKS:-4}"
    -DnumForkedUT="${PHOENIX_UT_FORKS:-2}"
    -Dmaven.javadoc.skip=true
    -Dspotbugs.skip=true
    -Drat.skip=true
    -Denforcer.skip=true
    -Dskip.code-coverage=true
    -Dmaven.dependency.plugin.skip=true
    -Dmdep.analyze.skip=true
    -Ddependency-check.skip=true
)

# When -Dit.test is given, also skip surefire's unit-test phase entirely so
# we don't run thousands of unit tests on the way to the requested IT class.
if [[ -n "${PHOENIX_IT_PATTERN:-}" && -z "${PHOENIX_RUN_ALL:-}" ]]; then
    VERIFY_ARGS+=( -Dtest=NOTHING -Dsurefire.failIfNoSpecifiedTests=false )
fi

if [[ -z "${PHOENIX_RUN_ALL:-}" ]]; then
    if [[ -z "${PHOENIX_IT_PATTERN:-}" ]]; then
        echo "ERROR: PHOENIX_IT_PATTERN not set and PHOENIX_RUN_ALL not '1'" >&2
        exit 64
    fi
    VERIFY_ARGS+=( -Dit.test="${PHOENIX_IT_PATTERN}" )
fi

if [[ -n "${PHOENIX_EXTRA_ARGS:-}" ]]; then
    # shellcheck disable=SC2206
    EXTRA=( ${PHOENIX_EXTRA_ARGS} )
    VERIFY_ARGS+=( "${EXTRA[@]}" )
fi

log "mvn ${VERIFY_ARGS[*]}"
mvn "${VERIFY_ARGS[@]}"
