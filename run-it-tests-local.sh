#!/usr/bin/env bash
# run-it-tests-local.sh — set up and run Phoenix integration tests locally.
#
# By default, on macOS this runs the test suite inside a Linux Docker container
# because direct execution on Darwin currently hits a Netty/macOS TCP_NODELAY
# bug that prevents the embedded HBase mini-cluster from starting.
#
# Usage:
#   ./run-it-tests-local.sh                          # smoke run: BSON-path ITs, docker mode, 4 forks
#   ./run-it-tests-local.sh --it 'PhoenixTestDriverIT'
#   ./run-it-tests-local.sh --it 'BsonPathIndex*IT' --forks 2
#   ./run-it-tests-local.sh --all                    # full IT suite (hours)
#   ./run-it-tests-local.sh --shell                  # interactive shell in the container
#   ./run-it-tests-local.sh --mode host --force-host # run on this host (Linux recommended)
#
# See runtestLocalsetup.md for the verified design.

set -euo pipefail

REPO_ROOT="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
cd "${REPO_ROOT}"

# ---------- defaults ----------
DEFAULT_IT_PATTERN='BsonPathIndex*IT'
DEFAULT_FORKS=4
DEFAULT_HBASE_PROFILE='2.5.4'
IMAGE_TAG='phoenix-it-runner:local'
CONTAINER_NAME='phoenix-it-runner'

IT_PATTERN="${DEFAULT_IT_PATTERN}"
FORKS="${DEFAULT_FORKS}"
HBASE_PROFILE="${DEFAULT_HBASE_PROFILE}"
RUN_ALL=''
DO_CLEAN=''
KEEP_LOGS=''
INSTALL_FIRST='1'      # default: warm the maven cache once per container
SHELL_MODE=''
FORCE_HOST=''
EXTRA_ARGS=''

# default mode: docker on Darwin, host on Linux
case "$(uname -s)" in
    Darwin) MODE='docker' ;;
    Linux)  MODE='host' ;;
    *)      MODE='docker' ;;
esac

# ---------- args ----------
print_help() {
    sed -n '2,18p' "$0"
    cat <<EOH

Options:
  --it PATTERN          -Dit.test glob (default: ${DEFAULT_IT_PATTERN})
  --all                 run the full IT suite (drops --it)
  --forks N             numForkedIT (default: ${DEFAULT_FORKS})
  --hbase-profile P     -Dhbase.profile (default: ${DEFAULT_HBASE_PROFILE})
  --mode {docker|host}  default: docker on macOS, host on Linux
  --force-host          required to run --mode host on Darwin
  --clean               run 'mvn clean' before tests
  --no-install          skip the warm-up 'mvn install -DskipTests' step
  --keep-logs           don't delete target/failsafe-reports before run
  --shell               drop into the container shell (docker mode only)
  --extra "ARGS"        extra args appended to the inner mvn command
  -h | --help           show this help

EOH
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --it) IT_PATTERN="$2"; shift 2 ;;
        --all) RUN_ALL='1'; shift ;;
        --forks) FORKS="$2"; shift 2 ;;
        --hbase-profile) HBASE_PROFILE="$2"; shift 2 ;;
        --mode) MODE="$2"; shift 2 ;;
        --force-host) FORCE_HOST='1'; shift ;;
        --clean) DO_CLEAN='1'; shift ;;
        --no-install) INSTALL_FIRST=''; shift ;;
        --keep-logs) KEEP_LOGS='1'; shift ;;
        --shell) SHELL_MODE='1'; MODE='docker'; shift ;;
        --extra) EXTRA_ARGS="$2"; shift 2 ;;
        -h|--help) print_help; exit 0 ;;
        *) echo "unknown arg: $1" >&2; print_help; exit 64 ;;
    esac
done

# ---------- helpers ----------
TS="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="${REPO_ROOT}/it-run.${TS}.log"

log()  { printf '\n=== %s ===\n' "$*" | tee -a "${LOG_FILE}"; }
note() { printf '%s\n' "$*"           | tee -a "${LOG_FILE}"; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not on PATH: $1"
}

free_disk_gb() {
    case "$(uname -s)" in
        Darwin) df -g "$1" | awk 'NR==2 {print $4}' ;;
        Linux)  df -BG "$1" | awk 'NR==2 {sub("G",""); print $4}' ;;
        *) echo 0 ;;
    esac
}

# ---------- preflight ----------
note "repo: ${REPO_ROOT}"
note "mode: ${MODE}"
note "log:  ${LOG_FILE}"

require_cmd uname
free_gb="$(free_disk_gb "${REPO_ROOT}")"
if [[ "${free_gb}" =~ ^[0-9]+$ ]] && (( free_gb < 10 )); then
    note "WARNING: only ${free_gb} GB free on the repo volume; recommend >= 10 GB"
fi

if [[ -z "${KEEP_LOGS}" ]]; then
    rm -rf "${REPO_ROOT}/phoenix-core/target/failsafe-reports" \
           "${REPO_ROOT}/phoenix-core/target/surefire-reports" 2>/dev/null || true
fi

mkdir -p "${HOME}/.m2"

# ---------- mode-specific run ----------
run_docker() {
    require_cmd docker
    docker info >/dev/null 2>&1 \
        || die "docker daemon not reachable. On macOS: 'colima start' or open Docker Desktop, then retry."

    log "build image ${IMAGE_TAG}"
    # BuildKit is faster but buildx may be missing on some Docker installations.
    BUILDKIT=1
    docker buildx version >/dev/null 2>&1 || BUILDKIT=0
    DOCKER_BUILDKIT="${BUILDKIT}" docker build \
        -f "${REPO_ROOT}/docker/it-runner.Dockerfile" \
        -t "${IMAGE_TAG}" \
        "${REPO_ROOT}/docker" \
        2>&1 | tee -a "${LOG_FILE}"

    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

    DOCKER_RUN=(
        docker run --rm
        --name "${CONTAINER_NAME}"
        -v "${REPO_ROOT}:/work"
        -v "${HOME}/.m2:/root/.m2"
        -e PHOENIX_IT_PATTERN="${IT_PATTERN}"
        -e PHOENIX_IT_FORKS="${FORKS}"
        -e PHOENIX_HBASE_PROFILE="${HBASE_PROFILE}"
        -e PHOENIX_RUN_ALL="${RUN_ALL}"
        -e PHOENIX_DO_CLEAN="${DO_CLEAN}"
        -e PHOENIX_INSTALL_FIRST="${INSTALL_FIRST}"
        -e PHOENIX_EXTRA_ARGS="${EXTRA_ARGS}"
        --memory=14g
        --cpus=6
    )

    if [[ -n "${SHELL_MODE}" ]]; then
        log "drop into shell in container"
        "${DOCKER_RUN[@]}" -it --entrypoint /bin/bash "${IMAGE_TAG}"
    else
        log "run integration tests in container"
        "${DOCKER_RUN[@]}" "${IMAGE_TAG}" \
            2>&1 | tee -a "${LOG_FILE}"
    fi
}

run_host() {
    if [[ "$(uname -s)" == "Darwin" && -z "${FORCE_HOST}" ]]; then
        die "host mode on Darwin is broken (HBase mini-cluster TCP_NODELAY bug). Use docker mode, or pass --force-host to override."
    fi
    require_cmd mvn
    require_cmd java

    PHOENIX_IT_PATTERN="${IT_PATTERN}" \
    PHOENIX_IT_FORKS="${FORKS}" \
    PHOENIX_HBASE_PROFILE="${HBASE_PROFILE}" \
    PHOENIX_RUN_ALL="${RUN_ALL}" \
    PHOENIX_DO_CLEAN="${DO_CLEAN}" \
    PHOENIX_INSTALL_FIRST="${INSTALL_FIRST}" \
    PHOENIX_EXTRA_ARGS="${EXTRA_ARGS}" \
        bash "${REPO_ROOT}/docker/it-runner-entrypoint.sh" \
        2>&1 | tee -a "${LOG_FILE}"
}

case "${MODE}" in
    docker) run_docker ;;
    host)   run_host ;;
    *) die "invalid mode: ${MODE}" ;;
esac

log "done. summary log: ${LOG_FILE}"
log "failsafe reports: ${REPO_ROOT}/phoenix-core/target/failsafe-reports/"
