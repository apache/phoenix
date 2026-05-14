# Local Phoenix IT Test Setup — Verified Plan

**Goal:** A single executable script (`run-it-tests-local.sh`) that sets up everything needed to run the Phoenix integration test (IT) suite on this workstation, runs the tests, and produces an aggregated report. **No existing repo code is modified — only new files are created.**

---

## 1. What Phoenix ITs need

Verified by inspecting `pom.xml`, `phoenix-core/src/test/java/org/apache/phoenix/query/BaseTest.java`, and live execution traces:

1. **JDK 11 or 17.** Both supported; Phoenix root pom toggles `--add-opens` flags by JDK version. `mvn -v` on this host already reports JDK 17.0.19.
2. **Apache Maven 3.x.** Already present (3.9.14).
3. **No external HBase / ZooKeeper / HDFS install required.** Each IT class spins up an embedded HBase mini-cluster via `HBaseTestingUtility.startMiniCluster()` (BaseTest.java:485). Mini-cluster includes embedded ZK + mini-DFS.
4. **Disk:** ~10 GB free for `target/` directories and the maven local repo (`~/.m2/repository`).
5. **RAM:** Each forked surefire JVM uses up to 2.2 GB heap (`surefire.Xmx=2200m` in pom.xml:181). Default is 7 IT forks (`numForkedIT=7`, pom.xml:174) → up to ~16 GB total. **The script makes the fork count configurable** and defaults to a lower number for laptop runs.
6. **POSIX networking.** This is the gotcha — see the next section.

## 2. Why running directly on macOS fails (verified empirically)

A live test run on this host (Darwin 25.4, JDK 17.0.19) reproduced the failure mode and produced this stack from `/phoenix-core/target/surefire-reports/...PhoenixSyncTableOutputRepositoryTest-output.txt`:

```
WARN  bootstrap.AbstractBootstrap: Failed to set channel option 'TCP_NODELAY' with value 'true'
org.apache.hbase.thirdparty.io.netty.channel.ChannelException: java.net.SocketException: Invalid argument
    at sun.nio.ch.Net.setIntOption0(Native Method)
    at sun.nio.ch.SocketAdaptor.setTcpNoDelay
    ...
WARN  ServerBootstrapAcceptor: Failed to register an accepted channel
```

The HBase Master starts, ZK comes up, the RegionServer opens its socket, but every accepted RPC channel fails when Netty calls `setTcpNoDelay(true)` on the server-side socket. The result is the `Master not initialized after 200000ms` symptom we saw earlier.

This is a known JDK-on-Darwin networking quirk — `Net.setIntOption0` rejects certain socket-options on already-accepted SOCK_STREAM sockets in newer Darwin builds. **Workaround: run the JVM on Linux.** The simplest portable Linux is a Docker container.

## 3. Strategy

The script supports two modes; default is `docker`:

| Mode | When to use | What happens |
|------|-------------|--------------|
| `docker` (default) | macOS (this host) and any host with Docker | Builds a small Linux image with JDK 17 + Maven, mounts the repo and the user's `~/.m2`, runs `mvn ... verify` inside |
| `host` | Linux workstation, or for debugging on macOS | Runs `mvn verify` directly — the script still validates JDK/Maven/disk first |

The script accepts:

* `--mode {docker|host}` — defaults to `docker` on Darwin, `host` on Linux
* `--it <pattern>` — `-Dit.test=<pattern>` glob for failsafe (e.g. `BsonPath*IT`); default runs the BSON-path ITs only because the full suite takes hours
* `--all` — alias for `--it '*IT' --groups all`; runs full IT suite
* `--forks <N>` — override `numForkedIT`; default 4 (laptop-friendly)
* `--hbase-profile <p>` — `-Dhbase.profile=2.5.4|2.5|2.6`; defaults to `2.5.4`
* `--keep-logs` — don't delete the `target/` reports between runs
* `--shell` — drop into the docker container with the repo mounted (interactive debug)
* `--clean` — `mvn clean` before testing
* `-h | --help`

## 4. Deliverables

All under the repo root (new files only):

```
run-it-tests-local.sh                   # the entry point script (executable)
docker/it-runner.Dockerfile             # JDK17 + Maven base image
docker/it-runner-entrypoint.sh          # in-container launcher (called by run-it-tests-local.sh)
docker/it-runner.dockerignore           # excludes target/, .git/ from the docker context
runtestLocalsetup.md                    # this plan
```

`run-it-tests-local.sh` and `it-runner-entrypoint.sh` source-share a small log-helper preamble; both are POSIX-portable bash.

## 5. Verified design choices

* **JDK17 image.** Verified the project builds on JDK 17 already — `mvn -pl phoenix-core-client -am -DskipTests install` succeeded on this host. Use `eclipse-temurin:17-jdk-jammy` (multi-arch, well-maintained).
* **Maven cache mount.** Mount `~/.m2` from host into `/root/.m2` in the container so the first run downloads everything once and subsequent runs are fast.
* **Repo mount.** Mount the repo r/w at `/work` inside the container; `target/` ends up under the host repo so reports are accessible without copying out. Add a `.dockerignore` for `target/` because the container has its own.
* **`--add-host` not needed.** The container talks to its own embedded mini-cluster — no host-network plumbing required. We use the default bridge network.
* **Linux loopback works.** Confirmed by reading `BaseTest.setUpConfigForMiniCluster` — mini-cluster binds to `localhost` / `127.0.0.1` on the same host as the test JVM. Linux containers have a working loopback by default.
* **`forkCount`.** Default 4 inside the container. Each fork uses up to 2.2 GB heap, so 4 × 2.2 ≈ 9 GB. Configurable via `--forks`.
* **Failsafe target.** Use `mvn -pl phoenix-core -am verify -DfailIfNoTests=false -Dit.test=<pattern>`. The `-am` ensures dependent modules build. We pass `-DskipTests=false -DskipITs=false` for clarity.
* **Test categories.** Phoenix's failsafe config has three executions (`ParallelStatsEnabledTest`, `ParallelStatsDisabledTest`, `NeedTheirOwnClusterTests`). When `-Dit.test=...` is set, all three execute on the matched classes; that's correct behavior for our smoke run.
* **Surefire JDK17 flags.** Already wired in pom.xml:206 — no manual `--add-opens` needed.

## 6. Smoke-test path (what we run after the script is built)

1. **Build everything once** (`mvn install -DskipTests`) — script does this automatically on first run inside the container; takes ~3 minutes once the maven cache is warm.
2. **Single fast IT first** — `--it PhoenixTestDriverIT` to confirm the mini-cluster boots in the Linux container. ~2-3 minutes.
3. **BSON-path ITs from this branch** — `--it 'BsonPathIndex*IT'` runs the three new IT classes (`BsonPathIndexWriteIT`, `BsonPathIndexQueryIT`, `BsonPathIndexConsistencyIT`) plus regression on `Bson*IT`.
4. **Broader sweep (optional, takes hours).** `--all` runs everything. Not the default.

## 7. Failure-mode checks the script must guard

| Check | When | Action |
|-------|------|--------|
| Docker daemon reachable | docker mode entry | Print exact `colima start` / `docker desktop` instruction and exit 2 |
| Free disk < 10 GB | always | Warn but continue |
| `~/.m2` missing | always | `mkdir -p` |
| Stale running container with the same name | docker mode entry | `docker rm -f` it |
| User passes `--mode host` on Darwin | always | Print loud warning re. `setTcpNoDelay` issue, ask for `--force-host` |

## 8. Validation pass

After the script is built we verify by:

1. `./run-it-tests-local.sh --it PhoenixTestDriverIT` — must reach `BUILD SUCCESS` with `Tests run > 0`.
2. `./run-it-tests-local.sh --it 'BsonPathIndex*IT'` — must run the 3 BSON-path ITs; failures get debugged.
3. `./run-it-tests-local.sh --it 'Bson*IT'` — regression check on the 6 existing BSON ITs.

Output of each run is preserved under `phoenix-core/target/failsafe-reports/` (already the failsafe convention) and a top-level `it-run.<timestamp>.log` summary is dropped at the repo root.

## 9. Out of scope

* Running ITs on a real distributed HBase cluster (BaseTest's "distributed mode" — IntegrationTestingUtility) — not needed for v1.
* Code coverage reports (`-Pcoverage`) — Phoenix's existing maven setup handles this via its own `mvn verify -Dskip.code-coverage=false`; not added to the script.
* Patching upstream Phoenix code to work around the macOS networking issue — explicit non-goal per the user request.
