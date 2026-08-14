# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Phoenix is a SQL layer on top of Apache HBase (which uses HDFS). This repo lives inside a parent workspace:

- **phoenix/** (this repo): Active development happens here.
- **hbase/**, **hadoop/**: Sibling repos for reference only — do not modify.

## Maven Module Structure

- **phoenix-core-client/**: Client-side SQL parsing, compilation, JDBC driver, HA connection logic
- **phoenix-core-server/**: Server-side coprocessors, indexing, replication, MapReduce
- **phoenix-core/**: Combined artifact — all unit tests and integration tests live here
- **phoenix-hbase-compat-\*/**: HBase version compatibility layers (2.5.0, 2.5.4, 2.6.0, 2.6.4)

## Build Commands

```bash
# Full build (skip tests)
mvn package -DskipTests

# Build specific module
mvn package -pl phoenix-core-server -DskipTests

# Build with a specific HBase profile (default is 2.5)
mvn package -DskipTests -Dhbase.profile=2.6
```

## Running Tests

```bash
# Single unit test
mvn test -pl phoenix-core -Dtest=MyTest

# Single integration test
mvn verify -pl phoenix-core -Dit.test=MyIT

# All unit tests in a module
mvn test -pl phoenix-core
```

Integration tests use HBase mini-clusters. They are categorized by marker interfaces:
- `ParallelStatsEnabledTest` / `ParallelStatsDisabledTest` — standard parallel execution
- `NeedsOwnMiniClusterTest` — isolated mini-cluster, runs with `reuseForks=false`

## Code Formatting

A **git pre-commit hook** runs `mvn spotless:apply` automatically on every commit. Always run it manually before creating a PR to verify:

```bash
mvn spotless:apply
```

CI will reject changes that don't pass spotless checks.

## Import Restrictions

- **No wildcard imports** (e.g. `import java.util.*`) — always use explicit imports
- **No** `com.google.common.*` (unshaded Guava) — use `org.apache.phoenix.thirdparty.com.google.common.*`
- **No** `org.apache.commons.logging` — use SLF4J
- **No** `commons-cli` or `commons-lang` (v2)

## HA and Replication Code Locations

The HA feature spans client and server:

- **Client-side HA** (HAGroup, ClusterRoleRecord, FailoverPhoenixConnection, ParallelPhoenixConnection):
  `phoenix-core-client/src/main/java/org/apache/phoenix/jdbc/`
- **Server-side replication log writer/reader** (PHOENIX-7562):
  `phoenix-core-server/src/main/java/org/apache/phoenix/replication/`
- **Server-side coprocessor endpoint** (getClusterRoleRecord RPC, prewarming):
  `phoenix-core-server/src/main/java/org/apache/phoenix/coprocessor/PhoenixRegionServerEndpoint.java`
- **Server-side write path integration**:
  `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/IndexRegionObserver.java`

## Test Infrastructure

- Unit tests: `phoenix-core/src/test/java/`
- Integration tests: `phoenix-core/src/it/java/`
- Key base classes: `BaseTest` (mini-cluster setup), `ReplicationLogBaseTest` (replication tests)
- End-to-end tests: `org.apache.phoenix.end2end.*`

## PR Review Guidelines

When running `/review` on a checked-out PR branch (typically in a worktree):

1. Run `gh pr view` and `gh pr diff` in parallel from the working directory — do not `cd` anywhere.
2. Read the full source files for changed methods to understand surrounding context.
3. Review the diff for:
   - **Correctness**: edge cases, off-by-one errors, concurrency issues, null handling
   - **Import restrictions**: enforce the rules listed above (no wildcards, no unshaded Guava, etc.)
   - **Test coverage**: are new code paths tested? are boundary conditions covered?
   - **Performance**: unnecessary allocations in hot paths, locking scope
   - **Javadoc/comments**: stale docs that contradict the new code
4. Format the review with clear sections: Overview, Correctness, Concerns, Code Quality, Test Coverage, Summary.

## CI

Jenkinsfile runs a matrix build across HBase profiles 2.5 and 2.6.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
