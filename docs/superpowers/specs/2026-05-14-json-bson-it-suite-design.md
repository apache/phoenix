# JSON/BSON Index IT Suite — Design Spec

**Date:** 2026-05-14
**Branch:** `feature/json-indexes`
**Scope:** Comprehensive integration-test coverage for JSON/BSON functional secondary
indexes on Apache Phoenix, plus a per-run test-execution report artifact.

## 1. Goals

1. Exercise every supported predicate shape and JSONPath form against BSON/JSON
   functional indexes, on four representative tables (2 BSON + 2 JSON), each
   loaded with 100 deterministic rows.
2. For every query, assert whether the EXPLAIN PLAN uses the functional index or
   falls back to a full data-table scan, and pin that assertion to an explicit
   `expectIndex` / `expectFullScan` annotation per query case.
3. Produce two report artifacts per IT JVM run — `json-test-report-<run>-<timestamp>.json`
   and `.md` — that capture table names, query names, SQL, EXPLAIN output, expected
   vs. actual index usage, pass/fail, durations, error/stack info, and a summary of any
   bugs surfaced.
4. Reach **100% pass rate** end-to-end. When a query fails (correctness or planner),
   debug, fix the underlying code, re-run, and re-emit the report.

## 2. Non-goals

- **No new Phoenix runtime features.** This work is test-only and (where bugs surface)
  bug-fix-only — it does not change the public surface, the on-disk index format, or
  the planner's index-matching policy beyond fixing verified defects.
- **No new variants matrix.** Salted, multi-tenant, local-index, and transactional
  variants are out of scope; existing `Bson5IT` already covers `INCLUDE` /
  `CONSISTENCY = EVENTUAL` over UNCOVERED INDEX, which is the production shape we
  validate against.
- **No new functions or operators.** No `->` / `->>`, no GIN-style multi-valued indexes,
  no containment predicates.
- **No replacement of existing tests.** `Bson1IT`–`Bson6IT`, `BsonPathIndexWriteIT`,
  `BsonPathIndexQueryIT`, `BsonPathIndexConsistencyIT`, and `JsonFunctionsIT` continue
  to live and run unchanged.

## 3. Architecture

```
phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/
├── JsonBsonTestDataset.java          // 100-row fixed-seed dataset generator
├── IndexUsageAssertion.java          // expectIndex / expectFullScan helpers
├── JsonBsonTestReporter.java         // singleton reporter; emits JSON+MD
├── JsonBsonReportListener.java       // JUnit RunListener -> reporter
├── JsonBsonReportClassRule.java      // @ClassRule installs listener per IT
├── BsonFlatIndexIT.java              // 1st BSON table — flat path
├── BsonNestedIndexIT.java            // 2nd BSON table — nested numeric path
├── JsonFlatIndexIT.java              // 1st JSON table — flat path
└── JsonNestedIndexIT.java            // 2nd JSON table — nested path

phoenix-core/target/json-bson-reports/  (gitignored, mvn-cleaned)
├── json-test-report-1-<epoch_ms>.json
├── json-test-report-1-<epoch_ms>.md
├── json-test-report-2-<epoch_ms>.json
└── json-test-report-2-<epoch_ms>.md
```

The IT classes share a small base/helper layer; reporting is wired via a
`@ClassRule` so no surefire config or RunWith change is needed.

## 4. Components

### 4.1 `JsonBsonTestDataset`

- Seed `0xC0FFEEL`. Pure-Java generator; no JDBC, no I/O.
- Returns a list of `Row` records: `String pk`, `String docJson`, `String email`,
  `String name`, `Integer score`, `String city`, `String zip`. Same logical
  payload presented two ways for BSON tables (`BsonDocument`) and JSON tables
  (`String`), so a single ground-truth set drives all four ITs.
- 100 rows per fixture. Distribution:
  - 80 rows have all paths populated.
  - 15 rows omit the indexed path (sparse).
  - 5 rows have edge values (empty strings, integer zero, negative scores,
    decimals, large strings up to 256 chars).
- Provides query-builder helpers: `eq(value)`, `range(lo, hi)`, `in(values...)`,
  `like(pattern)`, `isNull()`, `isNotNull()`. Each helper returns a
  `QueryCase(label, sql, expectedIndexUsage, expectedRowCount)`.

### 4.2 `IndexUsageAssertion`

```java
boolean planUsesIndex(String explainPlan, String indexName);
void assertExpected(QueryCase q, String explainPlan);   // throws AssertionError on mismatch
String classifyPlan(String explainPlan);                 // -> "INDEX_RANGE", "FULL_SCAN", etc.
```

The classifier is regex-based on the standard Phoenix EXPLAIN format:
`RANGE SCAN OVER <index>`, `SERVER FILTER`, `FULL SCAN OVER <table>`. It does not
attempt to parse the entire plan tree.

### 4.3 `JsonBsonTestReporter`

Singleton, package-private. Holds:

```java
record QueryRecord(String testClass, String testMethod, String tableName,
                   String indexName, String queryLabel, String sql,
                   String explainPlan, String expectedIndexUsage,
                   String actualIndexUsage, boolean pass, long durationMs,
                   String errorMessage, String stackTrace);
```

API:

```java
static JsonBsonTestReporter get();           // lazy init, registers shutdown hook
void record(QueryRecord r);
void flush();                                 // writes JSON + MD
```

JSON schema:

```json
{
  "run":     1,
  "startedAt": "2026-05-14T19:00:00Z",
  "endedAt":   "2026-05-14T19:09:42Z",
  "branch":  "feature/json-indexes",
  "commit":  "<git rev>",
  "totals":  { "tests": 96, "passed": 96, "failed": 0, "errors": 0 },
  "tables":  [
    { "name": "T_BSON_FLAT_001", "type": "BSON",  "rowCount": 100,
      "index": { "name": "IDX_BSON_FLAT_001", "expression": "BSON_VALUE(DOC,'$.name','VARCHAR')" } },
    ...
  ],
  "queries": [
    { "testClass": "BsonFlatIndexIT", "testMethod": "equalityCanonicalPath",
      "tableName":"T_BSON_FLAT_001", "indexName":"IDX_BSON_FLAT_001",
      "queryLabel":"eq($.name)", "sql":"SELECT PK FROM ...",
      "explainPlan":"RANGE SCAN OVER ...",
      "expectedIndexUsage":"INDEX","actualIndexUsage":"INDEX",
      "pass": true, "durationMs": 23, "errorMessage": null, "stackTrace": null }
  ],
  "bugs":    [
    { "id":"B-001","queryRef":"BsonNestedIndexIT.rangeOnNestedNumeric",
      "summary":"...","status":"FIXED","commit":"<sha>" }
  ]
}
```

Markdown summary: header table per IT class, then per-query rows
(label, expected, actual, status, ms), then a "Bugs found this run" section.

Run-numbering: scans the existing files in `target/json-bson-reports/` for the
highest `<run>` and increments. Always emits a `<run>-<epoch_ms>` pair so re-runs
within the same second don't collide.

### 4.4 `JsonBsonReportListener` + `JsonBsonReportClassRule`

`JsonBsonReportListener extends RunListener` — overrides `testFinished`,
`testFailure`, `testIgnored`. Uses a `ThreadLocal<QueryRecord.Builder>` populated by
the IT method via a tiny `Reporting` helper:

```java
Reporting.with(tableName, indexName, queryLabel, sql, explainPlan, expected)
         .run(() -> assertExpected(...));
```

`JsonBsonReportClassRule` is a `TestRule` that registers the listener with the
JUnit `RunNotifier` for the class and ensures `reporter.flush()` runs in
`@AfterClass`.

### 4.5 The four IT classes

Each extends `ParallelStatsDisabledIT` and `@Category(ParallelStatsDisabledTest.class)`.
Each does:

```
@BeforeClass
  - generateUniqueName -> tableName, indexName
  - CREATE TABLE
  - INSERT 100 rows from JsonBsonTestDataset
  - CREATE INDEX
  - reporter.recordTable(...)

@Test methods (~24 each)
  - build SQL via JsonBsonTestDataset.queryBuilder
  - capture EXPLAIN
  - assertExpected(query, plan, indexName)
  - run query, compare result to ground-truth via dataset.expectedRows(query)
  - reporter.record(...)
```

Predicate matrix per IT (subset shown — full list in plans):

| Query case | Expected |
|---|---|
| `WHERE BSON_VALUE(DOC,'$.name','VARCHAR') = 'alice'` | INDEX |
| `WHERE BSON_VALUE(DOC,'name','VARCHAR') = 'alice'` (bare path) | INDEX |
| `WHERE BSON_VALUE(DOC,'$.name','VARCHAR') IN ('a','b','c')` | INDEX |
| `WHERE BSON_VALUE(DOC,'$.name','VARCHAR') BETWEEN 'a' AND 'z'` | INDEX |
| `WHERE BSON_VALUE(DOC,'$.name','VARCHAR') >= 'm'` | INDEX |
| `WHERE BSON_VALUE(DOC,'$.name','VARCHAR') IS NOT NULL` | INDEX |
| `WHERE BSON_VALUE(DOC,'$.name','VARCHAR') LIKE 'a%'` | INDEX (RANGE+filter) |
| `WHERE BSON_VALUE(DOC,'$.name','VARCHAR') != 'alice'` | INDEX (full range) |
| `WHERE UPPER(BSON_VALUE(DOC,'$.name','VARCHAR')) = 'ALICE'` | INDEX (server filter, planner rewrites inside UPPER) |
| `WHERE BSON_VALUE(DOC,'$.other','VARCHAR') = 'x'` (different path) | FULL_SCAN |
| `SELECT * (no predicate)` | FULL_SCAN |

Nested IT extends with `BSON_VALUE(DOC,'$.profile.score','BIGINT')` covering numeric
range / between / IN. JSON ITs mirror with `JSON_VALUE`.

## 5. What this design does NOT change

- No edits to existing `Bson*IT.java`, `BsonPathIndex*IT.java`, or `JsonFunctionsIT.java`.
- No edits to `phoenix-core-client/` or `phoenix-core/src/main/` source — except for
  bug fixes that are surfaced by the new ITs and confirmed against the design spec.
- No surefire / failsafe config changes. Reports land in `target/` because that is
  Maven-cleaned and is where existing surefire-reports live; consistent with the user's
  request.

## 6. Error handling and edge cases

| Situation | Behavior |
|---|---|
| Query result rows ≠ ground-truth | Test fails. Report records full row diff. |
| EXPLAIN says full scan, query was annotated `expectIndex` | Test fails. Report records the EXPLAIN text. |
| EXPLAIN says index, query was annotated `expectFullScan` | Test fails (over-eager rewrite). |
| `@BeforeClass` setup throws | All tests in the class fail; reporter records a class-level error entry. |
| Reporter shutdown hook throws | Caught and logged; never propagated. We never want a reporting bug to mask a real test failure. |
| Same `<run>` collides on filesystem | We append `-<epoch_ms>`; collision impossible without 1ms resolution loss. |

## 7. Testing strategy

- **Local execution loop:** `./run-it-tests-local.sh --it 'BsonFlatIndexIT,BsonNestedIndexIT,JsonFlatIndexIT,JsonNestedIndexIT' --no-install`.
- **Pass criteria for this work:** all 4 ITs green, all assertions including
  EXPLAIN-PLAN expectations satisfied, two artifacts (.json + .md) per run produced.
- **Bug-discovery loop:** when a query fails, dispatch an `innerloop:innerloop-fixer`
  subagent with the failing query + EXPLAIN + report path. Subagent root-causes,
  patches, commits with `--no-gpg-sign`. We re-run and regenerate report.

## 8. Phased delivery

| Batch | Deliverable | Files |
|---|---|---|
| B1 | Reporter + dataset infrastructure | `JsonBsonTestDataset`, `IndexUsageAssertion`, `JsonBsonTestReporter`, listener + class-rule |
| B2 | `BsonFlatIndexIT` end-to-end (the canary) | 1 IT class + tests |
| B3 | `BsonNestedIndexIT` (nested numeric) | 1 IT class + tests |
| B4 | `JsonFlatIndexIT` (JSON parity) | 1 IT class + tests |
| B5 | `JsonNestedIndexIT` (JSON nested) | 1 IT class + tests |
| B6 | Run all four, debug failures, re-run until 100% green | reports under `target/json-bson-reports/` |

Each batch is one git commit (or more, if a bug fix is required) with `--no-gpg-sign`.

## 9. Rollback

All work is additive under `phoenix-core/src/it/`. Worst case is `git revert` on the
batch commits — no Phoenix runtime behavior changes from this design except where a
bug fix is required, and those bug fixes ride their own commits with their own
revert path.

## 10. Key file references

- `phoenix-core/src/it/java/org/apache/phoenix/end2end/ParallelStatsDisabledIT.java` — base.
- `phoenix-core/src/it/java/org/apache/phoenix/end2end/Bson5IT.java` — predicate-shape reference.
- `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexQueryIT.java` —
  starting reference for EXPLAIN assertions.
- `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/JsonFunctionsIT.java` — JSON reference.
- `phoenix-core-client/src/main/java/org/apache/phoenix/expression/function/BsonValueFunction.java` —
  function under test.
- `phoenix-core-client/src/main/java/org/apache/phoenix/expression/function/JsonValueFunction.java` —
  function under test.
- `run-it-tests-local.sh` — local runner.
