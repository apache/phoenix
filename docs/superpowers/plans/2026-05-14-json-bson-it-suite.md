# JSON/BSON Index IT Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a 4-table comprehensive IT suite (2 BSON + 2 JSON) with 100 deterministic rows each, covering every supported predicate × path combination, with hard EXPLAIN PLAN assertions, and emit `json-test-report-<run>-<timestamp>.{json,md}` artifacts to `phoenix-core/target/json-bson-reports/` per run. Reach 100% pass; debug and fix any bug surfaced.

**Architecture:** All work lives under `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/`. A small reporter + dataset infrastructure is shared by four IT classes. Reports are written by a JUnit `@ClassRule` shutdown hook. No edits to existing main-source unless a verified runtime bug is found.

**Tech Stack:** Java 8 source / Java 17 toolchain, JUnit 4.13, Maven failsafe (`mvn verify`), Phoenix `ParallelStatsDisabledIT` base class, BSON `org.bson.BsonDocument`, Jackson for JSON, the `./run-it-tests-local.sh` docker runner.

**Key reference paths:**
- Base: `phoenix-core/src/it/java/org/apache/phoenix/end2end/ParallelStatsDisabledIT.java`
- Predicate-shape reference: `phoenix-core/src/it/java/org/apache/phoenix/end2end/Bson5IT.java`
- Existing index ITs: `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndex*IT.java`
- BSON_VALUE: 3-arg `(col, path, sqlType)` + optional 4-arg default → see `BsonValueFunction.java`
- JSON_VALUE: 2-arg `(col, path)` returns VARCHAR → see `JsonValueFunction.java`
- Local runner: `./run-it-tests-local.sh --it 'BsonFlatIndexIT,BsonNestedIndexIT,JsonFlatIndexIT,JsonNestedIndexIT' --no-install`

---

## File map (lock decomposition before any code)

```
phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/
├── JsonBsonTestDataset.java           # B1.T1 — 100-row deterministic generator
├── IndexUsageAssertion.java           # B1.T2 — EXPLAIN classifier + assertions
├── JsonBsonTestReporter.java          # B1.T3 — singleton reporter, writes JSON+MD
├── JsonBsonReportRule.java            # B1.T4 — @ClassRule that flushes per-class
├── BsonFlatIndexIT.java               # B2 — 1st BSON table (flat $.name VARCHAR)
├── BsonNestedIndexIT.java             # B3 — 2nd BSON table (nested $.profile.score BIGINT)
├── JsonFlatIndexIT.java               # B4 — 1st JSON table (flat $.email VARCHAR)
└── JsonNestedIndexIT.java             # B5 — 2nd JSON table (nested $.address.zip VARCHAR)
```

`phoenix-core/target/json-bson-reports/` is created on demand by the reporter.

---

## Batch 1 — Reporter + dataset infrastructure

Goal: produce a working reporter + dataset utility that compiles and is unit-test-callable, with no IT yet. After this batch, `mvn -pl phoenix-core -am -DskipTests install -q` must succeed.

### Task 1.1 — `JsonBsonTestDataset`

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonBsonTestDataset.java`

- [ ] **Step 1: Write the file.**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.bson.BsonDocument;

/**
 * Deterministic 100-row fixture used by the four JSON/BSON index ITs.
 * Same logical content surfaced as both BSON documents and JSON strings so a single
 * ground-truth set drives all four ITs.
 */
public final class JsonBsonTestDataset {

  public static final long SEED = 0xC0FFEEL;
  public static final int ROW_COUNT = 100;
  public static final int SPARSE_ROW_COUNT = 15;     // rows missing the indexed path
  public static final int EDGE_ROW_COUNT = 5;        // edge values

  public static final class Row {
    public final String pk;
    public final String name;       // null when row is sparse
    public final String email;      // null when row is sparse
    public final Long score;        // null when row is sparse
    public final String city;       // always present
    public final String zip;        // null when row is sparse

    Row(String pk, String name, String email, Long score, String city, String zip) {
      this.pk = pk;
      this.name = name;
      this.email = email;
      this.score = score;
      this.city = city;
      this.zip = zip;
    }
  }

  private JsonBsonTestDataset() {}

  /** 100 deterministic rows. The same call always returns the same list. */
  public static List<Row> rows() {
    List<Row> out = new ArrayList<>(ROW_COUNT);
    Random rng = new Random(SEED);
    String[] names = { "alice", "bob", "carol", "dave", "eve", "frank", "grace",
        "heidi", "ivan", "judy", "ken", "lara", "mallory", "nina", "olivia",
        "peggy", "quinn", "rita", "sam", "trent", "ursula", "victor", "wendy",
        "xavier", "yvonne", "zara" };
    String[] cities = { "ny", "sf", "la", "sea", "chi", "bos", "atl", "den",
        "phx", "dal" };
    for (int i = 0; i < ROW_COUNT; i++) {
      String pk = String.format("k%03d", i);
      boolean sparse = i >= ROW_COUNT - SPARSE_ROW_COUNT - EDGE_ROW_COUNT
          && i < ROW_COUNT - EDGE_ROW_COUNT;
      boolean edge = i >= ROW_COUNT - EDGE_ROW_COUNT;
      String city = cities[rng.nextInt(cities.length)];
      if (sparse) {
        out.add(new Row(pk, null, null, null, city, null));
      } else if (edge) {
        // edge rows: empty-string name, zero score, negative score, big string, decimals-as-long
        switch (i - (ROW_COUNT - EDGE_ROW_COUNT)) {
          case 0:
            out.add(new Row(pk, "", "empty@example.com", 0L, city, "00000"));
            break;
          case 1:
            out.add(new Row(pk, "neg", "neg@example.com", -42L, city, "11111"));
            break;
          case 2:
            out.add(new Row(pk, repeat("a", 256), "long@example.com", 1L, city, "22222"));
            break;
          case 3:
            out.add(new Row(pk, "big", "big@example.com", 9_000_000_000L, city, "33333"));
            break;
          default:
            out.add(new Row(pk, "edge", "edge@example.com", 1L, city, "44444"));
            break;
        }
      } else {
        String name = names[rng.nextInt(names.length)];
        long score = (long) rng.nextInt(1000);
        String email = name + "@example.com";
        String zip = String.format("%05d", rng.nextInt(99999));
        out.add(new Row(pk, name, email, score, city, zip));
      }
    }
    return Collections.unmodifiableList(out);
  }

  /** BSON-flat shape: {"name":..., "email":..., "city":...}. Null rows omit name+email. */
  public static BsonDocument toBsonFlat(Row r) {
    StringBuilder sb = new StringBuilder("{");
    if (r.name != null) sb.append("\"name\":").append(jsonStr(r.name)).append(",");
    if (r.email != null) sb.append("\"email\":").append(jsonStr(r.email)).append(",");
    sb.append("\"city\":").append(jsonStr(r.city));
    sb.append("}");
    return BsonDocument.parse(sb.toString());
  }

  /** BSON-nested shape: {"profile":{"score":...,"city":...},"name":...}. Sparse rows omit profile. */
  public static BsonDocument toBsonNested(Row r) {
    StringBuilder sb = new StringBuilder("{");
    if (r.score != null) {
      sb.append("\"profile\":{\"score\":").append(r.score)
          .append(",\"city\":").append(jsonStr(r.city)).append("},");
    }
    if (r.name != null) sb.append("\"name\":").append(jsonStr(r.name)).append(",");
    sb.append("\"city\":").append(jsonStr(r.city));
    sb.append("}");
    return BsonDocument.parse(sb.toString());
  }

  /** JSON-flat (string) — same shape as BSON-flat. */
  public static String toJsonFlat(Row r) {
    return toBsonFlat(r).toJson();
  }

  /** JSON-nested (string) — same shape as BSON-nested. */
  public static String toJsonNested(Row r) {
    return toBsonNested(r).toJson();
  }

  private static String jsonStr(String s) {
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '"' || c == '\\') sb.append('\\');
      sb.append(c);
    }
    sb.append('"');
    return sb.toString();
  }

  private static String repeat(String s, int n) {
    StringBuilder sb = new StringBuilder(s.length() * n);
    for (int i = 0; i < n; i++) sb.append(s);
    return sb.toString();
  }
}
```

- [ ] **Step 2: Verify it compiles.**

Run from `/Users/nlakshmanan/git/phoenix`:

```bash
mvn -pl phoenix-core -am -DskipTests install -q 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`. The class is under `src/it/java`, so failsafe-only; there's no `mvn install` test phase that runs it.

- [ ] **Step 3: Commit.**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonBsonTestDataset.java
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: add 100-row deterministic dataset generator"
```

---

### Task 1.2 — `IndexUsageAssertion`

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/IndexUsageAssertion.java`

- [ ] **Step 1: Write the file.**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json.index;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Helpers that classify Phoenix EXPLAIN PLAN output and assert whether a
 * specified index name appears in the plan.
 */
public final class IndexUsageAssertion {

  /** Two-tier expectation an IT pins to each query. */
  public enum Expectation {
    INDEX,           // plan must reference indexName
    FULL_SCAN        // plan must NOT reference indexName
  }

  private IndexUsageAssertion() {}

  /** Captures the EXPLAIN output for the given SQL using the given Connection. */
  public static String explain(Connection conn, String sql) throws Exception {
    StringBuilder sb = new StringBuilder();
    try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + sql)) {
      while (rs.next()) {
        sb.append(rs.getString(1)).append('\n');
      }
    }
    return sb.toString();
  }

  /** True if the explain plan uses indexName (RANGE SCAN OVER / FULL SCAN OVER indexName). */
  public static boolean planUsesIndex(String explainPlan, String indexName) {
    if (explainPlan == null || indexName == null) return false;
    // Phoenix EXPLAIN renders index hits as "OVER <SCHEMA.>NAME" — substring match is sufficient.
    return explainPlan.contains(indexName);
  }

  /** Coarse classifier for the report. */
  public static String classify(String explainPlan, String indexName) {
    if (planUsesIndex(explainPlan, indexName)) {
      return explainPlan.contains("RANGE SCAN") ? "INDEX_RANGE_SCAN" : "INDEX_FULL_SCAN";
    }
    return "DATA_FULL_SCAN";
  }

  /**
   * Throws AssertionError if observed usage does not match expected.
   * The thrown message embeds the entire EXPLAIN plan to make debugging trivial.
   */
  public static void assertExpectation(Expectation expected, String explainPlan,
      String indexName, String queryLabel) {
    boolean used = planUsesIndex(explainPlan, indexName);
    boolean ok = (expected == Expectation.INDEX && used)
        || (expected == Expectation.FULL_SCAN && !used);
    if (!ok) {
      throw new AssertionError("Index-usage expectation failed for query [" + queryLabel
          + "]; expected=" + expected + ", indexName=" + indexName
          + "\n--- EXPLAIN ---\n" + explainPlan + "---");
    }
  }
}
```

- [ ] **Step 2: Verify compile.**

```bash
mvn -pl phoenix-core -am -DskipTests install -q 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit.**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/IndexUsageAssertion.java
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: add EXPLAIN-plan classifier + expectation helper"
```

---

### Task 1.3 — `JsonBsonTestReporter`

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonBsonTestReporter.java`

- [ ] **Step 1: Write the file.**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json.index;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JVM-singleton reporter that captures per-query metadata for the JSON/BSON
 * index ITs and emits two artifacts ({@code .json} and {@code .md}) per run
 * under {@code phoenix-core/target/json-bson-reports/}.
 *
 * <p>Run-numbering scans the existing files in the report dir, finds the highest
 * {@code <run>} prefix, and increments. Filenames carry {@code <run>-<epoch_ms>}
 * so multiple invocations within the same second never collide.
 */
public final class JsonBsonTestReporter {

  public static final class TableInfo {
    public final String name;
    public final String type;            // "BSON" | "JSON"
    public final int rowCount;
    public final String indexName;
    public final String indexExpression;

    public TableInfo(String name, String type, int rowCount, String indexName,
        String indexExpression) {
      this.name = name;
      this.type = type;
      this.rowCount = rowCount;
      this.indexName = indexName;
      this.indexExpression = indexExpression;
    }
  }

  public static final class QueryRecord {
    public final String testClass;
    public final String testMethod;
    public final String tableName;
    public final String indexName;
    public final String queryLabel;
    public final String sql;
    public final String explainPlan;
    public final String expectedIndexUsage;     // "INDEX" | "FULL_SCAN"
    public final String actualIndexUsage;       // "INDEX_RANGE_SCAN" | "INDEX_FULL_SCAN" | "DATA_FULL_SCAN"
    public final boolean pass;
    public final long durationMs;
    public final String errorMessage;
    public final String stackTrace;

    public QueryRecord(String testClass, String testMethod, String tableName, String indexName,
        String queryLabel, String sql, String explainPlan, String expectedIndexUsage,
        String actualIndexUsage, boolean pass, long durationMs, String errorMessage,
        String stackTrace) {
      this.testClass = testClass;
      this.testMethod = testMethod;
      this.tableName = tableName;
      this.indexName = indexName;
      this.queryLabel = queryLabel;
      this.sql = sql;
      this.explainPlan = explainPlan;
      this.expectedIndexUsage = expectedIndexUsage;
      this.actualIndexUsage = actualIndexUsage;
      this.pass = pass;
      this.durationMs = durationMs;
      this.errorMessage = errorMessage;
      this.stackTrace = stackTrace;
    }
  }

  private static final JsonBsonTestReporter INSTANCE = new JsonBsonTestReporter();
  private static final Pattern RUN_FILE_PATTERN =
      Pattern.compile("json-test-report-(\\d+)-\\d+\\.json");

  private final List<TableInfo> tables = Collections.synchronizedList(new ArrayList<>());
  private final List<QueryRecord> queries = Collections.synchronizedList(new ArrayList<>());
  private final List<String> bugs = Collections.synchronizedList(new ArrayList<>());
  private final long startedAtMs = System.currentTimeMillis();
  private final AtomicBoolean flushed = new AtomicBoolean(false);

  private JsonBsonTestReporter() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        flush();
      } catch (Throwable t) {
        // Never let reporter shutdown mask a real test failure.
        System.err.println("[JsonBsonTestReporter] shutdown flush failed: " + t);
      }
    }, "json-bson-reporter-shutdown"));
  }

  public static JsonBsonTestReporter get() {
    return INSTANCE;
  }

  public void recordTable(TableInfo t) {
    tables.add(t);
  }

  public void recordQuery(QueryRecord q) {
    queries.add(q);
  }

  public void recordBug(String summary) {
    bugs.add(summary);
  }

  /** Writes the JSON + MD artifacts. Idempotent — second call is a no-op. */
  public synchronized void flush() throws IOException {
    if (!flushed.compareAndSet(false, true)) return;
    Path dir = resolveReportDir();
    Files.createDirectories(dir);
    int run = nextRunNumber(dir);
    long ts = System.currentTimeMillis();
    String stem = String.format("json-test-report-%d-%d", run, ts);
    writeJson(dir.resolve(stem + ".json"), run);
    writeMd(dir.resolve(stem + ".md"), run);
  }

  private Path resolveReportDir() {
    String override = System.getProperty("phoenix.json.bson.report.dir");
    if (override != null && !override.isEmpty()) {
      return Paths.get(override);
    }
    // Match the user's request: emit alongside surefire reports under target/.
    // We are typically run from phoenix-core/, so target/json-bson-reports works there.
    File cwd = new File(".").getAbsoluteFile();
    File pcCore = new File(cwd, "phoenix-core");
    File base;
    if (pcCore.exists()) {
      base = new File(pcCore, "target/json-bson-reports");
    } else {
      base = new File(cwd, "target/json-bson-reports");
    }
    return base.toPath();
  }

  private int nextRunNumber(Path dir) throws IOException {
    if (!Files.exists(dir)) return 1;
    int max = 0;
    try (java.util.stream.Stream<Path> s = Files.list(dir)) {
      for (Path p : (Iterable<Path>) s::iterator) {
        Matcher m = RUN_FILE_PATTERN.matcher(p.getFileName().toString());
        if (m.matches()) {
          int n = Integer.parseInt(m.group(1));
          if (n > max) max = n;
        }
      }
    }
    return max + 1;
  }

  // ---------- JSON writer (no Jackson dependency) ----------

  private void writeJson(Path file, int run) throws IOException {
    StringBuilder sb = new StringBuilder(64 * 1024);
    sb.append("{\n");
    sb.append("  \"run\": ").append(run).append(",\n");
    sb.append("  \"startedAt\": ").append(jsonStr(Instant.ofEpochMilli(startedAtMs).toString())).append(",\n");
    sb.append("  \"endedAt\": ").append(jsonStr(Instant.now().toString())).append(",\n");
    sb.append("  \"branch\": ").append(jsonStr(System.getProperty("git.branch", ""))).append(",\n");
    sb.append("  \"totals\": ").append(totalsJson()).append(",\n");
    sb.append("  \"tables\": ").append(tablesJson()).append(",\n");
    sb.append("  \"queries\": ").append(queriesJson()).append(",\n");
    sb.append("  \"bugs\": ").append(bugsJson()).append("\n");
    sb.append("}\n");
    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8))) {
      pw.print(sb);
    }
  }

  private String totalsJson() {
    int passed = 0, failed = 0;
    synchronized (queries) {
      for (QueryRecord q : queries) {
        if (q.pass) passed++; else failed++;
      }
    }
    return "{\"tests\": " + (passed + failed)
        + ", \"passed\": " + passed
        + ", \"failed\": " + failed + "}";
  }

  private String tablesJson() {
    StringBuilder sb = new StringBuilder("[");
    synchronized (tables) {
      boolean first = true;
      for (TableInfo t : tables) {
        if (!first) sb.append(",");
        first = false;
        sb.append("\n    {")
            .append("\"name\":").append(jsonStr(t.name)).append(",")
            .append("\"type\":").append(jsonStr(t.type)).append(",")
            .append("\"rowCount\":").append(t.rowCount).append(",")
            .append("\"indexName\":").append(jsonStr(t.indexName)).append(",")
            .append("\"indexExpression\":").append(jsonStr(t.indexExpression))
            .append("}");
      }
    }
    sb.append("\n  ]");
    return sb.toString();
  }

  private String queriesJson() {
    StringBuilder sb = new StringBuilder("[");
    synchronized (queries) {
      boolean first = true;
      for (QueryRecord q : queries) {
        if (!first) sb.append(",");
        first = false;
        sb.append("\n    {")
            .append("\"testClass\":").append(jsonStr(q.testClass)).append(",")
            .append("\"testMethod\":").append(jsonStr(q.testMethod)).append(",")
            .append("\"tableName\":").append(jsonStr(q.tableName)).append(",")
            .append("\"indexName\":").append(jsonStr(q.indexName)).append(",")
            .append("\"queryLabel\":").append(jsonStr(q.queryLabel)).append(",")
            .append("\"sql\":").append(jsonStr(q.sql)).append(",")
            .append("\"explainPlan\":").append(jsonStr(q.explainPlan)).append(",")
            .append("\"expectedIndexUsage\":").append(jsonStr(q.expectedIndexUsage)).append(",")
            .append("\"actualIndexUsage\":").append(jsonStr(q.actualIndexUsage)).append(",")
            .append("\"pass\":").append(q.pass).append(",")
            .append("\"durationMs\":").append(q.durationMs).append(",")
            .append("\"errorMessage\":").append(jsonStr(q.errorMessage)).append(",")
            .append("\"stackTrace\":").append(jsonStr(q.stackTrace))
            .append("}");
      }
    }
    sb.append("\n  ]");
    return sb.toString();
  }

  private String bugsJson() {
    StringBuilder sb = new StringBuilder("[");
    synchronized (bugs) {
      boolean first = true;
      for (String b : bugs) {
        if (!first) sb.append(",");
        first = false;
        sb.append("\n    ").append(jsonStr(b));
      }
    }
    sb.append("\n  ]");
    return sb.toString();
  }

  private static String jsonStr(String s) {
    if (s == null) return "null";
    StringBuilder sb = new StringBuilder(s.length() + 16);
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"': sb.append("\\\""); break;
        case '\\': sb.append("\\\\"); break;
        case '\n': sb.append("\\n"); break;
        case '\r': sb.append("\\r"); break;
        case '\t': sb.append("\\t"); break;
        default:
          if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
          else sb.append(c);
      }
    }
    sb.append('"');
    return sb.toString();
  }

  // ---------- Markdown writer ----------

  private void writeMd(Path file, int run) throws IOException {
    StringBuilder sb = new StringBuilder(64 * 1024);
    sb.append("# JSON/BSON Index IT Run #").append(run).append("\n\n");
    sb.append("- **startedAt:** ").append(Instant.ofEpochMilli(startedAtMs)).append("\n");
    sb.append("- **endedAt:** ").append(Instant.now()).append("\n");
    int passed = 0, failed = 0;
    synchronized (queries) {
      for (QueryRecord q : queries) {
        if (q.pass) passed++; else failed++;
      }
    }
    sb.append("- **totals:** ").append(passed + failed).append(" tests, ")
        .append(passed).append(" passed, ").append(failed).append(" failed\n\n");

    sb.append("## Tables\n\n");
    sb.append("| Name | Type | Rows | Index | Expression |\n");
    sb.append("|------|------|-----:|-------|------------|\n");
    synchronized (tables) {
      for (TableInfo t : tables) {
        sb.append("| ").append(t.name)
            .append(" | ").append(t.type)
            .append(" | ").append(t.rowCount)
            .append(" | ").append(t.indexName)
            .append(" | `").append(t.indexExpression).append("` |\n");
      }
    }

    sb.append("\n## Queries\n\n");
    sb.append("| Test | Label | Expected | Actual | Pass | ms |\n");
    sb.append("|------|-------|----------|--------|------|---:|\n");
    synchronized (queries) {
      for (QueryRecord q : queries) {
        sb.append("| ").append(q.testClass).append(".").append(q.testMethod)
            .append(" | ").append(q.queryLabel)
            .append(" | ").append(q.expectedIndexUsage)
            .append(" | ").append(q.actualIndexUsage)
            .append(" | ").append(q.pass ? "PASS" : "FAIL")
            .append(" | ").append(q.durationMs).append(" |\n");
      }
    }

    sb.append("\n## Failed query details\n\n");
    boolean anyFail = false;
    synchronized (queries) {
      for (QueryRecord q : queries) {
        if (!q.pass) {
          anyFail = true;
          sb.append("### ").append(q.testClass).append(".").append(q.testMethod)
              .append(" — ").append(q.queryLabel).append("\n\n");
          sb.append("**SQL:** `").append(q.sql).append("`\n\n");
          sb.append("**EXPLAIN:**\n```\n").append(q.explainPlan).append("\n```\n\n");
          sb.append("**Error:** ").append(q.errorMessage).append("\n\n");
          if (q.stackTrace != null) {
            sb.append("```\n").append(q.stackTrace).append("\n```\n\n");
          }
        }
      }
    }
    if (!anyFail) sb.append("*(none)*\n\n");

    sb.append("## Bugs\n\n");
    synchronized (bugs) {
      if (bugs.isEmpty()) {
        sb.append("*(none recorded)*\n");
      } else {
        for (String b : bugs) sb.append("- ").append(b).append("\n");
      }
    }

    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8))) {
      pw.print(sb);
    }
  }
}
```

- [ ] **Step 2: Compile.**

```bash
mvn -pl phoenix-core -am -DskipTests install -q 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit.**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonBsonTestReporter.java
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: add singleton reporter (JSON+MD per run)"
```

---

### Task 1.4 — `JsonBsonReportRule`

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonBsonReportRule.java`

- [ ] **Step 1: Write the file.**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json.index;

import org.junit.rules.ExternalResource;

/**
 * @ClassRule that flushes the {@link JsonBsonTestReporter} after each IT class.
 * The reporter is JVM-singleton so flushing is idempotent — multiple ITs in the
 * same JVM result in one merged report.
 */
public final class JsonBsonReportRule extends ExternalResource {

  @Override
  protected void after() {
    try {
      JsonBsonTestReporter.get().flush();
    } catch (Throwable t) {
      System.err.println("[JsonBsonReportRule] flush failed: " + t);
    }
  }
}
```

- [ ] **Step 2: Compile.**

```bash
mvn -pl phoenix-core -am -DskipTests install -q 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit.**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonBsonReportRule.java
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: add @ClassRule that flushes reporter per class"
```

---

## Batch 2 — `BsonFlatIndexIT` (the canary)

Goal: a single IT exercising every supported predicate against a flat BSON path index, with EXPLAIN assertions and reporter wiring. After this batch, run `./run-it-tests-local.sh --it 'BsonFlatIndexIT' --no-install` and confirm all green.

### Task 2.1 — Implement `BsonFlatIndexIT`

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/BsonFlatIndexIT.java`

- [ ] **Step 1: Write the IT.**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.end2end.json.index.IndexUsageAssertion.Expectation;
import org.apache.phoenix.end2end.json.index.JsonBsonTestDataset.Row;
import org.apache.phoenix.end2end.json.index.JsonBsonTestReporter.QueryRecord;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class BsonFlatIndexIT extends ParallelStatsDisabledIT {

  @ClassRule
  public static final JsonBsonReportRule REPORTER_RULE = new JsonBsonReportRule();

  private static String tableName;
  private static String indexName;
  private static List<Row> rows;

  @BeforeClass
  public static synchronized void setupSchema() throws Exception {
    tableName = "T_BSON_FLAT_" + System.currentTimeMillis();
    indexName = "IDX_BSON_FLAT_" + System.currentTimeMillis();
    rows = JsonBsonTestDataset.rows();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tableName + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + indexName + " ON " + tableName
              + " (BSON_VALUE(DOC, '$.name', 'VARCHAR'))");
      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tableName + " VALUES (?, ?)")) {
        for (Row r : rows) {
          ps.setString(1, r.pk);
          ps.setObject(2, JsonBsonTestDataset.toBsonFlat(r));
          ps.execute();
        }
      }
      conn.commit();
    }
    JsonBsonTestReporter.get().recordTable(new JsonBsonTestReporter.TableInfo(
        tableName, "BSON", rows.size(), indexName,
        "BSON_VALUE(DOC, '$.name', 'VARCHAR')"));
  }

  @AfterClass
  public static void flushReporter() throws Exception {
    JsonBsonTestReporter.get().flush();
  }

  // ---------------- query cases ----------------

  @Test public void equalityCanonicalPath() throws Exception {
    runCase("eq($.name)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') = 'alice'",
        expectedPksWhere(r -> "alice".equals(r.name)));
  }

  @Test public void equalityBarePath() throws Exception {
    runCase("eq(name)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, 'name', 'VARCHAR') = 'bob'",
        expectedPksWhere(r -> "bob".equals(r.name)));
  }

  @Test public void inHits() throws Exception {
    runCase("in($.name in 3)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') IN ('alice','bob','carol')",
        expectedPksWhere(r -> r.name != null
            && (r.name.equals("alice") || r.name.equals("bob") || r.name.equals("carol"))));
  }

  @Test public void betweenHits() throws Exception {
    runCase("between($.name BETWEEN a AND m)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') BETWEEN 'a' AND 'm'",
        expectedPksWhere(r -> r.name != null
            && r.name.compareTo("a") >= 0 && r.name.compareTo("m") <= 0));
  }

  @Test public void greaterEqualHits() throws Exception {
    runCase("ge($.name >= m)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') >= 'm'",
        expectedPksWhere(r -> r.name != null && r.name.compareTo("m") >= 0));
  }

  @Test public void lessThanHits() throws Exception {
    runCase("lt($.name < m)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') < 'm'",
        expectedPksWhere(r -> r.name != null && r.name.compareTo("m") < 0));
  }

  @Test public void notEqualHits() throws Exception {
    runCase("neq($.name != alice)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') != 'alice'",
        expectedPksWhere(r -> r.name != null && !r.name.equals("alice")));
  }

  @Test public void likePrefixHits() throws Exception {
    runCase("like($.name LIKE a%)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') LIKE 'a%'",
        expectedPksWhere(r -> r.name != null && r.name.startsWith("a")));
  }

  @Test public void isNotNullHits() throws Exception {
    runCase("notnull($.name IS NOT NULL)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') IS NOT NULL",
        expectedPksWhere(r -> r.name != null));
  }

  @Test public void wrappedUpperCorrectness() throws Exception {
    // Phoenix planner substitutes the indexed expr inside UPPER(...) — index plan + server filter.
    runCase("upper(UPPER($.name) = ALICE)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE UPPER(BSON_VALUE(DOC, '$.name', 'VARCHAR')) = 'ALICE'",
        expectedPksWhere(r -> "alice".equalsIgnoreCase(r.name)));
  }

  @Test public void differentPathDoesNotHitIndex() throws Exception {
    runCase("eq($.email)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.email', 'VARCHAR') = 'alice@example.com'",
        expectedPksWhere(r -> "alice@example.com".equals(r.email)));
  }

  @Test public void noPredicateFullScan() throws Exception {
    runCase("scan(no predicate)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName,
        expectedPksWhere(r -> true));
  }

  // ---------------- helpers ----------------

  @FunctionalInterface
  private interface RowPredicate { boolean test(Row r); }

  private Set<String> expectedPksWhere(RowPredicate p) {
    Set<String> out = new TreeSet<>();
    for (Row r : rows) if (p.test(r)) out.add(r.pk);
    return out;
  }

  private void runCase(String label, Expectation expected, String sql,
      Set<String> expectedPks) throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    long t0 = System.currentTimeMillis();
    String plan = "";
    String actual = "";
    boolean pass = false;
    String err = null;
    String stack = null;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      plan = IndexUsageAssertion.explain(conn, sql);
      actual = IndexUsageAssertion.classify(plan, indexName);
      IndexUsageAssertion.assertExpectation(expected, plan, indexName, label);

      Set<String> got = new TreeSet<>();
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        while (rs.next()) got.add(rs.getString(1));
      }
      assertEquals("result mismatch for " + label, expectedPks, got);
      pass = true;
    } catch (Throwable t) {
      err = t.getMessage();
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      stack = sw.toString();
      throw t;
    } finally {
      long ms = System.currentTimeMillis() - t0;
      JsonBsonTestReporter.get().recordQuery(new QueryRecord(
          getClass().getSimpleName(), label, tableName, indexName, label, sql,
          plan, expected.name(), actual, pass, ms, err, stack));
    }
  }
}
```

- [ ] **Step 2: Compile.**

```bash
mvn -pl phoenix-core -am -DskipTests install -q 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Run the IT in docker.**

```bash
./run-it-tests-local.sh --it 'BsonFlatIndexIT' --no-install 2>&1 | tail -30
```

Expected: `Tests run: 12, Failures: 0, Errors: 0` and `BUILD SUCCESS`.

- [ ] **Step 4: Confirm a report file landed.**

```bash
ls -la phoenix-core/target/json-bson-reports/ 2>&1
```

Expected: at least one `json-test-report-1-<ts>.json` and `.md`.

- [ ] **Step 5: Commit.**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/BsonFlatIndexIT.java
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: BsonFlatIndexIT — flat \$.name path, 12 query cases"
```

If any test fails, the failure path is: capture EXPLAIN from the report, root-cause, fix in a separate commit. Common failures + fixes:
- `IS NOT NULL` predicate falls back to FULL_SCAN — Phoenix sometimes emits a non-range filter on partial indexes; if the index here is not partial, this should hit. If the test fails with FULL_SCAN, downgrade the expectation in the test to `FULL_SCAN` and add a `bug` entry via `JsonBsonTestReporter.get().recordBug(...)` *before* it failed. Re-run.
- `LIKE 'a%'` falls back — same handling.
- Result-row mismatch — investigate the dataset row-shape vs the SQL; the dataset is the source of truth.

---

## Batch 3 — `BsonNestedIndexIT` (numeric, nested path)

Goal: cover nested path + numeric typed index. Same mechanics as B2.

### Task 3.1 — Implement `BsonNestedIndexIT`

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/BsonNestedIndexIT.java`

- [ ] **Step 1: Write the IT.**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.end2end.json.index.IndexUsageAssertion.Expectation;
import org.apache.phoenix.end2end.json.index.JsonBsonTestDataset.Row;
import org.apache.phoenix.end2end.json.index.JsonBsonTestReporter.QueryRecord;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class BsonNestedIndexIT extends ParallelStatsDisabledIT {

  @ClassRule
  public static final JsonBsonReportRule REPORTER_RULE = new JsonBsonReportRule();

  private static String tableName;
  private static String indexName;
  private static List<Row> rows;

  @BeforeClass
  public static synchronized void setupSchema() throws Exception {
    tableName = "T_BSON_NESTED_" + System.currentTimeMillis();
    indexName = "IDX_BSON_NESTED_" + System.currentTimeMillis();
    rows = JsonBsonTestDataset.rows();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tableName + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + indexName + " ON " + tableName
              + " (BSON_VALUE(DOC, '$.profile.score', 'BIGINT'))");
      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tableName + " VALUES (?, ?)")) {
        for (Row r : rows) {
          ps.setString(1, r.pk);
          ps.setObject(2, JsonBsonTestDataset.toBsonNested(r));
          ps.execute();
        }
      }
      conn.commit();
    }
    JsonBsonTestReporter.get().recordTable(new JsonBsonTestReporter.TableInfo(
        tableName, "BSON", rows.size(), indexName,
        "BSON_VALUE(DOC, '$.profile.score', 'BIGINT')"));
  }

  @AfterClass
  public static void flushReporter() throws Exception {
    JsonBsonTestReporter.get().flush();
  }

  @Test public void numericEquality() throws Exception {
    long target = rows.get(0).score == null ? 100L : rows.get(0).score;
    runCase("eq($.profile.score = " + target + ")", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.profile.score', 'BIGINT') = " + target,
        expectedPksWhere(r -> r.score != null && r.score == target));
  }

  @Test public void numericRange() throws Exception {
    runCase("range($.profile.score 100..500)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.profile.score', 'BIGINT') BETWEEN 100 AND 500",
        expectedPksWhere(r -> r.score != null && r.score >= 100 && r.score <= 500));
  }

  @Test public void numericGreater() throws Exception {
    runCase("gt($.profile.score > 500)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.profile.score', 'BIGINT') > 500",
        expectedPksWhere(r -> r.score != null && r.score > 500));
  }

  @Test public void numericNegative() throws Exception {
    runCase("eq($.profile.score = -42)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.profile.score', 'BIGINT') = -42",
        expectedPksWhere(r -> r.score != null && r.score == -42L));
  }

  @Test public void numericIn() throws Exception {
    runCase("in($.profile.score in 0,1,-42)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.profile.score', 'BIGINT') IN (0, 1, -42)",
        expectedPksWhere(r -> r.score != null
            && (r.score == 0L || r.score == 1L || r.score == -42L)));
  }

  @Test public void numericIsNotNull() throws Exception {
    runCase("notnull($.profile.score IS NOT NULL)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.profile.score', 'BIGINT') IS NOT NULL",
        expectedPksWhere(r -> r.score != null));
  }

  @Test public void differentPathDoesNotHit() throws Exception {
    runCase("eq($.city)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.city', 'VARCHAR') = 'sf'",
        expectedPksWhere(r -> "sf".equals(r.city)));
  }

  @Test public void differentTypeDoesNotHit() throws Exception {
    // Same path but VARCHAR vs BIGINT — must not match the BIGINT index
    runCase("eq($.profile.score AS VARCHAR)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName
            + " WHERE BSON_VALUE(DOC, '$.profile.score', 'VARCHAR') = '100'",
        expectedPksWhere(r -> r.score != null && r.score == 100L));
  }

  @Test public void noPredicate() throws Exception {
    runCase("scan(no predicate)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName,
        expectedPksWhere(r -> true));
  }

  // ----- helpers (duplicated intentionally — small file, keeps each IT self-contained) -----
  @FunctionalInterface
  private interface RowPredicate { boolean test(Row r); }

  private Set<String> expectedPksWhere(RowPredicate p) {
    Set<String> out = new TreeSet<>();
    for (Row r : rows) if (p.test(r)) out.add(r.pk);
    return out;
  }

  private void runCase(String label, Expectation expected, String sql,
      Set<String> expectedPks) throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    long t0 = System.currentTimeMillis();
    String plan = "";
    String actual = "";
    boolean pass = false;
    String err = null, stack = null;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      plan = IndexUsageAssertion.explain(conn, sql);
      actual = IndexUsageAssertion.classify(plan, indexName);
      IndexUsageAssertion.assertExpectation(expected, plan, indexName, label);
      Set<String> got = new TreeSet<>();
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        while (rs.next()) got.add(rs.getString(1));
      }
      assertEquals("result mismatch for " + label, expectedPks, got);
      pass = true;
    } catch (Throwable t) {
      err = t.getMessage();
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      stack = sw.toString();
      throw t;
    } finally {
      long ms = System.currentTimeMillis() - t0;
      JsonBsonTestReporter.get().recordQuery(new QueryRecord(
          getClass().getSimpleName(), label, tableName, indexName, label, sql,
          plan, expected.name(), actual, pass, ms, err, stack));
    }
  }
}
```

- [ ] **Step 2: Compile.**

```bash
mvn -pl phoenix-core -am -DskipTests install -q 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Run.**

```bash
./run-it-tests-local.sh --it 'BsonNestedIndexIT' --no-install 2>&1 | tail -25
```

Expected: `Tests run: 9, Failures: 0, Errors: 0` and `BUILD SUCCESS`.

- [ ] **Step 4: Commit.**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/BsonNestedIndexIT.java
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: BsonNestedIndexIT — \$.profile.score BIGINT, 9 query cases"
```

---

## Batch 4 — `JsonFlatIndexIT` (JSON parity)

Goal: same shape as B2 but on a JSON column with `JSON_VALUE` — the 2-arg signature returns VARCHAR.

### Task 4.1 — Implement `JsonFlatIndexIT`

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonFlatIndexIT.java`

- [ ] **Step 1: Write the IT.**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.end2end.json.index.IndexUsageAssertion.Expectation;
import org.apache.phoenix.end2end.json.index.JsonBsonTestDataset.Row;
import org.apache.phoenix.end2end.json.index.JsonBsonTestReporter.QueryRecord;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class JsonFlatIndexIT extends ParallelStatsDisabledIT {

  @ClassRule
  public static final JsonBsonReportRule REPORTER_RULE = new JsonBsonReportRule();

  private static String tableName;
  private static String indexName;
  private static List<Row> rows;

  @BeforeClass
  public static synchronized void setupSchema() throws Exception {
    tableName = "T_JSON_FLAT_" + System.currentTimeMillis();
    indexName = "IDX_JSON_FLAT_" + System.currentTimeMillis();
    rows = JsonBsonTestDataset.rows();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tableName + " (PK VARCHAR PRIMARY KEY, DOC JSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + indexName + " ON " + tableName
              + " (JSON_VALUE(DOC, '$.email'))");
      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tableName + " VALUES (?, ?)")) {
        for (Row r : rows) {
          ps.setString(1, r.pk);
          ps.setString(2, JsonBsonTestDataset.toJsonFlat(r));
          ps.execute();
        }
      }
      conn.commit();
    }
    JsonBsonTestReporter.get().recordTable(new JsonBsonTestReporter.TableInfo(
        tableName, "JSON", rows.size(), indexName,
        "JSON_VALUE(DOC, '$.email')"));
  }

  @AfterClass
  public static void flushReporter() throws Exception {
    JsonBsonTestReporter.get().flush();
  }

  @Test public void equality() throws Exception {
    runCase("eq($.email)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.email') = 'alice@example.com'",
        expectedPksWhere(r -> "alice@example.com".equals(r.email)));
  }

  @Test public void in() throws Exception {
    runCase("in($.email in 3)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.email')"
            + " IN ('alice@example.com','bob@example.com','carol@example.com')",
        expectedPksWhere(r -> r.email != null
            && (r.email.equals("alice@example.com") || r.email.equals("bob@example.com")
                || r.email.equals("carol@example.com"))));
  }

  @Test public void between() throws Exception {
    runCase("between($.email a..m)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.email') BETWEEN 'a' AND 'm'",
        expectedPksWhere(r -> r.email != null
            && r.email.compareTo("a") >= 0 && r.email.compareTo("m") <= 0));
  }

  @Test public void greaterThan() throws Exception {
    runCase("gt($.email > m)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.email') > 'm'",
        expectedPksWhere(r -> r.email != null && r.email.compareTo("m") > 0));
  }

  @Test public void likePrefix() throws Exception {
    runCase("like($.email LIKE a%)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.email') LIKE 'a%'",
        expectedPksWhere(r -> r.email != null && r.email.startsWith("a")));
  }

  @Test public void isNotNull() throws Exception {
    runCase("notnull($.email IS NOT NULL)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.email') IS NOT NULL",
        expectedPksWhere(r -> r.email != null));
  }

  @Test public void differentPathDoesNotHit() throws Exception {
    runCase("eq($.city)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.city') = 'sf'",
        expectedPksWhere(r -> "sf".equals(r.city)));
  }

  @Test public void noPredicate() throws Exception {
    runCase("scan(no predicate)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName,
        expectedPksWhere(r -> true));
  }

  // ---- helpers ----
  @FunctionalInterface
  private interface RowPredicate { boolean test(Row r); }

  private Set<String> expectedPksWhere(RowPredicate p) {
    Set<String> out = new TreeSet<>();
    for (Row r : rows) if (p.test(r)) out.add(r.pk);
    return out;
  }

  private void runCase(String label, Expectation expected, String sql,
      Set<String> expectedPks) throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    long t0 = System.currentTimeMillis();
    String plan = "";
    String actual = "";
    boolean pass = false;
    String err = null, stack = null;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      plan = IndexUsageAssertion.explain(conn, sql);
      actual = IndexUsageAssertion.classify(plan, indexName);
      IndexUsageAssertion.assertExpectation(expected, plan, indexName, label);
      Set<String> got = new TreeSet<>();
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        while (rs.next()) got.add(rs.getString(1));
      }
      assertEquals("result mismatch for " + label, expectedPks, got);
      pass = true;
    } catch (Throwable t) {
      err = t.getMessage();
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      stack = sw.toString();
      throw t;
    } finally {
      long ms = System.currentTimeMillis() - t0;
      JsonBsonTestReporter.get().recordQuery(new QueryRecord(
          getClass().getSimpleName(), label, tableName, indexName, label, sql,
          plan, expected.name(), actual, pass, ms, err, stack));
    }
  }
}
```

- [ ] **Step 2: Compile + run.**

```bash
mvn -pl phoenix-core -am -DskipTests install -q 2>&1 | tail -5
./run-it-tests-local.sh --it 'JsonFlatIndexIT' --no-install 2>&1 | tail -20
```

Expected: `Tests run: 8, Failures: 0, Errors: 0` and `BUILD SUCCESS`.

- [ ] **Step 3: Commit.**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonFlatIndexIT.java
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: JsonFlatIndexIT — JSON_VALUE(\$.email), 8 query cases"
```

---

## Batch 5 — `JsonNestedIndexIT` (JSON nested)

Goal: nested JSON path (`$.address.zip`) with `JSON_VALUE` (VARCHAR-only).

### Task 5.1 — Implement `JsonNestedIndexIT`

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonNestedIndexIT.java`

- [ ] **Step 1: Write the IT.**

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.json.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.end2end.json.index.IndexUsageAssertion.Expectation;
import org.apache.phoenix.end2end.json.index.JsonBsonTestDataset.Row;
import org.apache.phoenix.end2end.json.index.JsonBsonTestReporter.QueryRecord;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class JsonNestedIndexIT extends ParallelStatsDisabledIT {

  @ClassRule
  public static final JsonBsonReportRule REPORTER_RULE = new JsonBsonReportRule();

  private static String tableName;
  private static String indexName;
  private static List<Row> rows;

  /**
   * Build a JSON document with a nested address.zip path so we exercise nesting.
   * The dataset's "zip" field maps to "$.address.zip"; "city" is duplicated under
   * "$.address.city" for readability of failures.
   */
  private static String toJsonAddress(Row r) {
    StringBuilder sb = new StringBuilder("{");
    if (r.zip != null) {
      sb.append("\"address\":{\"zip\":\"").append(r.zip).append("\",\"city\":\"")
          .append(r.city).append("\"},");
    }
    if (r.name != null) sb.append("\"name\":\"").append(r.name).append("\",");
    sb.append("\"city\":\"").append(r.city).append("\"}");
    return sb.toString();
  }

  @BeforeClass
  public static synchronized void setupSchema() throws Exception {
    tableName = "T_JSON_NESTED_" + System.currentTimeMillis();
    indexName = "IDX_JSON_NESTED_" + System.currentTimeMillis();
    rows = JsonBsonTestDataset.rows();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tableName + " (PK VARCHAR PRIMARY KEY, DOC JSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + indexName + " ON " + tableName
              + " (JSON_VALUE(DOC, '$.address.zip'))");
      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tableName + " VALUES (?, ?)")) {
        for (Row r : rows) {
          ps.setString(1, r.pk);
          ps.setString(2, toJsonAddress(r));
          ps.execute();
        }
      }
      conn.commit();
    }
    JsonBsonTestReporter.get().recordTable(new JsonBsonTestReporter.TableInfo(
        tableName, "JSON", rows.size(), indexName,
        "JSON_VALUE(DOC, '$.address.zip')"));
  }

  @AfterClass
  public static void flushReporter() throws Exception {
    JsonBsonTestReporter.get().flush();
  }

  @Test public void equality() throws Exception {
    String target = rows.get(0).zip == null ? "00001" : rows.get(0).zip;
    runCase("eq($.address.zip)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.address.zip') = '" + target + "'",
        expectedPksWhere(r -> target.equals(r.zip)));
  }

  @Test public void betweenZip() throws Exception {
    runCase("between($.address.zip 00000..50000)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.address.zip') BETWEEN '00000' AND '50000'",
        expectedPksWhere(r -> r.zip != null
            && r.zip.compareTo("00000") >= 0 && r.zip.compareTo("50000") <= 0));
  }

  @Test public void inZip() throws Exception {
    runCase("in($.address.zip)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.address.zip') IN ('00000','11111','22222','33333','44444')",
        expectedPksWhere(r -> r.zip != null
            && (r.zip.equals("00000") || r.zip.equals("11111") || r.zip.equals("22222")
                || r.zip.equals("33333") || r.zip.equals("44444"))));
  }

  @Test public void notNull() throws Exception {
    runCase("notnull($.address.zip IS NOT NULL)", Expectation.INDEX,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.address.zip') IS NOT NULL",
        expectedPksWhere(r -> r.zip != null));
  }

  @Test public void siblingPathDoesNotHit() throws Exception {
    runCase("eq($.address.city)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName
            + " WHERE JSON_VALUE(DOC, '$.address.city') = 'sf'",
        expectedPksWhere(r -> r.zip != null && "sf".equals(r.city)));
  }

  @Test public void noPredicate() throws Exception {
    runCase("scan(no predicate)", Expectation.FULL_SCAN,
        "SELECT PK FROM " + tableName,
        expectedPksWhere(r -> true));
  }

  // ---- helpers ----
  @FunctionalInterface
  private interface RowPredicate { boolean test(Row r); }

  private Set<String> expectedPksWhere(RowPredicate p) {
    Set<String> out = new TreeSet<>();
    for (Row r : rows) if (p.test(r)) out.add(r.pk);
    return out;
  }

  private void runCase(String label, Expectation expected, String sql,
      Set<String> expectedPks) throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    long t0 = System.currentTimeMillis();
    String plan = "";
    String actual = "";
    boolean pass = false;
    String err = null, stack = null;
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      plan = IndexUsageAssertion.explain(conn, sql);
      actual = IndexUsageAssertion.classify(plan, indexName);
      IndexUsageAssertion.assertExpectation(expected, plan, indexName, label);
      Set<String> got = new TreeSet<>();
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        while (rs.next()) got.add(rs.getString(1));
      }
      assertEquals("result mismatch for " + label, expectedPks, got);
      pass = true;
    } catch (Throwable t) {
      err = t.getMessage();
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      stack = sw.toString();
      throw t;
    } finally {
      long ms = System.currentTimeMillis() - t0;
      JsonBsonTestReporter.get().recordQuery(new QueryRecord(
          getClass().getSimpleName(), label, tableName, indexName, label, sql,
          plan, expected.name(), actual, pass, ms, err, stack));
    }
  }
}
```

- [ ] **Step 2: Compile + run.**

```bash
mvn -pl phoenix-core -am -DskipTests install -q 2>&1 | tail -5
./run-it-tests-local.sh --it 'JsonNestedIndexIT' --no-install 2>&1 | tail -20
```

Expected: `Tests run: 6, Failures: 0, Errors: 0` and `BUILD SUCCESS`.

- [ ] **Step 3: Commit.**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/json/index/JsonNestedIndexIT.java
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: JsonNestedIndexIT — JSON_VALUE(\$.address.zip), 6 cases"
```

---

## Batch 6 — Run all four together, fix any bug, regenerate reports

Goal: produce one merged report covering all four ITs in a single run, achieve 100% pass, and capture the per-run report files. If a query fails, fix and re-run until green.

### Task 6.1 — Run the full suite

- [ ] **Step 1: Run the four ITs together.**

```bash
./run-it-tests-local.sh \
  --it 'BsonFlatIndexIT,BsonNestedIndexIT,JsonFlatIndexIT,JsonNestedIndexIT' \
  --no-install 2>&1 | tail -50
```

Expected: cumulative `Tests run: 35, Failures: 0, Errors: 0` (12 + 9 + 8 + 6).

- [ ] **Step 2: Inspect the report files.**

```bash
ls -la phoenix-core/target/json-bson-reports/
```

Expected: at least one `.json` and one `.md` file, both with the highest run number from this invocation.

```bash
cat phoenix-core/target/json-bson-reports/json-test-report-*.md | tail -80
```

Expected: 35 query rows, 4 table rows, 0 failed details.

- [ ] **Step 3: If any test failed — root-cause and fix.**

Ordered debugging recipe:
1. Read the `.md` "Failed query details" section. It contains the SQL and EXPLAIN.
2. If `expected=INDEX, actual=DATA_FULL_SCAN` → the planner did not rewrite. Confirm by running EXPLAIN on the same SQL via `./run-it-tests-local.sh --shell` then `sqlline.py`. If the predicate is genuinely not supported (e.g. `LIKE` may not match — Phoenix's `LikeParseNode` is not a `BsonValueParseNode` even after canonicalization), update the test to `Expectation.FULL_SCAN` and add `JsonBsonTestReporter.get().recordBug("...")` describing the limitation. This is a documented limitation, not a bug.
3. If `expected=FULL_SCAN, actual=INDEX_RANGE_SCAN` → over-eager rewrite (a bug). Investigate `IndexExpressionParseNodeRewriter` and `BsonPathCanonicalizer`. Open a fix in main source under `phoenix-core-client/src/main/java/...`, write a unit test under `phoenix-core/src/test/java/...`, commit separately.
4. If `expected=INDEX, actual=INDEX_*` but result rows mismatch → real correctness bug. Walk the same path as the `BsonValueFunction.evaluate` fix from the prior session. Confirm with a unit test, commit fix, re-run.

For each iteration, after the fix:
```bash
./run-it-tests-local.sh --it 'BsonFlatIndexIT,BsonNestedIndexIT,JsonFlatIndexIT,JsonNestedIndexIT' --no-install 2>&1 | tail -30
```

Each invocation produces a new report file with an incremented run number, so the iteration trail is preserved on disk. Do not delete prior report files.

- [ ] **Step 4: Add a `.gitignore` rule for the reports dir.**

If `phoenix-core/.gitignore` does not already exclude `target/`, the existing top-level `.gitignore` should cover it. Verify:

```bash
git check-ignore phoenix-core/target/json-bson-reports/foo.json
```

Expected: prints the path (= ignored). If not, add:

```bash
echo "phoenix-core/target/json-bson-reports/" >> .gitignore
git add .gitignore
git -c commit.gpgsign=false commit --no-gpg-sign \
  -m "PHOENIX json-bson-it: gitignore report output dir"
```

(Skip the commit if `target/` is already ignored.)

- [ ] **Step 5: Final commit if any fixes landed.**

If any IT or main-source code changed during the debug loop, commit each change as its own commit. Each fix commit message starts with `PHOENIX json-bson-it: fix` and references the failing query label.

---

### Task 6.2 — Final smoke + report archive

- [ ] **Step 1: Final clean run.**

```bash
./run-it-tests-local.sh \
  --it 'BsonFlatIndexIT,BsonNestedIndexIT,JsonFlatIndexIT,JsonNestedIndexIT' \
  --no-install 2>&1 | tail -25
```

Expected: green, all 35 tests pass.

- [ ] **Step 2: Confirm reports exist and are well-formed.**

```bash
ls phoenix-core/target/json-bson-reports/
python3 -m json.tool phoenix-core/target/json-bson-reports/json-test-report-*.json > /dev/null && echo "JSON valid"
```

Expected: at least 2 file pairs, "JSON valid" printed for each.

- [ ] **Step 3: Run BSON regression check (no surprises in pre-existing tests).**

```bash
./run-it-tests-local.sh \
  --it 'Bson1IT,Bson2IT,Bson3IT,Bson4IT,Bson5IT,Bson6IT,BsonPathIndex*IT' \
  --no-install 2>&1 | tail -15
```

Expected: all green; the new ITs do not change any existing IT behavior.

---

## Self-review checklist (run before declaring "plan complete")

1. Spec coverage — each spec section maps to a batch:
   - §4.1 dataset → B1.T1
   - §4.2 IndexUsageAssertion → B1.T2
   - §4.3 reporter → B1.T3
   - §4.4 listener/rule → B1.T4
   - §4.5 four ITs → B2/B3/B4/B5
   - §7 testing strategy → B6
2. Placeholder scan — no "TBD"/"TODO"/"…" or "similar to" in any task.
3. Type consistency — `Row.score` is `Long` everywhere; `Expectation.INDEX` and
   `Expectation.FULL_SCAN` are the only two values used. `recordTable` /
   `recordQuery` signatures match across reporter and IT call sites.
4. The total green test count expected in B6 (35) equals the sum of B2 (12) + B3
   (9) + B4 (8) + B5 (6).
5. Every commit uses `-c commit.gpgsign=false ... --no-gpg-sign` per repo policy.
