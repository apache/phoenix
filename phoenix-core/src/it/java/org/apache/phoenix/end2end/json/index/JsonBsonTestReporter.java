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
