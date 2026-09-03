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
package org.apache.phoenix.compile;

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.ENTITY_HISTORY_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH microbenchmark comparing v1 vs v2 WHERE optimizer compile-time latency. Measures the
 * time to compile a SQL statement (parse + resolve + WHERE push + plan) under each flag
 * setting, which isolates optimizer cost without HBase network noise.
 * <p>
 * Workloads cover representative shapes that stress the legacy code path:
 * <ul>
 *   <li>RVC inequality ({@code (a, b) >= (v1, v2)}) — the canonical lex-expand case</li>
 *   <li>RVC IN list of configurable size — the compound-byte vs per-dim encoding trade-off</li>
 *   <li>OR chain on a leading PK column — exercises KeySpaceList merge fixpoint</li>
 *   <li>Mixed scalar equalities + RVC inequality — composes multiple predicates</li>
 * </ul>
 * <p>
 * Run with {@code mvn -pl phoenix-core test -Dtest=WhereOptimizerBenchmark} to invoke the
 * main method; JMH prints a table like
 *
 * <pre>
 * Benchmark                                    (flag)  (size)  Mode  Cnt   Score   Error  Units
 * WhereOptimizerBenchmark.rvcInequality            v1       -  avgt   10  45.2 ± 2.1  us/op
 * WhereOptimizerBenchmark.rvcInequality            v2       -  avgt   10  38.7 ± 1.8  us/op
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
// Forking disabled: running via mvn exec:java, forked JVMs don't inherit the surefire/test
// classpath so JMH's ForkedMain can't be loaded. The cost is losing JVM isolation between
// benchmarks; warmup + DCE protection still apply.
@Fork(0)
@State(Scope.Benchmark)
public class WhereOptimizerBenchmark extends BaseConnectionlessQueryTest {

  @Param({ "v1", "v2" })
  public String flag;

  /** Size parameter for workloads that scale (IN list cardinality, OR chain length). */
  @Param({ "5", "50", "500" })
  public int size;

  private PhoenixTestDriver driver;

  private String rvcInequalitySql;
  private String rvcInListSql;
  private String orChainSql;
  private String mixedPredicatesSql;
  private String cartesianExplosionSql;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    // Tear down any prior driver from the previous @Param combination.
    for (java.util.Enumeration<java.sql.Driver> e = DriverManager.getDrivers();
      e.hasMoreElements(); ) {
      java.sql.Driver d = e.nextElement();
      if (d instanceof PhoenixTestDriver) {
        try {
          ((PhoenixTestDriver) d).close();
        } catch (Exception ignored) {
          // best effort
        }
        DriverManager.deregisterDriver(d);
      }
    }
    Map<String, String> props = new HashMap<>();
    props.put(QueryServices.WHERE_OPTIMIZER_V2_ENABLED, "v2".equals(flag) ? "true" : "false");
    driver = new PhoenixTestDriver(new ReadOnlyProps(props));
    DriverManager.registerDriver(driver);
    ensureTableCreated(PHOENIX_CONNECTIONLESS_JDBC_URL, ATABLE_NAME);
    ensureTableCreated(PHOENIX_CONNECTIONLESS_JDBC_URL, ENTITY_HISTORY_TABLE_NAME);
    ensureCartesianExplosionTable();

    rvcInequalitySql =
      "select * from " + ATABLE_NAME + " where (organization_id, entity_id) >= (?, ?)";
    rvcInListSql = buildRvcInList(size);
    orChainSql = buildOrChain(size);
    mixedPredicatesSql = "select * from " + ENTITY_HISTORY_TABLE_NAME
      + " where organization_id = ? and (organization_id, parent_id) >= (?, ?) "
      + "and parent_id in (?, ?, ?, ?, ?)";
    cartesianExplosionSql = buildCartesianExplosion(size);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    if (driver != null) {
      try {
        driver.close();
      } finally {
        DriverManager.deregisterDriver(driver);
        driver = null;
      }
    }
  }

  private static String buildRvcInList(int n) {
    StringBuilder sb =
      new StringBuilder("select * from " + ATABLE_NAME + " where (organization_id, entity_id) in (");
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("(?, ?)");
    }
    sb.append(')');
    return sb.toString();
  }

  private static String buildOrChain(int n) {
    StringBuilder sb = new StringBuilder("select * from " + ATABLE_NAME + " where ");
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(" or ");
      }
      sb.append("organization_id = ?");
    }
    return sb.toString();
  }

  private static final String CART_TABLE = "CART_EXPLOSION_T";

  /**
   * Creates a table with 3 contiguous CHAR PK columns so we can expand independent IN
   * lists on the middle and trailing PK dims and observe the per-dim cartesian blow-up.
   * Using CHAR everywhere keeps param binding identical to the other benchmarks
   * (all strings, all {@code pstmt.setString}). The real {@code entity_history} table
   * has {@code created_date DATE} which would require a different binding path.
   */
  private void ensureCartesianExplosionTable() throws Exception {
    try (java.sql.Connection conn =
      DriverManager.getConnection(PHOENIX_CONNECTIONLESS_JDBC_URL)) {
      try {
        conn.createStatement().execute(
          "CREATE TABLE " + CART_TABLE + " (a CHAR(15) NOT NULL, b CHAR(15) NOT NULL, "
            + "c CHAR(15) NOT NULL, d CHAR(15) NOT NULL, v VARCHAR, "
            + "CONSTRAINT pk PRIMARY KEY(a, b, c, d))");
      } catch (java.sql.SQLException e) {
        // Already exists from a prior @Param combination or prior run.
      }
    }
  }

  /**
   * Builds a query whose per-column cartesian product exceeds the skip-scan bound
   * (50,000 by default in both v1 and v2) so that v2's "drop trailing dim" rule is
   * exercised. The table's PK is {@code (a, b, c, d)} with all four columns CHAR(15).
   * Query is {@code a = ? AND b IN (...) AND c IN (...) AND d IN (...)}; pinning dim 0
   * and expanding IN lists of size n on dims 1, 2, 3, so the naive cross-product is
   * {@code 1 · n · n · n = n³}.
   * <p>
   * At {@code n=5} the product is 125 (fits under 50,000); at {@code n=50} it's 125,000
   * (exceeds — trip at dim 2 since {@code 1·50·50·50 = 125,000} but the running product
   * already hits {@code 2,500} at dim 1, {@code 125,000} at dim 2 → v2 drops dim 3); at
   * {@code n=500} the product is 1.25×10⁸ — v2 drops dim 2 and dim 3.
   * <p>
   * When the bound trips, v2's extractor stops at the slot that tripped it — so the
   * trailing dim(s) are dropped from the emitted scan ranges and their IN predicates
   * move into the residual filter. V1 accumulates the full cartesian cardinality in
   * {@code inListSkipScanCardinality} and flips {@code forcedRangeScan=true}, widening
   * to a plain range scan over the whole dim-1 bounding box.
   */
  private static String buildCartesianExplosion(int n) {
    StringBuilder sb =
      new StringBuilder("select * from " + CART_TABLE + " where a = ? and b in (");
    for (int i = 0; i < n; i++) {
      if (i > 0) sb.append(',');
      sb.append('?');
    }
    sb.append(") and c in (");
    for (int i = 0; i < n; i++) {
      if (i > 0) sb.append(',');
      sb.append('?');
    }
    sb.append(") and d in (");
    for (int i = 0; i < n; i++) {
      if (i > 0) sb.append(',');
      sb.append('?');
    }
    sb.append(')');
    return sb.toString();
  }

  /** Canonical RVC inequality: {@code (a, b) >= (?, ?)}. Static size. */
  @Benchmark
  public void rvcInequality(Blackhole bh) throws Exception {
    compileAndSink(rvcInequalitySql, 2, bh);
  }

  /** RVC IN list: {@code (a, b) IN ((?, ?), …)} — scales with {@code size}. */
  @Benchmark
  public void rvcInList(Blackhole bh) throws Exception {
    compileAndSink(rvcInListSql, 2 * size, bh);
  }

  /** OR chain on a leading PK column: {@code a = ? OR a = ? OR …} — scales with {@code size}. */
  @Benchmark
  public void orChain(Blackhole bh) throws Exception {
    compileAndSink(orChainSql, size, bh);
  }

  /** Mixed equalities + RVC inequality + scalar IN — exercises multi-predicate composition. */
  @Benchmark
  public void mixedPredicates(Blackhole bh) throws Exception {
    compileAndSink(mixedPredicatesSql, 8, bh);
  }

  /**
   * {@code org_id = ? AND parent_id IN (... n ...) AND entity_history_id IN (... n ...)}
   * on a 4-col PK. The per-column cartesian product is n² — at size=500 that's 250,000,
   * far exceeding both v1's MAX_IN_LIST_SKIP_SCAN_SIZE (50k) and v2's
   * WHERE_OPTIMIZER_V2_CARTESIAN_BOUND (same 50k by default). V2 detects this during
   * extraction and drops the trailing entity_history_id slot before emitting ranges; v1
   * builds both slot lists then flips forced-range-scan. Observe the compile-time delta.
   */
  @Benchmark
  public void cartesianExplosion(Blackhole bh) throws Exception {
    // 1 (a) + size (b IN) + size (c IN) + size (d IN) parameters.
    compileAndSink(cartesianExplosionSql, 1 + 3 * size, bh);
  }

  private void compileAndSink(String sql, int paramCount, Blackhole bh) throws Exception {
    Properties props = new Properties();
    java.sql.Connection conn = DriverManager.getConnection(PHOENIX_CONNECTIONLESS_JDBC_URL, props);
    try {
      PhoenixPreparedStatement pstmt =
        (PhoenixPreparedStatement) conn.prepareStatement(sql);
      for (int i = 1; i <= paramCount; i++) {
        // Alternate string values so IN-list entries are distinct; PVarchar columns in atable
        // and entity_history accept strings.
        pstmt.setString(i, "000000000000" + String.format("%03d", i));
      }
      bh.consume(pstmt.compileQuery());
    } finally {
      conn.close();
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.runner.options.ChainedOptionsBuilder builder =
      new OptionsBuilder().include(WhereOptimizerBenchmark.class.getSimpleName());
    // Pass "prof" as the first arg (via -Dexec.args=prof) to enable the stack sampler on
    // just the orChain@size=500 benchmark. Uses longer iterations so samples land in the
    // hot path rather than in JMH's own setup/teardown.
    if (args.length > 0 && "prof".equals(args[0])) {
      builder = builder.include(".*orChain")
        .param("size", "500")
        .param("flag", "v2")
        .warmupIterations(2).warmupTime(org.openjdk.jmh.runner.options.TimeValue.seconds(3))
        .measurementIterations(3).measurementTime(org.openjdk.jmh.runner.options.TimeValue.seconds(5))
        // period=1ms samples frequently enough for 2ms/op work. Fall back to stack if
        // async isn't on the path.
        .addProfiler("stack", "period=1;lines=10;top=40");
    }
    new Runner(builder.build()).run();
  }

}
