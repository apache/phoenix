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
package org.apache.phoenix.query.explain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;

/**
 * Test helpers for retrieving the {@link ExplainPlan} and its structured
 * {@link ExplainPlanAttributes} for a query or mutation (without going through the textual
 * {@code EXPLAIN ...} ResultSet path), plus a fluent {@link ExplainPlanAssert} API for asserting on
 * the attribute values.
 */
public final class ExplainPlanTestUtil {

  private ExplainPlanTestUtil() {
  }

  /** Optimize {@code query} and return its {@link ExplainPlan}. */
  public static ExplainPlan getExplainPlan(Connection conn, String query) throws SQLException {
    PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
    return statement.optimizeQuery().getExplainPlan();
  }

  /** Optimize {@code query} and return its structured {@link ExplainPlanAttributes}. */
  public static ExplainPlanAttributes getExplainAttributes(Connection conn, String query)
    throws SQLException {
    return getExplainPlan(conn, query).getPlanStepsAsAttributes();
  }

  /** Optimize {@code query} and return its plan-steps text. */
  public static List<String> getPlanSteps(Connection conn, String query) throws SQLException {
    return getExplainPlan(conn, query).getPlanSteps();
  }

  /** Compile a mutation (UPSERT/DELETE) and return its {@link ExplainPlan}. */
  public static ExplainPlan getMutationExplainPlan(Connection conn, String query)
    throws SQLException {
    PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
    return statement.compileMutation().getExplainPlan();
  }

  /** Compile a mutation (UPSERT/DELETE) and return its structured {@link ExplainPlanAttributes}. */
  public static ExplainPlanAttributes getMutationExplainAttributes(Connection conn, String query)
    throws SQLException {
    return getMutationExplainPlan(conn, query).getPlanStepsAsAttributes();
  }

  /** Begin assertions on the given attributes. */
  public static ExplainPlanAssert assertPlan(ExplainPlanAttributes attributes) {
    assertNotNull("ExplainPlanAttributes must not be null", attributes);
    return new ExplainPlanAssert(attributes, null, "plan");
  }

  /** Optimize {@code query} on {@code conn} and begin assertions on its plan attributes. */
  public static ExplainPlanAssert assertPlan(Connection conn, String query) throws SQLException {
    return assertPlan(getExplainAttributes(conn, query));
  }

  /**
   * Optimize an already-prepared and, if needed, parameter-bound {@link PhoenixPreparedStatement}
   * and begin assertions on its plan attributes.
   */
  public static ExplainPlanAssert assertPlan(PhoenixPreparedStatement statement)
    throws SQLException {
    return assertPlan(statement.optimizeQuery().getExplainPlan().getPlanStepsAsAttributes());
  }

  /** Compile the mutation {@code query} on {@code conn} and begin assertions on its attributes. */
  public static ExplainPlanAssert assertMutationPlan(Connection conn, String query)
    throws SQLException {
    return assertPlan(getMutationExplainAttributes(conn, query));
  }

  /**
   * Compile an already-prepared and, if needed, parameter-bound mutation
   * {@link PhoenixPreparedStatement} and begin assertions on its attributes.
   */
  public static ExplainPlanAssert assertMutationPlan(PhoenixPreparedStatement statement)
    throws SQLException {
    return assertPlan(statement.compileMutation().getExplainPlan().getPlanStepsAsAttributes());
  }

  /** Fluent assertions over {@link ExplainPlanAttributes}. */
  public static final class ExplainPlanAssert {

    private final ExplainPlanAttributes attributes;
    private final ExplainPlanAssert parent;
    private final String context;

    private ExplainPlanAssert(ExplainPlanAttributes attributes, ExplainPlanAssert parent,
      String context) {
      this.attributes = attributes;
      this.parent = parent;
      this.context = context;
    }

    public ExplainPlanAttributes attributes() {
      return attributes;
    }

    /**
     * Assert the scan type, e.g. {@code "FULL SCAN"}, {@code "RANGE SCAN"},
     * {@code "POINT LOOKUP ON 1 KEY"}, {@code "SKIP SCAN ON 3 KEYS"}.
     */
    public ExplainPlanAssert scanType(String expected) {
      assertEquals(at("explainScanType"), trim(expected), trim(attributes.getExplainScanType()));
      return this;
    }

    /** Assert the scanned table (or index) name. */
    public ExplainPlanAssert table(String expected) {
      assertEquals(at("tableName"), expected, attributes.getTableName());
      return this;
    }

    /**
     * Assert the scanned table or index name contains {@code expected}. Useful for "index used"
     * checks where the exact name carries a suffix.
     */
    public ExplainPlanAssert tableContains(String expected) {
      String actual = attributes.getTableName();
      assertNotNull(at("tableName") + " must not be null", actual);
      assertTrue(
        at("tableName") + " expected to contain '" + expected + "' but was '" + actual + "'",
        actual.contains(expected));
      return this;
    }

    /** Assert the hex-string row-value-constructor offset marker. */
    public ExplainPlanAssert hexStringRVCOffset(String expected) {
      assertEquals(at("hexStringRVCOffset"), expected, attributes.getHexStringRVCOffset());
      return this;
    }

    /** Assert the key ranges string (exact; leading space is significant). */
    public ExplainPlanAssert keyRanges(String expected) {
      assertEquals(at("keyRanges"), expected, attributes.getKeyRanges());
      return this;
    }

    public ExplainPlanAssert abstractExplainPlan(String expected) {
      assertEquals(at("abstractExplainPlan"), expected, attributes.getAbstractExplainPlan());
      return this;
    }

    /** Assert the read consistency level. */
    public ExplainPlanAssert consistency(String expected) {
      assertEquals(at("consistency"), expected,
        attributes.getConsistency() == null ? null : attributes.getConsistency().name());
      return this;
    }

    public ExplainPlanAssert serverWhereFilter(String expected) {
      assertEquals(at("serverWhereFilter"), expected, attributes.getServerWhereFilter());
      return this;
    }

    public ExplainPlanAssert serverAggregate(String expected) {
      assertEquals(at("serverAggregate"), expected, attributes.getServerAggregate());
      return this;
    }

    public ExplainPlanAssert serverSortedBy(String expected) {
      assertEquals(at("serverSortedBy"), expected, attributes.getServerSortedBy());
      return this;
    }

    public ExplainPlanAssert serverDistinctFilter(String expected) {
      assertEquals(at("serverDistinctFilter"), expected, attributes.getServerDistinctFilter());
      return this;
    }

    public ExplainPlanAssert serverMergeColumns(String expected) {
      assertEquals(at("serverMergeColumns"), expected,
        attributes.getServerMergeColumns() == null
          ? null
          : attributes.getServerMergeColumns().toString());
      return this;
    }

    public ExplainPlanAssert clientFilterBy(String expected) {
      assertEquals(at("clientFilterBy"), expected, attributes.getClientFilterBy());
      return this;
    }

    public ExplainPlanAssert clientAggregate(String expected) {
      assertEquals(at("clientAggregate"), expected, attributes.getClientAggregate());
      return this;
    }

    public ExplainPlanAssert clientSortedBy(String expected) {
      assertEquals(at("clientSortedBy"), expected, attributes.getClientSortedBy());
      return this;
    }

    public ExplainPlanAssert clientAfterAggregate(String expected) {
      assertEquals(at("clientAfterAggregate"), expected, attributes.getClientAfterAggregate());
      return this;
    }

    public ExplainPlanAssert clientDistinctFilter(String expected) {
      assertEquals(at("clientDistinctFilter"), expected, attributes.getClientDistinctFilter());
      return this;
    }

    public ExplainPlanAssert clientSortAlgo(String expected) {
      assertEquals(at("clientSortAlgo"), expected, attributes.getClientSortAlgo());
      return this;
    }

    public ExplainPlanAssert serverRowLimit(Long expected) {
      assertEquals(at("serverRowLimit"), expected, attributes.getServerRowLimit());
      return this;
    }

    public ExplainPlanAssert serverGroupByLimit(Integer expected) {
      assertEquals(at("serverGroupByLimit"), expected, attributes.getServerGroupByLimit());
      return this;
    }

    public ExplainPlanAssert clientRowLimit(Integer expected) {
      assertEquals(at("clientRowLimit"), expected, attributes.getClientRowLimit());
      return this;
    }

    public ExplainPlanAssert serverOffset(Integer expected) {
      assertEquals(at("serverOffset"), expected, attributes.getServerOffset());
      return this;
    }

    public ExplainPlanAssert clientOffset(Integer expected) {
      assertEquals(at("clientOffset"), expected, attributes.getClientOffset());
      return this;
    }

    public ExplainPlanAssert clientSequenceCount(Integer expected) {
      assertEquals(at("clientSequenceCount"), expected, attributes.getClientSequenceCount());
      return this;
    }

    public ExplainPlanAssert hint(String expected) {
      assertEquals(at("hint"), expected,
        attributes.getHint() == null ? null : attributes.getHint().toString());
      return this;
    }

    public ExplainPlanAssert samplingRate(Double expected) {
      assertEquals(at("samplingRate"), expected, attributes.getSamplingRate());
      return this;
    }

    public ExplainPlanAssert serverArrayElementProjection(boolean expected) {
      assertEquals(at("serverArrayElementProjection"), expected,
        attributes.isServerArrayElementProjection());
      return this;
    }

    public ExplainPlanAssert useRoundRobinIterator(boolean expected) {
      assertEquals(at("useRoundRobinIterator"), expected, attributes.isUseRoundRobinIterator());
      return this;
    }

    public ExplainPlanAssert dynamicServerFilter(String expected) {
      assertEquals(at("dynamicServerFilter"), expected, attributes.getDynamicServerFilter());
      return this;
    }

    public ExplainPlanAssert afterJoinFilter(String expected) {
      assertEquals(at("afterJoinFilter"), expected, attributes.getAfterJoinFilter());
      return this;
    }

    public ExplainPlanAssert joinScannerLimit(Long expected) {
      assertEquals(at("joinScannerLimit"), expected, attributes.getJoinScannerLimit());
      return this;
    }

    /** Assert the sort-merge-join "(SKIP MERGE)" marker on this (left) side of the join. */
    public ExplainPlanAssert sortMergeSkipMerge(boolean expected) {
      assertEquals(at("sortMergeSkipMerge"), expected, attributes.isSortMergeSkipMerge());
      return this;
    }

    /** Assert the (normalized) parallelism width, e.g. {@code "PARALLEL"} or {@code "SERIAL"}. */
    public ExplainPlanAssert iteratorType(String expectedPrefix) {
      String iter = attributes.getIteratorTypeAndScanSize();
      assertNotNull(at("iteratorTypeAndScanSize"), iter);
      assertTrue(at("iteratorTypeAndScanSize") + " expected to start with '" + expectedPrefix
        + "' but was '" + iter + "'", iter.startsWith(expectedPrefix));
      return this;
    }

    /** Navigate to the right-hand side plan (sort-merge join / union all). */
    public ExplainPlanAssert rhs() {
      ExplainPlanAttributes rhs = attributes.getRhsJoinQueryExplainPlan();
      assertNotNull(at("rhsJoinQueryExplainPlan") + " must not be null", rhs);
      return new ExplainPlanAssert(rhs, this, context + ".rhs");
    }

    /** Assert the number of hash-join sub-plans (children). */
    public ExplainPlanAssert subPlanCount(int expected) {
      List<ExplainPlanAttributes> subPlans = attributes.getSubPlans();
      int actual = subPlans == null ? 0 : subPlans.size();
      assertEquals(at("subPlans.size"), expected, actual);
      return this;
    }

    /** Navigate to the i-th hash-join sub-plan (child). */
    public ExplainPlanAssert subPlan(int i) {
      List<ExplainPlanAttributes> subPlans = attributes.getSubPlans();
      assertNotNull(at("subPlans") + " must not be null", subPlans);
      assertTrue(at("subPlans") + " has no index " + i + " (size=" + subPlans.size() + ")",
        i >= 0 && i < subPlans.size());
      return new ExplainPlanAssert(subPlans.get(i), this, context + ".subPlan[" + i + "]");
    }

    /**
     * Return to the parent assertion after navigating into {@link #rhs()} or {@link #subPlan(int)}.
     */
    public ExplainPlanAssert end() {
      assertNotNull("end() called on a root ExplainPlanAssert", parent);
      return parent;
    }

    private String at(String field) {
      return context + "." + field;
    }

    private static String trim(String s) {
      return s == null ? null : s.trim();
    }
  }
}
