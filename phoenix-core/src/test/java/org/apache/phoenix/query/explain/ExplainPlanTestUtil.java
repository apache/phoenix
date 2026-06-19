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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.optimize.OptimizerReasons;
import org.apache.phoenix.optimize.RejectedIndexEntry;
import org.apache.phoenix.parse.ExplainOptions;
import org.apache.phoenix.parse.UpsertStatement.OnDuplicateKeyType;

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
    try (PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
      return statement.optimizeQuery().getExplainPlan();
    }
  }

  /** Optimize {@code query} and return its structured {@link ExplainPlanAttributes}. */
  public static ExplainPlanAttributes getExplainAttributes(Connection conn, String query)
    throws SQLException {
    return getExplainPlan(conn, query).getPlanStepsAsAttributes();
  }

  /**
   * Optimize {@code query} and return its structured {@link ExplainPlanAttributes} with the given
   * {@link ExplainOptions} applied to the plan's {@code StatementContext}. Used to exercise region
   * location information, which is only populated when the {@code REGIONS} option is requested.
   */
  public static ExplainPlanAttributes getExplainAttributes(Connection conn, String query,
    ExplainOptions options) throws SQLException {
    try (PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
      QueryPlan plan = statement.optimizeQuery();
      plan.getContext().setExplainOptions(options);
      return plan.getExplainPlan().getPlanStepsAsAttributes();
    }
  }

  /** Optimize {@code query} and return its plan-steps text. */
  public static List<String> getPlanSteps(Connection conn, String query) throws SQLException {
    return getExplainPlan(conn, query).getPlanSteps();
  }

  /**
   * Optimize {@code query} with the given {@link ExplainOptions} applied to the plan's
   * {@code StatementContext} and return its plan-steps text.
   */
  public static List<String> getPlanSteps(Connection conn, String query, ExplainOptions options)
    throws SQLException {
    try (PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
      QueryPlan plan = statement.optimizeQuery();
      plan.getContext().setExplainOptions(options);
      return plan.getExplainPlan().getPlanSteps();
    }
  }

  /** Compile a mutation (UPSERT/DELETE) and return its {@link ExplainPlan}. */
  public static ExplainPlan getMutationExplainPlan(Connection conn, String query)
    throws SQLException {
    try (PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)) {
      return statement.compileMutation().getExplainPlan();
    }
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
   * Optimize {@code query} on {@code conn} with the {@code REGIONS} option enabled and begin
   * assertions on its plan attributes. Use this instead of {@link #assertPlan(Connection, String)}
   * when asserting on region-location attributes, which are only populated when {@code REGIONS} is
   * requested.
   */
  public static ExplainPlanAssert assertPlanWithRegions(Connection conn, String query)
    throws SQLException {
    return assertPlan(getExplainAttributes(conn, query, ExplainOptions.WITH_REGIONS));
  }

  /**
   * Optimize {@code query} on {@code conn} with the {@code VERBOSE} option enabled and begin
   * assertions on its plan attributes. Use this when asserting on VERBOSE-only attributes such as
   * {@code serverProject}, {@code serverFilters}, and {@code ignoredHints}.
   */
  public static ExplainPlanAssert assertPlanWithVerbose(Connection conn, String query)
    throws SQLException {
    return assertPlan(getExplainAttributes(conn, query, ExplainOptions.VERBOSE));
  }

  /**
   * Optimize an already-prepared and, if needed, parameter-bound {@link PhoenixPreparedStatement}
   * and begin assertions on its plan attributes.
   */
  public static ExplainPlanAssert assertPlan(PhoenixPreparedStatement statement)
    throws SQLException {
    return assertPlan(statement.optimizeQuery().getExplainPlan().getPlanStepsAsAttributes());
  }

  /** Begin assertions on the plan attributes of a resolved {@link QueryPlan}. */
  public static ExplainPlanAssert assertPlan(QueryPlan queryPlan) throws SQLException {
    return assertPlan(queryPlan.getExplainPlan().getPlanStepsAsAttributes());
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

    /**
     * Assert the scan type starts with {@code prefix}, useful for variable width prefixes such as
     * {@code "POINT LOOKUP ON "} where the number of keys is data dependent.
     */
    public ExplainPlanAssert scanTypeStartsWith(String prefix) {
      String actual = attributes.getExplainScanType();
      assertNotNull(at("explainScanType") + " must not be null", actual);
      assertTrue(
        at("explainScanType") + " expected to start with '" + prefix + "' but was '" + actual + "'",
        actual.startsWith(prefix));
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

    /** Assert the chosen per-scan index (or data table) name. */
    public ExplainPlanAssert indexName(String expected) {
      assertEquals(at("indexName"), expected, attributes.getIndexName());
      return this;
    }

    /**
     * Assert the per-scan index kind token: {@code "LOCAL"}, {@code "GLOBAL"}, or
     * {@code "UNCOVERED GLOBAL"}.
     */
    public ExplainPlanAssert indexKind(String expected) {
      assertEquals(at("indexKind"), expected, attributes.getIndexKind());
      return this;
    }

    /**
     * Assert the optimizer's index-selection rule label, e.g. one of the
     * {@code OptimizerReasons.RULE_*} constants.
     */
    public ExplainPlanAssert indexRule(String expected) {
      assertEquals(at("indexRule"), expected, attributes.getIndexRule());
      return this;
    }

    /**
     * Assert the optimizer's separate functional-index match disclosure equals
     * {@code "matches <expression>"} (see {@link OptimizerReasons#matches(String)}). The selection
     * {@code indexRule} is disclosed independently and is asserted with {@link #indexRule(String)}.
     */
    public ExplainPlanAssert functionalMatch(String expression) {
      assertEquals(at("functionalMatch"), OptimizerReasons.matches(expression),
        attributes.getFunctionalMatch());
      return this;
    }

    /** Assert no functional-index match disclosure was recorded for this plan. */
    public ExplainPlanAssert functionalMatchNone() {
      assertEquals(at("functionalMatch"), null, attributes.getFunctionalMatch());
      return this;
    }

    /** Assert the number of rejected index candidates recorded for this plan. */
    public ExplainPlanAssert indexRejectedCount(int expected) {
      List<RejectedIndexEntry> rejected = attributes.getIndexRejected();
      int actual = rejected == null ? 0 : rejected.size();
      assertEquals(at("indexRejected.size"), expected, actual);
      return this;
    }

    /** Assert the i-th rejected index candidate's name and reason. */
    public ExplainPlanAssert indexRejected(int i, String name, String reason) {
      List<RejectedIndexEntry> rejected = attributes.getIndexRejected();
      assertNotNull(at("indexRejected") + " must not be null", rejected);
      assertTrue(at("indexRejected") + " has no index " + i + " (size=" + rejected.size() + ")",
        i >= 0 && i < rejected.size());
      RejectedIndexEntry entry = rejected.get(i);
      assertEquals(at("indexRejected[" + i + "].name"), name, entry.getName());
      assertEquals(at("indexRejected[" + i + "].reason"), reason, entry.getReason());
      return this;
    }

    /** Assert that no index candidates were rejected (null or empty list). */
    public ExplainPlanAssert indexRejectedNone() {
      List<RejectedIndexEntry> rejected = attributes.getIndexRejected();
      assertTrue(at("indexRejected") + " expected none but was " + rejected,
        rejected == null || rejected.isEmpty());
      return this;
    }

    /**
     * Assert that the rejected index list contains an entry with {@code name} and {@code reason}.
     */
    public ExplainPlanAssert indexRejectedContains(String name, String reason) {
      List<RejectedIndexEntry> rejected = attributes.getIndexRejected();
      assertNotNull(at("indexRejected") + " must not be null", rejected);
      for (RejectedIndexEntry entry : rejected) {
        if (entry.getName().equals(name) && entry.getReason().equals(reason)) {
          return this;
        }
      }
      throw new AssertionError(at("indexRejected") + " expected to contain {" + name + ", " + reason
        + "} but was " + rejected);
    }

    /** Assert the salt bucket count of the scanned table (null when not salted). */
    public ExplainPlanAssert saltBuckets(Integer expected) {
      assertEquals(at("saltBuckets"), expected, attributes.getSaltBuckets());
      return this;
    }

    /** Assert the number of regions the scan is planned to hit (null when unknown). */
    public ExplainPlanAssert regionsPlanned(Integer expected) {
      assertEquals(at("regionsPlanned"), expected, attributes.getRegionsPlanned());
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

    /**
     * Assert the {@code ON DUPLICATE KEY} flavor disclosed on an atomic UPSERT mutation operator.
     */
    public ExplainPlanAssert onDuplicateKeyAction(OnDuplicateKeyType expected) {
      assertEquals(at("onDuplicateKeyAction"), expected, attributes.getOnDuplicateKeyAction());
      return this;
    }

    /**
     * Assert the ordered {@code <col> = <expr>} assignments disclosed under
     * {@code ON DUPLICATE KEY UPDATE}.
     */
    public ExplainPlanAssert serverUpdateSet(String... expected) {
      assertEquals(at("serverUpdateSet"), Arrays.asList(expected), attributes.getServerUpdateSet());
      return this;
    }

    /** Assert the number of {@code SERVER UPDATE SET} assignments. */
    public ExplainPlanAssert serverUpdateSetCount(int expected) {
      List<String> actual = attributes.getServerUpdateSet();
      int actualCount = actual == null ? 0 : actual.size();
      assertEquals(at("serverUpdateSet.size"), expected, actualCount);
      return this;
    }

    /** Assert the mutation operator discloses {@code RETURNING *}. */
    public ExplainPlanAssert returningRow() {
      assertTrue(at("returningRow") + " expected true", attributes.isReturningRow());
      return this;
    }

    /** Assert the mutation operator does not disclose {@code RETURNING *}. */
    public ExplainPlanAssert noReturningRow() {
      assertTrue(at("returningRow") + " expected false", !attributes.isReturningRow());
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

    /** Assert that {@code serverWhereFilter} is equal to one of the {@code allowed} values. */
    public ExplainPlanAssert serverWhereFilterAnyOf(String... allowed) {
      String actual = attributes.getServerWhereFilter();
      for (String s : allowed) {
        if (s == null ? actual == null : s.equals(actual)) {
          return this;
        }
      }
      throw new AssertionError(at("serverWhereFilter") + " expected one of "
        + Arrays.toString(allowed) + " but was " + actual);
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

    /** Assert the entire server-parsed-projection map matches {@code expected}. */
    public ExplainPlanAssert serverParsedProjections(Map<String, List<String>> expected) {
      assertEquals(at("serverParsedProjections"), expected,
        attributes.getServerParsedProjections());
      return this;
    }

    /**
     * Assert that the named bucket ({@code ARRAY}, {@code JSON}, {@code BSON}) holds exactly the
     * listed per-expression renderings, in order.
     */
    public ExplainPlanAssert serverParsedProjections(String label, String... expected) {
      Map<String, List<String>> actual = attributes.getServerParsedProjections();
      assertNotNull(at("serverParsedProjections") + " must not be null", actual);
      List<String> bucket = actual.get(label);
      assertNotNull(at("serverParsedProjections[" + label + "]") + " must not be null", bucket);
      assertEquals(at("serverParsedProjections[" + label + "]"), Arrays.asList(expected), bucket);
      return this;
    }

    /** Assert that no server-parsed projections were disclosed (null or empty). */
    public ExplainPlanAssert serverParsedProjectionsNone() {
      Map<String, List<String>> actual = attributes.getServerParsedProjections();
      assertTrue(at("serverParsedProjections") + " expected none but was " + actual,
        actual == null || actual.isEmpty());
      return this;
    }

    /** Assert the number of expressions in the named bucket. */
    public ExplainPlanAssert serverParsedProjectionCount(String label, int expected) {
      Map<String, List<String>> actual = attributes.getServerParsedProjections();
      List<String> bucket = actual == null ? null : actual.get(label);
      int actualCount = bucket == null ? 0 : bucket.size();
      assertEquals(at("serverParsedProjections[" + label + "].size"), expected, actualCount);
      return this;
    }

    /** Assert the entire VERBOSE {@code PROJECT} column list matches {@code expected}, in order. */
    public ExplainPlanAssert serverProject(String... expected) {
      List<String> actual = attributes.getServerProject();
      List<String> actualOrEmpty = actual == null ? Collections.<String> emptyList() : actual;
      assertEquals(at("serverProject"), Arrays.asList(expected), actualOrEmpty);
      return this;
    }

    /** Assert the number of VERBOSE {@code PROJECT} columns. */
    public ExplainPlanAssert serverProjectCount(int expected) {
      List<String> actual = attributes.getServerProject();
      int actualCount = actual == null ? 0 : actual.size();
      assertEquals(at("serverProject.size"), expected, actualCount);
      return this;
    }

    /** Assert that no VERBOSE {@code PROJECT} disclosure was emitted (null or empty). */
    public ExplainPlanAssert serverProjectNone() {
      List<String> actual = attributes.getServerProject();
      assertTrue(at("serverProject") + " expected none but was " + actual,
        actual == null || actual.isEmpty());
      return this;
    }

    /** Assert the number of VERBOSE server filter predicates. */
    public ExplainPlanAssert serverFilterCount(int expected) {
      List<ExplainPlanAttributes.ExplainFilter> actual = attributes.getServerFilters();
      int actualCount = actual == null ? 0 : actual.size();
      assertEquals(at("serverFilters.size"), expected, actualCount);
      return this;
    }

    /** Assert that no VERBOSE server filter breakdown was emitted (null or empty). */
    public ExplainPlanAssert serverFiltersNone() {
      List<ExplainPlanAttributes.ExplainFilter> actual = attributes.getServerFilters();
      assertTrue(at("serverFilters") + " expected none but was " + actual,
        actual == null || actual.isEmpty());
      return this;
    }

    /** Assert the i-th VERBOSE server filter's rendered expression. */
    public ExplainPlanAssert serverFilter(int i, String expectedExpr) {
      ExplainPlanAttributes.ExplainFilter filter = serverFilterAt(i);
      assertEquals(at("serverFilters[" + i + "].expr"), expectedExpr, filter.getExpr());
      return this;
    }

    /** Assert the i-th VERBOSE server filter's origin attribution, in order. */
    public ExplainPlanAssert serverFilterOrigin(int i, String... expectedOrigin) {
      ExplainPlanAttributes.ExplainFilter filter = serverFilterAt(i);
      List<String> actual =
        filter.getOrigin() == null ? Collections.<String> emptyList() : filter.getOrigin();
      assertEquals(at("serverFilters[" + i + "].origin"), Arrays.asList(expectedOrigin), actual);
      return this;
    }

    /** Assert the i-th VERBOSE server filter's path-test sub-tag (or {@code null}). */
    public ExplainPlanAssert serverFilterPathTest(int i, String expectedSubtag) {
      ExplainPlanAttributes.ExplainFilter filter = serverFilterAt(i);
      assertEquals(at("serverFilters[" + i + "].pathTestSubtag"), expectedSubtag,
        filter.getPathTestSubtag());
      return this;
    }

    private ExplainPlanAttributes.ExplainFilter serverFilterAt(int i) {
      List<ExplainPlanAttributes.ExplainFilter> filters = attributes.getServerFilters();
      assertNotNull(at("serverFilters") + " must not be null", filters);
      assertTrue(at("serverFilters") + " has no index " + i + " (size=" + filters.size() + ")",
        i >= 0 && i < filters.size());
      return filters.get(i);
    }

    /** Assert the number of VERBOSE client filter predicates. */
    public ExplainPlanAssert clientFilterCount(int expected) {
      List<ExplainPlanAttributes.ExplainFilter> actual = attributes.getClientFilters();
      int actualCount = actual == null ? 0 : actual.size();
      assertEquals(at("clientFilters.size"), expected, actualCount);
      return this;
    }

    /** Assert that no VERBOSE client filter breakdown was emitted (null or empty). */
    public ExplainPlanAssert clientFiltersNone() {
      List<ExplainPlanAttributes.ExplainFilter> actual = attributes.getClientFilters();
      assertTrue(at("clientFilters") + " expected none but was " + actual,
        actual == null || actual.isEmpty());
      return this;
    }

    /** Assert the i-th VERBOSE client filter's rendered expression. */
    public ExplainPlanAssert clientFilter(int i, String expectedExpr) {
      ExplainPlanAttributes.ExplainFilter filter = clientFilterAt(i);
      assertEquals(at("clientFilters[" + i + "].expr"), expectedExpr, filter.getExpr());
      return this;
    }

    /** Assert the i-th VERBOSE client filter's origin attribution, in order. */
    public ExplainPlanAssert clientFilterOrigin(int i, String... expectedOrigin) {
      ExplainPlanAttributes.ExplainFilter filter = clientFilterAt(i);
      List<String> actual =
        filter.getOrigin() == null ? Collections.<String> emptyList() : filter.getOrigin();
      assertEquals(at("clientFilters[" + i + "].origin"), Arrays.asList(expectedOrigin), actual);
      return this;
    }

    /** Assert the i-th VERBOSE client filter's path-test sub-tag (or {@code null}). */
    public ExplainPlanAssert clientFilterPathTest(int i, String expectedSubtag) {
      ExplainPlanAttributes.ExplainFilter filter = clientFilterAt(i);
      assertEquals(at("clientFilters[" + i + "].pathTestSubtag"), expectedSubtag,
        filter.getPathTestSubtag());
      return this;
    }

    private ExplainPlanAttributes.ExplainFilter clientFilterAt(int i) {
      List<ExplainPlanAttributes.ExplainFilter> filters = attributes.getClientFilters();
      assertNotNull(at("clientFilters") + " must not be null", filters);
      assertTrue(at("clientFilters") + " has no index " + i + " (size=" + filters.size() + ")",
        i >= 0 && i < filters.size());
      return filters.get(i);
    }

    /** Assert the entire VERBOSE ignored-hint map matches {@code expected}. */
    public ExplainPlanAssert ignoredHints(Map<String, String> expected) {
      assertEquals(at("ignoredHints"), expected, attributes.getIgnoredHints());
      return this;
    }

    /** Assert the ignored-hint map carries {@code hint} mapped to {@code reason}. */
    public ExplainPlanAssert ignoredHint(String hint, String reason) {
      Map<String, String> actual = attributes.getIgnoredHints();
      assertNotNull(at("ignoredHints") + " must not be null", actual);
      assertEquals(at("ignoredHints[" + hint + "]"), reason, actual.get(hint));
      return this;
    }

    /** Assert that the ignored-hint map contains an entry for {@code hint}. */
    public ExplainPlanAssert hasIgnoredHint(String hint) {
      Map<String, String> actual = attributes.getIgnoredHints();
      assertTrue(at("ignoredHints") + " expected to contain '" + hint + "' but was " + actual,
        actual != null && actual.containsKey(hint));
      return this;
    }

    /** Assert that no ignored-hint disclosure was emitted (null or empty). */
    public ExplainPlanAssert ignoredHintsNone() {
      Map<String, String> actual = attributes.getIgnoredHints();
      assertTrue(at("ignoredHints") + " expected none but was " + actual,
        actual == null || actual.isEmpty());
      return this;
    }

    public ExplainPlanAssert serverFirstKeyOnlyProjection(boolean expected) {
      assertEquals(at("serverFirstKeyOnlyProjection"), expected,
        attributes.isServerFirstKeyOnlyProjection());
      return this;
    }

    /**
     * Assert when {@code firstKeyOnly} is true the scan carries the {@code FIRST KEY ONLY}
     * projection, otherwise it carries the {@code EMPTY COLUMN ONLY} projection. Convenience for
     * callers that pick the expected kind from a flag.
     */
    public ExplainPlanAssert serverProjectionFilter(boolean firstKeyOnly) {
      return firstKeyOnly
        ? serverFirstKeyOnlyProjection(true)
        : serverEmptyColumnOnlyProjection(true);
    }

    /**
     * Assert either the {@code FIRST KEY ONLY} or the {@code EMPTY COLUMN ONLY} projection
     * optimization is present.
     */
    public ExplainPlanAssert serverProjectionFilterAnyOf() {
      assertTrue(
        at("serverFirstKeyOnlyProjection|serverEmptyColumnOnlyProjection")
          + " expected one to be true",
        attributes.isServerFirstKeyOnlyProjection()
          || attributes.isServerEmptyColumnOnlyProjection());
      return this;
    }

    public ExplainPlanAssert serverEmptyColumnOnlyProjection(boolean expected) {
      assertEquals(at("serverEmptyColumnOnlyProjection"), expected,
        attributes.isServerEmptyColumnOnlyProjection());
      return this;
    }

    public ExplainPlanAssert useRoundRobinIterator(boolean expected) {
      assertEquals(at("useRoundRobinIterator"), expected, attributes.isUseRoundRobinIterator());
      return this;
    }

    public ExplainPlanAssert scanEstimatedRows(Long expected) {
      assertEquals(at("scanEstimatedRows"), expected, attributes.getScanEstimatedRows());
      return this;
    }

    public ExplainPlanAssert scanEstimatedBytes(Long expected) {
      assertEquals(at("scanEstimatedSizeInBytes"), expected,
        attributes.getScanEstimatedSizeInBytes());
      return this;
    }

    public ExplainPlanAssert estimatedRows(Long expected) {
      assertEquals(at("estimatedRows"), expected, attributes.getEstimatedRows());
      return this;
    }

    public ExplainPlanAssert estimatedBytes(Long expected) {
      assertEquals(at("estimatedSizeInBytes"), expected, attributes.getEstimatedSizeInBytes());
      return this;
    }

    public ExplainPlanAssert estimateInfoTs(Long expected) {
      assertEquals(at("estimateInfoTs"), expected, attributes.getEstimateInfoTs());
      return this;
    }

    public ExplainPlanAssert splitsChunk(Integer expected) {
      assertEquals(at("splitsChunk"), expected, attributes.getSplitsChunk());
      return this;
    }

    public ExplainPlanAssert regionLocationCount(int expected) {
      int actual =
        attributes.getRegionLocations() == null ? 0 : attributes.getRegionLocations().size();
      assertEquals(at("regionLocations.size"), expected, actual);
      return this;
    }

    /** Assert that the EXPLAIN plan emitted at least one region location. */
    public ExplainPlanAssert regionLocationsNotEmpty() {
      assertNotNull(at("regionLocations") + " must not be null", attributes.getRegionLocations());
      assertTrue(at("regionLocations") + " must not be empty",
        !attributes.getRegionLocations().isEmpty());
      return this;
    }

    /**
     * Assert the total distinct region locations seen before any trimming applied to
     * {@code regionLocations}. When the trim limit was not reached, this equals
     * {@link #regionLocationCount(int)}.
     */
    public ExplainPlanAssert regionLocationsTotalSize(Integer expected) {
      assertEquals(at("regionLocationsTotalSize"), expected,
        attributes.getRegionLocationsTotalSize());
      return this;
    }

    /** Assert the number of region location lookups performed by the planner. */
    public ExplainPlanAssert numRegionLocationLookups(int expected) {
      assertEquals(at("numRegionLocationLookups"), expected,
        attributes.getNumRegionLocationLookups());
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

    /** Navigate to the left-hand side plan (sort-merge join). */
    public ExplainPlanAssert lhs() {
      ExplainPlanAttributes lhs = attributes.getLhsJoinQueryExplainPlan();
      assertNotNull(at("lhsJoinQueryExplainPlan") + " must not be null", lhs);
      return new ExplainPlanAssert(lhs, this, context + ".lhs");
    }

    /** Navigate to the right-hand side plan (sort-merge join / union all). */
    public ExplainPlanAssert rhs() {
      ExplainPlanAttributes rhs = attributes.getRhsJoinQueryExplainPlan();
      assertNotNull(at("rhsJoinQueryExplainPlan") + " must not be null", rhs);
      return new ExplainPlanAssert(rhs, this, context + ".rhs");
    }

    /** Assert the number of ordered client-side pipeline steps on this node. */
    public ExplainPlanAssert clientStepCount(int expected) {
      List<String> steps = attributes.getClientSteps();
      int actual = steps == null ? 0 : steps.size();
      assertEquals(at("clientSteps.size"), expected, actual);
      return this;
    }

    /** Assert the i-th ordered client-side pipeline step on this node. */
    public ExplainPlanAssert clientStep(int i, String expected) {
      List<String> steps = attributes.getClientSteps();
      assertNotNull(at("clientSteps") + " must not be null", steps);
      assertTrue(at("clientSteps") + " has no index " + i + " (size=" + steps.size() + ")",
        i >= 0 && i < steps.size());
      assertEquals(at("clientSteps[" + i + "]"), expected, steps.get(i));
      return this;
    }

    /** Assert the entire ordered client-side pipeline on this node matches {@code expected}. */
    public ExplainPlanAssert clientSteps(String... expected) {
      List<String> actual = attributes.getClientSteps();
      List<String> actualOrEmpty = actual == null ? Collections.<String> emptyList() : actual;
      assertEquals(at("clientSteps"), Arrays.asList(expected), actualOrEmpty);
      return this;
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

    /** Assert the tenant id disclosed at the top of the plan. */
    public ExplainPlanAssert tenant(String expected) {
      assertEquals(at("tenantId"), expected, attributes.getTenantId());
      return this;
    }

    /** Assert that no tenant id disclosure was emitted. */
    public ExplainPlanAssert tenantNone() {
      assertNull(at("tenantId") + " expected none but was " + attributes.getTenantId(),
        attributes.getTenantId());
      return this;
    }

    /** Assert the view name and its base (physical) table name disclosed at the top of the plan. */
    public ExplainPlanAssert view(String expectedName, String expectedBaseName) {
      assertEquals(at("viewName"), expectedName, attributes.getViewName());
      assertEquals(at("viewBaseName"), expectedBaseName, attributes.getViewBaseName());
      return this;
    }

    /** Assert the view name disclosed at the top of the plan. */
    public ExplainPlanAssert viewName(String expected) {
      assertEquals(at("viewName"), expected, attributes.getViewName());
      return this;
    }

    /** Assert that no view disclosure was emitted. */
    public ExplainPlanAssert viewNone() {
      assertNull(at("viewName") + " expected none but was " + attributes.getViewName(),
        attributes.getViewName());
      return this;
    }

    /** Assert the CDC change scopes disclosed at the top of the plan. */
    public ExplainPlanAssert cdcScopes(String expected) {
      assertEquals(at("cdcScopes"), expected, attributes.getCdcScopes());
      return this;
    }

    /** Assert that no CDC scope disclosure was emitted. */
    public ExplainPlanAssert cdcScopesNone() {
      assertNull(at("cdcScopes") + " expected none but was " + attributes.getCdcScopes(),
        attributes.getCdcScopes());
      return this;
    }

    /** Assert the transaction provider disclosed at the top of the plan. */
    public ExplainPlanAssert txnProvider(String expected) {
      assertEquals(at("txnProvider"), expected, attributes.getTxnProvider());
      return this;
    }

    /** Assert that no transaction provider disclosure was emitted. */
    public ExplainPlanAssert txnProviderNone() {
      assertNull(at("txnProvider") + " expected none but was " + attributes.getTxnProvider(),
        attributes.getTxnProvider());
      return this;
    }

    /** Assert the number of distinct rewrite breadcrumbs disclosed at the top of the plan. */
    public ExplainPlanAssert rewriteCount(int expected) {
      List<String> rewrites = attributes.getRewrites();
      int actual = rewrites == null ? 0 : rewrites.size();
      assertEquals(at("rewrites.size"), expected, actual);
      return this;
    }

    /** Assert the i-th rewrite breadcrumb. */
    public ExplainPlanAssert rewrite(int i, String expected) {
      List<String> rewrites = attributes.getRewrites();
      assertNotNull(at("rewrites") + " must not be null", rewrites);
      assertTrue(at("rewrites") + " has no index " + i + " (size=" + rewrites.size() + ")",
        i >= 0 && i < rewrites.size());
      assertEquals(at("rewrites[" + i + "]"), expected, rewrites.get(i));
      return this;
    }

    /** Assert the entire ordered rewrite breadcrumb list matches {@code expected}. */
    public ExplainPlanAssert rewrites(String... expected) {
      List<String> actual = attributes.getRewrites();
      List<String> actualOrEmpty = actual == null ? Collections.<String> emptyList() : actual;
      assertEquals(at("rewrites"), Arrays.asList(expected), actualOrEmpty);
      return this;
    }

    /** Assert the rewrite breadcrumb list contains {@code expected}. */
    public ExplainPlanAssert rewriteContains(String expected) {
      List<String> rewrites = attributes.getRewrites();
      assertNotNull(at("rewrites") + " must not be null", rewrites);
      assertTrue(at("rewrites") + " expected to contain '" + expected + "' but was " + rewrites,
        rewrites.contains(expected));
      return this;
    }

    /** Assert that no rewrite breadcrumbs were disclosed (null or empty). */
    public ExplainPlanAssert rewritesNone() {
      List<String> rewrites = attributes.getRewrites();
      assertTrue(at("rewrites") + " expected none but was " + rewrites,
        rewrites == null || rewrites.isEmpty());
      return this;
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
