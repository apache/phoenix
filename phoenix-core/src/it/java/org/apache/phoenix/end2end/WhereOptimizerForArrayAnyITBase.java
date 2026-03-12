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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;

/**
 * Base class for WhereOptimizerForArrayAnyIT tests containing shared helper methods.
 */
public abstract class WhereOptimizerForArrayAnyITBase extends BaseTest {

  @BeforeClass
  public static void setup() throws Exception {
    setUpTestDriver(new ReadOnlyProps(new HashMap<String, String>()));
  }

  protected void assertPointLookupsAreGenerated(PreparedStatement stmt, int noOfPointLookups)
    throws SQLException {
    QueryPlan queryPlan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
    assertPointLookupsAreGenerated(queryPlan, noOfPointLookups);
  }

  protected void assertPointLookupsAreGenerated(QueryPlan queryPlan, int noOfPointLookups)
    throws SQLException {
    ExplainPlan explain = queryPlan.getExplainPlan();
    ExplainPlanAttributes planAttributes = explain.getPlanStepsAsAttributes();
    String expectedScanType =
      "POINT LOOKUP ON " + noOfPointLookups + " KEY" + (noOfPointLookups > 1 ? "S " : " ");
    assertEquals(expectedScanType, planAttributes.getExplainScanType());
  }

  protected void assertSkipScanIsGenerated(PreparedStatement stmt, int skipListSize)
    throws SQLException {
    QueryPlan queryPlan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
    ExplainPlan explain = queryPlan.getExplainPlan();
    ExplainPlanAttributes planAttributes = explain.getPlanStepsAsAttributes();
    String expectedScanType =
      "SKIP SCAN ON " + skipListSize + " KEY" + (skipListSize > 1 ? "S " : " ");
    assertEquals(expectedScanType, planAttributes.getExplainScanType());
  }

  protected void assertRangeScanIsGenerated(PreparedStatement stmt) throws SQLException {
    QueryPlan queryPlan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
    ExplainPlan explain = queryPlan.getExplainPlan();
    ExplainPlanAttributes planAttributes = explain.getPlanStepsAsAttributes();
    String expectedScanType = "RANGE SCAN ";
    assertEquals(expectedScanType, planAttributes.getExplainScanType());
  }

  protected void assertDegenerateScanIsGenerated(PreparedStatement stmt) throws SQLException {
    QueryPlan queryPlan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
    ExplainPlan explain = queryPlan.getExplainPlan();
    ExplainPlanAttributes planAttributes = explain.getPlanStepsAsAttributes();
    assertNull(planAttributes.getExplainScanType());
  }
}
