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
package org.apache.phoenix.end2end.join;

import static org.apache.phoenix.query.explain.ExplainPlanTestUtil.assertPlan;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(ParallelStatsDisabledTest.class)
public class SortMergeJoinGlobalIndexIT extends SortMergeJoinIT {

  private static final Map<String, String> virtualNameToRealNameMap = Maps.newHashMap();
  private static final String schemaName = "S_" + generateUniqueName();

  @Override
  protected String getSchemaName() {
    // run all tests in a single schema
    return schemaName;
  }

  @Override
  protected Map<String, String> getTableNameMap() {
    // cache across tests, so that tables and
    // indexes are not recreated each time
    return virtualNameToRealNameMap;
  }

  public SortMergeJoinGlobalIndexIT(String[] indexDDL) {
    super(indexDDL);
  }

  @Override
  protected void assertSkipMergeOptimizationPlan(Connection conn, String query) throws Exception {
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String itemIndex = getSchemaName() + ".idx_item";
    String supplierIndex = getSchemaName() + ".idx_supplier";
    assertPlan(conn, query).abstractExplainPlan("SORT-MERGE-JOIN (LEFT)").sortMergeSkipMerge(false)
      .lhs().scanType("FULL SCAN").table(supplierIndex).serverFirstKeyOnlyProjection(true)
      .serverSortedBy("[\"S.:supplier_id\"]").clientSortAlgo("CLIENT MERGE SORT").end().rhs()
      .abstractExplainPlan("SORT-MERGE-JOIN (INNER)").sortMergeSkipMerge(true)
      .clientSortedBy("[\"I.0:supplier_id\"]").lhs().scanType("FULL SCAN").table(itemIndex)
      .serverSortedBy("[\"I.:item_id\"]").clientSortAlgo("CLIENT MERGE SORT").end().rhs()
      .scanType("FULL SCAN").table(order).serverWhereFilter("SERVER FILTER BY QUANTITY < 5000")
      .serverSortedBy("[\"O.item_id\"]").clientSortAlgo("CLIENT MERGE SORT").end().end();
  }

  @Override
  protected void assertSelfJoinPlan(Connection conn, String query) throws Exception {
    String itemIndex = getSchemaName() + ".idx_item";
    assertPlan(conn, query).abstractExplainPlan("SORT-MERGE-JOIN (INNER)").sortMergeSkipMerge(false)
      .lhs().scanType("FULL SCAN").table(itemIndex).serverFirstKeyOnlyProjection(true)
      .serverSortedBy("[\"I1.:item_id\"]").clientSortAlgo("CLIENT MERGE SORT").end().rhs()
      .scanType("FULL SCAN").table(itemIndex).serverFirstKeyOnlyProjection(true)
      .serverSortedBy("[\"I2.:item_id\"]").clientSortAlgo("CLIENT MERGE SORT").end();
  }

  @Override
  protected void assertSetMaxRowsPlan(Connection conn, String query, int queryIndex)
    throws Exception {
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String itemIndex = getSchemaName() + ".idx_item";
    PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
    statement.setMaxRows(4);
    ExplainPlanAttributes attributes =
      statement.optimizeQuery().getExplainPlan().getPlanStepsAsAttributes();
    assertPlan(attributes).abstractExplainPlan("SORT-MERGE-JOIN (INNER)").sortMergeSkipMerge(false)
      .clientRowLimit(4).lhs().scanType("FULL SCAN").table(itemIndex)
      .serverFirstKeyOnlyProjection(true).serverSortedBy("[\"I.:item_id\"]")
      .clientSortAlgo("CLIENT MERGE SORT").end().rhs().scanType("FULL SCAN").table(order)
      .serverSortedBy(queryIndex == 0 ? "[\"O.item_id\"]" : "[\"item_id\"]")
      .clientSortAlgo("CLIENT MERGE SORT").end();
  }

  // name is used by failsafe as file name in reports
  @Parameters(name = "SortMergeJoinGlobalIndexIT_{index}")
  public static synchronized Collection<Object> data() {
    List<Object> testCases = Lists.newArrayList();
    testCases.add(new String[][] {
      { "CREATE INDEX \"idx_customer\" ON " + JOIN_CUSTOMER_TABLE_FULL_NAME + " (name)",
        "CREATE INDEX \"idx_item\" ON " + JOIN_ITEM_TABLE_FULL_NAME
          + " (name) INCLUDE (price, discount1, discount2, \"supplier_id\", description)",
        "CREATE INDEX \"idx_supplier\" ON " + JOIN_SUPPLIER_TABLE_FULL_NAME + " (name)" } });
    return testCases;
  }
}
