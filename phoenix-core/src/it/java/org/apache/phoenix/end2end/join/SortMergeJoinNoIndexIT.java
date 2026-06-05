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
public class SortMergeJoinNoIndexIT extends SortMergeJoinIT {

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

  public SortMergeJoinNoIndexIT(String[] indexDDL) {
    super(indexDDL);
  }

  @Override
  protected void assertSkipMergeOptimizationPlan(Connection conn, String query) throws Exception {
    String item = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
    String supplier = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).abstractExplainPlan("SORT-MERGE-JOIN (LEFT)").scanType("FULL SCAN")
      .table(supplier).sortMergeSkipMerge(false).rhs()
      .abstractExplainPlan("SORT-MERGE-JOIN (INNER)").scanType("FULL SCAN").table(item)
      .sortMergeSkipMerge(true).clientSortedBy("[\"I.supplier_id\"]").rhs().scanType("FULL SCAN")
      .table(order).serverWhereFilter("SERVER FILTER BY QUANTITY < 5000")
      .serverSortedBy("[\"O.item_id\"]").clientSortAlgo("CLIENT MERGE SORT").end().end();
  }

  @Override
  protected void assertSelfJoinPlan(Connection conn, String query) throws Exception {
    String item = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
    assertPlan(conn, query).abstractExplainPlan("SORT-MERGE-JOIN (INNER)").scanType("FULL SCAN")
      .table(item).sortMergeSkipMerge(false).rhs().scanType("FULL SCAN").table(item)
      .serverWhereFilter("SERVER FILTER BY FIRST KEY ONLY").end();
  }

  @Override
  protected void assertSetMaxRowsPlan(Connection conn, String query, int queryIndex)
    throws Exception {
    String item = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
    statement.setMaxRows(4);
    ExplainPlanAttributes attributes =
      statement.optimizeQuery().getExplainPlan().getPlanStepsAsAttributes();
    assertPlan(attributes).abstractExplainPlan("SORT-MERGE-JOIN (INNER)").scanType("FULL SCAN")
      .table(item).sortMergeSkipMerge(false).clientRowLimit(4).rhs().scanType("FULL SCAN")
      .table(order).serverSortedBy(queryIndex == 0 ? "[\"O.item_id\"]" : "[\"item_id\"]")
      .clientSortAlgo("CLIENT MERGE SORT").end();
  }

  @Parameters(name = "SortMergeJoinNoIndexIT_{index}") // name is used by failsafe as file name in
                                                       // reports
  public static synchronized Collection<Object> data() {
    List<Object> testCases = Lists.newArrayList();
    testCases.add(new String[][] { {} });
    return testCases;
  }
}
