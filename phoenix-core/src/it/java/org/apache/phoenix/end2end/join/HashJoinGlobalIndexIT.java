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
import org.apache.phoenix.optimize.OptimizerReasons;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(ParallelStatsDisabledTest.class)
public class HashJoinGlobalIndexIT extends HashJoinIT {

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

  public HashJoinGlobalIndexIT(String[] indexDDL) {
    super(indexDDL);
  }

  @Override
  protected void assertLeftJoinWithAggPlan1(Connection conn, String query) throws Exception {
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    assertPlan(conn, query).scanType("FULL SCAN").table(order)
      .serverAggregate("SERVER AGGREGATE INTO DISTINCT ROWS BY [\"I.0:NAME\"]")
      .clientSortAlgo("CLIENT MERGE SORT").subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true).end();
  }

  @Override
  protected void assertLeftJoinWithAggPlan2(Connection conn, String query) throws Exception {
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    assertPlan(conn, query).scanType("FULL SCAN").table(order)
      .serverAggregate("SERVER AGGREGATE INTO DISTINCT ROWS BY [\"I.:item_id\"]")
      .clientSortAlgo("CLIENT MERGE SORT").clientSortedBy("[SUM(O.QUANTITY) DESC]").subPlanCount(1)
      .subPlan(0).abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true).end();
  }

  @Override
  protected void assertLeftJoinWithAggPlan3(Connection conn, String query) throws Exception {
    String item = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("FULL SCAN").table(item).serverFirstKeyOnlyProjection(true)
      .serverAggregate("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.item_id\"]")
      .clientSortedBy("[SUM(O.QUANTITY) DESC NULLS LAST, \"I.item_id\"]").subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(order).end();
  }

  @Override
  protected void assertRightJoinWithAggPlan1(Connection conn, String query) throws Exception {
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true)
      .serverAggregate("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.0:NAME\"]")
      .subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD LEFT */")
      .scanType("FULL SCAN").table(order).end();
  }

  @Override
  protected void assertRightJoinWithAggPlan2(Connection conn, String query) throws Exception {
    String item = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("FULL SCAN").table(item).serverFirstKeyOnlyProjection(true)
      .serverAggregate("SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.item_id\"]")
      .clientSortedBy("[SUM(O.QUANTITY) DESC NULLS LAST, \"I.item_id\"]").subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD LEFT */")
      .scanType("FULL SCAN").table(order).end();
  }

  @Override
  protected void assertJoinWithWildcardPlan(Connection conn, String query) throws Exception {
    String item = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
    String supplier = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("FULL SCAN").table(item).subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(supplier).end();
  }

  @Override
  protected void assertJoinPlanWithIndexPlan1(Connection conn, String query) throws Exception {
    String idxItem = getSchemaName() + ".idx_item";
    String idxSupplier = getSchemaName() + ".idx_supplier";
    assertPlan(conn, query).scanType("RANGE SCAN").table(idxItem).keyRanges("['T1'] - ['T5']")
      .serverFirstKeyOnlyProjection(true).subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("RANGE SCAN").table(idxSupplier).keyRanges("['S1'] - ['S5']")
      .serverFirstKeyOnlyProjection(true).end();
  }

  @Override
  protected void assertJoinPlanWithIndexPlan2(Connection conn, String query) throws Exception {
    String idxItem = getSchemaName() + ".idx_item";
    String idxSupplier = getSchemaName() + ".idx_supplier";
    assertPlan(conn, query).scanType("SKIP SCAN ON 2 KEYS").table(idxItem)
      .keyRanges("['T1'] - ['T5']").subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("SKIP SCAN ON 2 KEYS").table(idxSupplier).keyRanges("['S1'] - ['S5']")
      .serverFirstKeyOnlyProjection(true).end();
  }

  @Override
  protected void assertSkipMergeOptimizationPlan(Connection conn, String query) throws Exception {
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String idxSupplier = getSchemaName() + ".idx_supplier";
    assertPlan(conn, query).scanType("FULL SCAN").table(idxItem).subPlanCount(2).subPlan(0)
      .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT, SKIP MERGE */")
      .scanType("FULL SCAN").table(order).serverWhereFilter("SERVER FILTER BY QUANTITY < 5000")
      .end().subPlan(1).abstractExplainPlan("PARALLEL INNER-JOIN TABLE 1  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxSupplier).serverFirstKeyOnlyProjection(true).end();
  }

  @Override
  protected void assertSelfJoinPlan1(Connection conn, String query) throws Exception {
    String item = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    assertPlan(conn, query).scanType("FULL SCAN").table(item)
      .dynamicServerFilter("DYNAMIC SERVER FILTER BY \"I1.item_id\" IN (\"I2.:item_id\")")
      .subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true).end();
  }

  @Override
  protected void assertSelfJoinPlan2(Connection conn, String query) throws Exception {
    String idxItem = getSchemaName() + ".idx_item";
    assertPlan(conn, query).scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true)
      .serverSortedBy("[\"I1.0:NAME\", \"I2.0:NAME\"]").clientSortAlgo("CLIENT MERGE SORT")
      .subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).end();
  }

  @Override
  protected void assertStarJoinPlan(Connection conn, String query, boolean noStarJoin)
    throws Exception {
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String idxCustomer = getSchemaName() + ".idx_customer";
    String idxItem = getSchemaName() + ".idx_item";
    if (!noStarJoin) {
      assertPlan(conn, query).scanType("FULL SCAN").table(order).subPlanCount(2).subPlan(0)
        .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
        .scanType("FULL SCAN").table(idxCustomer).serverFirstKeyOnlyProjection(true).end()
        .subPlan(1).abstractExplainPlan("PARALLEL INNER-JOIN TABLE 1  /* HASH BUILD RIGHT */")
        .scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true).end();
    } else {
      assertPlan(conn, query).scanType("FULL SCAN").table(idxItem)
        .serverFirstKeyOnlyProjection(true).serverSortedBy("[\"O.order_id\"]")
        .clientSortAlgo("CLIENT MERGE SORT").subPlanCount(1).subPlan(0)
        .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD LEFT */")
        .scanType("FULL SCAN").table(order).subPlanCount(1).subPlan(0)
        .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
        .scanType("FULL SCAN").table(idxCustomer).serverFirstKeyOnlyProjection(true).end().end();
    }
  }

  @Override
  protected void assertSubJoinPlan(Connection conn, String query) throws Exception {
    String customer = getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME);
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    String supplier = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("RANGE SCAN").table(customer).keyRanges("[*] - ['0000000005']")
      .serverSortedBy("[\"C.customer_id\", \"I.0:NAME\"]").clientSortAlgo("CLIENT MERGE SORT")
      .dynamicServerFilter("DYNAMIC SERVER FILTER BY \"C.customer_id\" IN (\"O.customer_id\")")
      .subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(order)
      .serverWhereFilter("SERVER FILTER BY \"order_id\" != '000000000000003'").subPlanCount(1)
      .subPlan(0).abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).serverWhereFilter("SERVER FILTER BY \"NAME\" != 'T3'")
      .subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD LEFT */")
      .scanType("FULL SCAN").table(supplier).end().end().end();
  }

  @Override
  protected void assertSubqueryAggPlan1(Connection conn, String query) throws Exception {
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    assertPlan(conn, query).scanType("FULL SCAN").table(order)
      .serverAggregate("SERVER AGGREGATE INTO DISTINCT ROWS BY [I.NAME]")
      .clientSortAlgo("CLIENT MERGE SORT").subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true)
      .indexRule(OptimizerReasons.RULE_NON_LOCAL_PREFERRED).indexRejectedNone().end();
  }

  @Override
  protected void assertSubqueryAggPlan2(Connection conn, String query) throws Exception {
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    assertPlan(conn, query).scanType("FULL SCAN").table(order)
      .serverAggregate("SERVER AGGREGATE INTO DISTINCT ROWS BY [O.IID]")
      .clientSortAlgo("CLIENT MERGE SORT").clientSortedBy("[SUM(O.QUANTITY) DESC]").subPlanCount(1)
      .subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT, SKIP MERGE */")
      .scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true)
      .indexRule(OptimizerReasons.RULE_NON_LOCAL_PREFERRED).indexRejectedNone().end();
  }

  @Override
  protected void assertSubqueryAggPlan3(Connection conn, String query) throws Exception {
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true)
      .serverSortedBy("[O.Q DESC NULLS LAST, I.IID]").clientSortAlgo("CLIENT MERGE SORT")
      .subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(order)
      .serverAggregate("SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]")
      .clientSortAlgo("CLIENT MERGE SORT").indexRule(OptimizerReasons.RULE_DATA_TABLE)
      .indexRejectedNone().end();
  }

  @Override
  protected void assertSubqueryAggPlan4(Connection conn, String query) throws Exception {
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true)
      .serverSortedBy("[O.Q DESC, I.IID]").clientSortAlgo("CLIENT MERGE SORT").subPlanCount(1)
      .subPlan(0).abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD LEFT */")
      .scanType("FULL SCAN").table(order)
      .serverAggregate("SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]")
      .clientSortAlgo("CLIENT MERGE SORT").indexRule(OptimizerReasons.RULE_DATA_TABLE)
      .indexRejectedNone().end();
  }

  @Override
  protected void assertNestedSubqueriesPlan(Connection conn, String query) throws Exception {
    String customer = getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME);
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    String supplier = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("RANGE SCAN").table(customer).keyRanges("[*] - ['0000000005']")
      .serverSortedBy("[C.CID, QO.INAME]").clientSortAlgo("CLIENT MERGE SORT").subPlanCount(1)
      .subPlan(0).abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(order)
      .serverWhereFilter("SERVER FILTER BY \"order_id\" != '000000000000003'").subPlanCount(1)
      .subPlan(0).abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).serverWhereFilter("SERVER FILTER BY \"NAME\" != 'T3'")
      .subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD LEFT */")
      .scanType("FULL SCAN").table(supplier).indexRule(OptimizerReasons.RULE_ONLY_CANDIDATE)
      .indexRejectedCount(1)
      .indexRejected(0, "idx_supplier", OptimizerReasons.REASON_DOES_NOT_COVER_PROJECTION).end()
      .end().end();
  }

  @Override
  protected void assertJoinWithLimitPlan1(Connection conn, String query) throws Exception {
    String supplier = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).iteratorType("SERIAL").scanType("FULL SCAN").table(supplier)
      .serverRowLimit(4L).clientRowLimit(4).joinScannerLimit(4L).subPlanCount(2).subPlan(0)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).end().subPlan(1)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 1  /* HASH BUILD RIGHT, DELAYED EVALUATION */")
      .scanType("FULL SCAN").table(order).end();
  }

  @Override
  protected void assertJoinWithLimitPlan2(Connection conn, String query) throws Exception {
    String supplier = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).scanType("FULL SCAN").table(supplier).clientRowLimit(4)
      .dynamicServerFilter("DYNAMIC SERVER FILTER BY \"S.supplier_id\" IN (\"I.0:supplier_id\")")
      .joinScannerLimit(4L).subPlanCount(2).subPlan(0)
      .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).end().subPlan(1)
      .abstractExplainPlan(
        "PARALLEL INNER-JOIN TABLE 1  /* HASH BUILD RIGHT, DELAYED EVALUATION */")
      .scanType("FULL SCAN").table(order).end();
  }

  @Override
  protected void assertSetMaxRowsPlan(Connection conn, String query) throws Exception {
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    PhoenixPreparedStatement statement =
      conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
    statement.setMaxRows(4);
    ExplainPlanAttributes attributes =
      statement.optimizeQuery().getExplainPlan().getPlanStepsAsAttributes();
    assertPlan(attributes).scanType("FULL SCAN").table(idxItem).serverFirstKeyOnlyProjection(true)
      .clientRowLimit(4).joinScannerLimit(4L).subPlanCount(1).subPlan(0)
      .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(order).end();
  }

  @Override
  protected void assertJoinWithOffsetPlan1(Connection conn, String query) throws Exception {
    String supplier = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).iteratorType("SERIAL").scanType("FULL SCAN").table(supplier)
      .serverOffset(2).serverRowLimit(3L).clientRowLimit(1).joinScannerLimit(3L).subPlanCount(2)
      .subPlan(0).abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).end().subPlan(1)
      .abstractExplainPlan("PARALLEL LEFT-JOIN TABLE 1  /* HASH BUILD RIGHT, DELAYED EVALUATION */")
      .scanType("FULL SCAN").table(order).end();
  }

  @Override
  protected void assertJoinWithOffsetPlan2(Connection conn, String query) throws Exception {
    String supplier = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
    String idxItem = getSchemaName() + ".idx_item";
    String order = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
    assertPlan(conn, query).iteratorType("SERIAL").scanType("FULL SCAN").table(supplier)
      .serverOffset(2).clientRowLimit(1)
      .dynamicServerFilter("DYNAMIC SERVER FILTER BY \"S.supplier_id\" IN (\"I.0:supplier_id\")")
      .joinScannerLimit(3L).subPlanCount(2).subPlan(0)
      .abstractExplainPlan("PARALLEL INNER-JOIN TABLE 0  /* HASH BUILD RIGHT */")
      .scanType("FULL SCAN").table(idxItem).end().subPlan(1)
      .abstractExplainPlan(
        "PARALLEL INNER-JOIN TABLE 1  /* HASH BUILD RIGHT, DELAYED EVALUATION */")
      .scanType("FULL SCAN").table(order).end();
  }

  @Parameters(name = "HashJoinGlobalIndexIT_{index}") // name is used by failsafe as file name in
                                                      // reports
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
