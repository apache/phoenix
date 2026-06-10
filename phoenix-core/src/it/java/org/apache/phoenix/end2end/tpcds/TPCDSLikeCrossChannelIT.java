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
package org.apache.phoenix.end2end.tpcds;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * TPC-DS derived integration tests, exercising unions and sort-merge joins.
 * <p>
 * See {@link TPCDSLikeFixture} for the schema and the catalog of queries intentionally not ported,
 * and {@link TPCDSLikeSingleChannelIT} for the single channel queries.
 */
@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class TPCDSLikeCrossChannelIT extends TPCDSLikeBaseIT {

  public TPCDSLikeCrossChannelIT(String label, String schema, boolean noIndex) {
    super(label, schema, noIndex);
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return indexParameters();
  }

  @BeforeClass
  public static synchronized void setup() throws SQLException {
    loadFixture();
  }

  // TPC-DS Q56: total ext sales price by item across store/catalog/web for a color set and region.
  static final String Q56 = "SELECT i_item_id, SUM(total_sales) agg_total_sales FROM ("
    + " SELECT i.i_item_id i_item_id, SUM(ss.ss_ext_sales_price) total_sales"
    + " FROM TPCDS.store_sales ss, TPCDS.date_dim d, TPCDS.customer_address ca, TPCDS.item i"
    + " WHERE ss.ss_sold_date_sk = d.d_date_sk AND ss.ss_addr_sk = ca.ca_address_sk"
    + " AND ss.ss_item_sk = i.i_item_sk AND i.i_color IN ('red', 'blue', 'green')"
    + " AND d.d_year = 2000 AND ca.ca_gmt_offset = -5 GROUP BY i.i_item_id" + " UNION ALL"
    + " SELECT i.i_item_id i_item_id, SUM(cs.cs_ext_sales_price) total_sales"
    + " FROM TPCDS.catalog_sales cs, TPCDS.date_dim d, TPCDS.customer_address ca, TPCDS.item i"
    + " WHERE cs.cs_sold_date_sk = d.d_date_sk AND cs.cs_ship_addr_sk = ca.ca_address_sk"
    + " AND cs.cs_item_sk = i.i_item_sk AND i.i_color IN ('red', 'blue', 'green')"
    + " AND d.d_year = 2000 AND ca.ca_gmt_offset = -5 GROUP BY i.i_item_id" + " UNION ALL"
    + " SELECT i.i_item_id i_item_id, SUM(ws.ws_ext_sales_price) total_sales"
    + " FROM TPCDS.web_sales ws, TPCDS.date_dim d, TPCDS.customer_address ca, TPCDS.item i"
    + " WHERE ws.ws_sold_date_sk = d.d_date_sk AND ws.ws_ship_addr_sk = ca.ca_address_sk"
    + " AND ws.ws_item_sk = i.i_item_sk AND i.i_color IN ('red', 'blue', 'green')"
    + " AND d.d_year = 2000 AND ca.ca_gmt_offset = -5 GROUP BY i.i_item_id"
    + " ) tmp GROUP BY i_item_id ORDER BY agg_total_sales, i_item_id LIMIT 100";

  // TPC-DS Q60: cross-channel total ext sales price by item for a category set and region.
  static final String Q60 = "SELECT i_item_id, SUM(total_sales) agg_total_sales FROM ("
    + " SELECT i.i_item_id i_item_id, SUM(ss.ss_ext_sales_price) total_sales"
    + " FROM TPCDS.store_sales ss, TPCDS.date_dim d, TPCDS.customer_address ca, TPCDS.item i"
    + " WHERE ss.ss_sold_date_sk = d.d_date_sk AND ss.ss_addr_sk = ca.ca_address_sk"
    + " AND ss.ss_item_sk = i.i_item_sk AND i.i_category IN ('Books', 'Music', 'Home')"
    + " AND d.d_year = 2001 AND ca.ca_gmt_offset = -6 GROUP BY i.i_item_id" + " UNION ALL"
    + " SELECT i.i_item_id i_item_id, SUM(cs.cs_ext_sales_price) total_sales"
    + " FROM TPCDS.catalog_sales cs, TPCDS.date_dim d, TPCDS.customer_address ca, TPCDS.item i"
    + " WHERE cs.cs_sold_date_sk = d.d_date_sk AND cs.cs_ship_addr_sk = ca.ca_address_sk"
    + " AND cs.cs_item_sk = i.i_item_sk AND i.i_category IN ('Books', 'Music', 'Home')"
    + " AND d.d_year = 2001 AND ca.ca_gmt_offset = -6 GROUP BY i.i_item_id" + " UNION ALL"
    + " SELECT i.i_item_id i_item_id, SUM(ws.ws_ext_sales_price) total_sales"
    + " FROM TPCDS.web_sales ws, TPCDS.date_dim d, TPCDS.customer_address ca, TPCDS.item i"
    + " WHERE ws.ws_sold_date_sk = d.d_date_sk AND ws.ws_ship_addr_sk = ca.ca_address_sk"
    + " AND ws.ws_item_sk = i.i_item_sk AND i.i_category IN ('Books', 'Music', 'Home')"
    + " AND d.d_year = 2001 AND ca.ca_gmt_offset = -6 GROUP BY i.i_item_id"
    + " ) tmp GROUP BY i_item_id ORDER BY agg_total_sales, i_item_id LIMIT 100";

  // TPC-DS Q83: cross-channel return quantities by item.
  static final String Q83 = "SELECT sr.item_id, sr.sr_item_qty, cr.cr_item_qty, wr.wr_item_qty"
    + " FROM (SELECT i.i_item_id item_id, SUM(srt.sr_return_quantity) sr_item_qty"
    + " FROM TPCDS.store_returns srt, TPCDS.item i, TPCDS.date_dim d"
    + " WHERE srt.sr_item_sk = i.i_item_sk AND srt.sr_returned_date_sk = d.d_date_sk"
    + " AND d.d_year IN (2000, 2001) GROUP BY i.i_item_id) sr"
    + " JOIN (SELECT i.i_item_id item_id, SUM(crt.cr_return_quantity) cr_item_qty"
    + " FROM TPCDS.catalog_returns crt, TPCDS.item i, TPCDS.date_dim d"
    + " WHERE crt.cr_item_sk = i.i_item_sk AND crt.cr_returned_date_sk = d.d_date_sk"
    + " AND d.d_year IN (2000, 2001) GROUP BY i.i_item_id) cr ON sr.item_id = cr.item_id"
    + " JOIN (SELECT i.i_item_id item_id, SUM(wrt.wr_return_quantity) wr_item_qty"
    + " FROM TPCDS.web_returns wrt, TPCDS.item i, TPCDS.date_dim d"
    + " WHERE wrt.wr_item_sk = i.i_item_sk AND wrt.wr_returned_date_sk = d.d_date_sk"
    + " AND d.d_year IN (2000, 2001) GROUP BY i.i_item_id) wr ON sr.item_id = wr.item_id"
    + " ORDER BY sr.item_id LIMIT 100";

  // TPC-DS Q97: store-only / catalog-only / both customer-item pairs via a FULL OUTER JOIN.
  // The USE_SORT_MERGE_JOIN hint forces the sort-merge strategy Phoenix requires for FULL OUTER.
  static final String Q97 = "SELECT /*+ USE_SORT_MERGE_JOIN */"
    + " SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL"
    + " THEN 1 ELSE 0 END) store_only,"
    + " SUM(CASE WHEN ssci.customer_sk IS NULL AND csci.customer_sk IS NOT NULL"
    + " THEN 1 ELSE 0 END) catalog_only,"
    + " SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NOT NULL"
    + " THEN 1 ELSE 0 END) store_and_catalog"
    + " FROM (SELECT ss.ss_customer_sk customer_sk, ss.ss_item_sk item_sk"
    + " FROM TPCDS.store_sales ss, TPCDS.date_dim d"
    + " WHERE ss.ss_sold_date_sk = d.d_date_sk AND d.d_year = 2000"
    + " GROUP BY ss.ss_customer_sk, ss.ss_item_sk) ssci"
    + " FULL OUTER JOIN (SELECT cs.cs_bill_customer_sk customer_sk, cs.cs_item_sk item_sk"
    + " FROM TPCDS.catalog_sales cs, TPCDS.date_dim d"
    + " WHERE cs.cs_sold_date_sk = d.d_date_sk AND d.d_year = 2000"
    + " GROUP BY cs.cs_bill_customer_sk, cs.cs_item_sk) csci"
    + " ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk LIMIT 100";

  // TPC-DS Q88: counts of store sales in several time-of-day buckets, one per derived table,
  // combined by cross join.
  static final String Q88 = "SELECT * FROM"
    + " (SELECT COUNT(*) h8 FROM TPCDS.store_sales ss, TPCDS.household_demographics hd,"
    + " TPCDS.time_dim t, TPCDS.store s"
    + " WHERE ss.ss_sold_time_sk = t.t_time_sk AND ss.ss_hdemo_sk = hd.hd_demo_sk"
    + " AND ss.ss_store_sk = s.s_store_sk AND t.t_hour = 8 AND hd.hd_dep_count >= 0"
    + " AND s.s_company_id = 1) s1,"
    + " (SELECT COUNT(*) h10 FROM TPCDS.store_sales ss, TPCDS.household_demographics hd,"
    + " TPCDS.time_dim t, TPCDS.store s"
    + " WHERE ss.ss_sold_time_sk = t.t_time_sk AND ss.ss_hdemo_sk = hd.hd_demo_sk"
    + " AND ss.ss_store_sk = s.s_store_sk AND t.t_hour = 10 AND hd.hd_dep_count >= 0"
    + " AND s.s_company_id = 1) s2,"
    + " (SELECT COUNT(*) h12 FROM TPCDS.store_sales ss, TPCDS.household_demographics hd,"
    + " TPCDS.time_dim t, TPCDS.store s"
    + " WHERE ss.ss_sold_time_sk = t.t_time_sk AND ss.ss_hdemo_sk = hd.hd_demo_sk"
    + " AND ss.ss_store_sk = s.s_store_sk AND t.t_hour = 12 AND hd.hd_dep_count >= 0"
    + " AND s.s_company_id = 1) s3";

  // TPC-DS Q1: customers whose store-return total exceeds 1.2x their store's average.
  static final String Q01 = "SELECT c.c_customer_id"
    + " FROM (SELECT sr.sr_customer_sk ctr_customer_sk, sr.sr_store_sk ctr_store_sk,"
    + " SUM(sr.sr_return_amt) ctr_total_return" + " FROM TPCDS.store_returns sr, TPCDS.date_dim d"
    + " WHERE sr.sr_returned_date_sk = d.d_date_sk AND d.d_year = 2000"
    + " GROUP BY sr.sr_customer_sk, sr.sr_store_sk) ctr1,"
    + " (SELECT x.ctr_store_sk ctr_store_sk, AVG(x.ctr_total_return) * 1.2 avg_return FROM"
    + " (SELECT sr.sr_customer_sk ctr_customer_sk, sr.sr_store_sk ctr_store_sk,"
    + " SUM(sr.sr_return_amt) ctr_total_return" + " FROM TPCDS.store_returns sr, TPCDS.date_dim d"
    + " WHERE sr.sr_returned_date_sk = d.d_date_sk AND d.d_year = 2000"
    + " GROUP BY sr.sr_customer_sk, sr.sr_store_sk) x GROUP BY x.ctr_store_sk) ctr2,"
    + " TPCDS.store s, TPCDS.customer c"
    + " WHERE ctr1.ctr_total_return > ctr2.avg_return AND s.s_store_sk = ctr1.ctr_store_sk"
    + " AND ctr2.ctr_store_sk = ctr1.ctr_store_sk AND ctr1.ctr_customer_sk = c.c_customer_sk"
    + " ORDER BY c.c_customer_id LIMIT 100";

  private static final String[][] Q56_EXPECTED =
    { { "ITEM0000000024", "47" }, { "ITEM0000000012", "237.5" }, { "ITEM0000000020", "758" },
      { "ITEM0000000002", "962" }, { "ITEM0000000018", "1081.5" }, { "ITEM0000000001", "1442.5" },
      { "ITEM0000000007", "1818" }, { "ITEM0000000008", "1957.5" }, { "ITEM0000000014", "2092" },
      { "ITEM0000000006", "2725" }, { "ITEM0000000013", "2854.5" }, { "ITEM0000000019", "5817" }, };
  private static final String[][] Q60_EXPECTED = { { "ITEM0000000001", "849" },
    { "ITEM0000000007", "897" }, { "ITEM0000000018", "1293.5" }, { "ITEM0000000013", "1650" },
    { "ITEM0000000019", "1784.5" }, { "ITEM0000000006", "2093.5" }, { "ITEM0000000024", "2345" },
    { "ITEM0000000020", "3031.5" }, { "ITEM0000000012", "3220" }, { "ITEM0000000002", "3226.5" },
    { "ITEM0000000014", "3287.5" }, { "ITEM0000000008", "3913.5" }, };
  private static final String[][] Q83_EXPECTED =
    { { "ITEM0000000001", "17", "13", "11" }, { "ITEM0000000002", "20", "17", "10" },
      { "ITEM0000000003", "16", "12", "14" }, { "ITEM0000000004", "17", "12", "12" },
      { "ITEM0000000005", "13", "12", "13" }, { "ITEM0000000006", "18", "9", "7" },
      { "ITEM0000000007", "16", "15", "13" }, { "ITEM0000000008", "17", "9", "12" },
      { "ITEM0000000009", "23", "10", "12" }, { "ITEM0000000010", "16", "15", "17" },
      { "ITEM0000000011", "17", "13", "9" }, { "ITEM0000000012", "15", "14", "10" },
      { "ITEM0000000013", "12", "11", "5" }, { "ITEM0000000014", "18", "15", "10" },
      { "ITEM0000000015", "21", "16", "14" }, { "ITEM0000000016", "15", "15", "15" },
      { "ITEM0000000017", "19", "11", "12" }, { "ITEM0000000018", "14", "10", "11" },
      { "ITEM0000000019", "17", "11", "10" }, { "ITEM0000000020", "21", "8", "11" },
      { "ITEM0000000021", "19", "7", "11" }, { "ITEM0000000022", "19", "9", "11" },
      { "ITEM0000000023", "19", "13", "15" }, { "ITEM0000000024", "20", "15", "13" }, };
  private static final String[][] Q97_EXPECTED = { { "130", "82", "14" }, };
  private static final String[][] Q88_EXPECTED = { { "24", "24", "24" }, };
  private static final String[][] Q01_EXPECTED = { { "CUST000000000003" }, { "CUST000000000005" },
    { "CUST000000000006" }, { "CUST000000000009" }, { "CUST000000000014" }, { "CUST000000000014" },
    { "CUST000000000014" }, { "CUST000000000015" }, { "CUST000000000016" }, { "CUST000000000019" },
    { "CUST000000000020" }, { "CUST000000000021" }, { "CUST000000000023" }, { "CUST000000000023" },
    { "CUST000000000024" }, { "CUST000000000024" }, { "CUST000000000025" }, { "CUST000000000025" },
    { "CUST000000000026" }, { "CUST000000000027" }, { "CUST000000000029" }, };

  /** For {@link TPCDSLikeExpectedRegenerator}. */
  public static Map<String, String> queries() {
    Map<String, String> q = new LinkedHashMap<>();
    q.put("Q56", Q56);
    q.put("Q60", Q60);
    q.put("Q83", Q83);
    q.put("Q97", Q97);
    q.put("Q88", Q88);
    q.put("Q01", Q01);
    return q;
  }

  @Test
  public void testQ56() throws SQLException {
    check("Q56", Q56, Q56_EXPECTED, "UNION ALL OVER 3 QUERIES");
  }

  @Test
  public void testQ60() throws SQLException {
    check("Q60", Q60, Q60_EXPECTED, "UNION ALL OVER 3 QUERIES");
  }

  @Test
  public void testQ83() throws SQLException {
    check("Q83", Q83, Q83_EXPECTED, "HASH BUILD");
  }

  @Test
  public void testQ97() throws SQLException {
    check("Q97", Q97, Q97_EXPECTED, new String[] { "SORT-MERGE-JOIN (FULL)" },
      new String[] { "SS_I", "CS_I" });
  }

  @Test
  public void testQ88() throws SQLException {
    check("Q88", Q88, Q88_EXPECTED, "HASH BUILD");
  }

  @Test
  public void testQ01() throws SQLException {
    check("Q01", Q01, Q01_EXPECTED, "HASH BUILD");
  }
}
