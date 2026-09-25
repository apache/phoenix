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
 * TPC-DS derived integration tests over a single sales and returns channel. Each {@code @Test}
 * adapts a public TPC-DS query to Phoenix, asserts the full ordered result, and asserts the
 * rendered EXPLAIN plan.
 */
@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class TPCDSLikeSingleChannelIT extends TPCDSLikeBaseIT {

  public TPCDSLikeSingleChannelIT(String label, String schema, boolean noIndex) {
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

  // TPC-DS Q3: store sales discount by brand for a manufacturer/month.
  static final String Q03 = "SELECT d.d_year, i.i_brand_id brand_id, i.i_brand brand,"
    + " SUM(ss.ss_ext_discount_amt) sum_agg"
    + " FROM TPCDS.date_dim d, TPCDS.store_sales ss, TPCDS.item i"
    + " WHERE d.d_date_sk = ss.ss_sold_date_sk AND ss.ss_item_sk = i.i_item_sk"
    + " AND i.i_manufact_id = 1 AND d.d_moy = 11" + " GROUP BY d.d_year, i.i_brand, i.i_brand_id"
    + " ORDER BY d.d_year, sum_agg DESC, brand_id LIMIT 100";

  // TPC-DS Q7: average sale measures by item for a gender/marital/promo segment.
  static final String Q07 = "SELECT i.i_item_id, AVG(ss.ss_quantity) agg1,"
    + " AVG(ss.ss_list_price) agg2, AVG(ss.ss_coupon_amt) agg3, AVG(ss.ss_sales_price) agg4"
    + " FROM TPCDS.store_sales ss, TPCDS.customer_demographics cd, TPCDS.date_dim d,"
    + " TPCDS.item i, TPCDS.promotion p"
    + " WHERE ss.ss_sold_date_sk = d.d_date_sk AND ss.ss_item_sk = i.i_item_sk"
    + " AND ss.ss_cdemo_sk = cd.cd_demo_sk AND ss.ss_promo_sk = p.p_promo_sk"
    + " AND cd.cd_gender = 'M' AND cd.cd_marital_status = 'S'"
    + " AND (p.p_channel_email = 'N' OR p.p_channel_event = 'N') AND d.d_year = 2000"
    + " GROUP BY i.i_item_id ORDER BY i.i_item_id LIMIT 100";

  // TPC-DS Q42: store ext sales price by category for a manager/month/year.
  static final String Q42 =
    "SELECT d.d_year, i.i_category_id, i.i_category," + " SUM(ss.ss_ext_sales_price) sum_price"
      + " FROM TPCDS.date_dim d, TPCDS.store_sales ss, TPCDS.item i"
      + " WHERE d.d_date_sk = ss.ss_sold_date_sk AND ss.ss_item_sk = i.i_item_sk"
      + " AND i.i_manager_id = 1 AND d.d_moy = 11 AND d.d_year = 2000"
      + " GROUP BY d.d_year, i.i_category_id, i.i_category"
      + " ORDER BY sum_price DESC, d.d_year, i.i_category_id, i.i_category LIMIT 100";

  // TPC-DS Q52: store ext sales price by brand for a manager/month/year.
  static final String Q52 = "SELECT d.d_year, i.i_brand_id brand_id, i.i_brand brand,"
    + " SUM(ss.ss_ext_sales_price) ext_price"
    + " FROM TPCDS.date_dim d, TPCDS.store_sales ss, TPCDS.item i"
    + " WHERE d.d_date_sk = ss.ss_sold_date_sk AND ss.ss_item_sk = i.i_item_sk"
    + " AND i.i_manager_id = 1 AND d.d_moy = 11 AND d.d_year = 2000"
    + " GROUP BY d.d_year, i.i_brand, i.i_brand_id"
    + " ORDER BY d.d_year, ext_price DESC, brand_id LIMIT 100";

  // TPC-DS Q55: store ext sales price by brand for a manager/month/year.
  static final String Q55 =
    "SELECT i.i_brand_id brand_id, i.i_brand brand," + " SUM(ss.ss_ext_sales_price) ext_price"
      + " FROM TPCDS.date_dim d, TPCDS.store_sales ss, TPCDS.item i"
      + " WHERE d.d_date_sk = ss.ss_sold_date_sk AND ss.ss_item_sk = i.i_item_sk"
      + " AND i.i_manager_id = 2 AND d.d_moy = 11 AND d.d_year = 2001"
      + " GROUP BY i.i_brand, i.i_brand_id ORDER BY ext_price DESC, brand_id LIMIT 100";

  // TPC-DS Q43: store sales by day of week (CASE pivot) for a region/year.
  static final String Q43 = "SELECT s.s_store_name, s.s_store_id,"
    + " SUM(CASE WHEN d.d_day_name = 'Sunday' THEN ss.ss_sales_price ELSE 0 END) sun_sales,"
    + " SUM(CASE WHEN d.d_day_name = 'Monday' THEN ss.ss_sales_price ELSE 0 END) mon_sales,"
    + " SUM(CASE WHEN d.d_day_name = 'Tuesday' THEN ss.ss_sales_price ELSE 0 END) tue_sales,"
    + " SUM(CASE WHEN d.d_day_name = 'Wednesday' THEN ss.ss_sales_price ELSE 0 END) wed_sales,"
    + " SUM(CASE WHEN d.d_day_name = 'Thursday' THEN ss.ss_sales_price ELSE 0 END) thu_sales,"
    + " SUM(CASE WHEN d.d_day_name = 'Friday' THEN ss.ss_sales_price ELSE 0 END) fri_sales,"
    + " SUM(CASE WHEN d.d_day_name = 'Saturday' THEN ss.ss_sales_price ELSE 0 END) sat_sales"
    + " FROM TPCDS.date_dim d, TPCDS.store_sales ss, TPCDS.store s"
    + " WHERE d.d_date_sk = ss.ss_sold_date_sk AND ss.ss_store_sk = s.s_store_sk"
    + " AND s.s_gmt_offset = -5 AND d.d_year = 2000" + " GROUP BY s.s_store_name, s.s_store_id"
    + " ORDER BY s.s_store_name, s.s_store_id, sun_sales, mon_sales, tue_sales, wed_sales,"
    + " thu_sales, fri_sales, sat_sales LIMIT 100";

  // TPC-DS Q96: count of store sales in an hour band / demographic / company.
  static final String Q96 = "SELECT COUNT(*) cnt"
    + " FROM TPCDS.store_sales ss, TPCDS.household_demographics hd, TPCDS.time_dim t,"
    + " TPCDS.store s"
    + " WHERE ss.ss_sold_time_sk = t.t_time_sk AND ss.ss_hdemo_sk = hd.hd_demo_sk"
    + " AND ss.ss_store_sk = s.s_store_sk AND t.t_hour >= 8 AND hd.hd_dep_count >= 0"
    + " AND s.s_company_id = 1 ORDER BY cnt LIMIT 100";

  // TPC-DS Q19: brand revenue where customer zip differs from store zip.
  static final String Q19 = "SELECT i.i_brand_id brand_id, i.i_brand brand, i.i_manufact_id,"
    + " i.i_manufact, SUM(ss.ss_ext_sales_price) ext_price"
    + " FROM TPCDS.date_dim d, TPCDS.store_sales ss, TPCDS.item i, TPCDS.customer c,"
    + " TPCDS.customer_address ca, TPCDS.store s"
    + " WHERE d.d_date_sk = ss.ss_sold_date_sk AND ss.ss_item_sk = i.i_item_sk"
    + " AND ss.ss_customer_sk = c.c_customer_sk AND c.c_current_addr_sk = ca.ca_address_sk"
    + " AND ss.ss_store_sk = s.s_store_sk AND i.i_manager_id = 1 AND d.d_moy = 11"
    + " AND d.d_year = 2000 AND SUBSTR(ca.ca_zip, 1, 5) <> SUBSTR(s.s_zip, 1, 5)"
    + " GROUP BY i.i_brand, i.i_brand_id, i.i_manufact_id, i.i_manufact"
    + " ORDER BY ext_price DESC, i.i_brand, i.i_brand_id, i.i_manufact_id, i.i_manufact LIMIT 100";

  // TPC-DS Q46: coupon amount and profit by customer/city for a demographic segment.
  static final String Q46 = "SELECT c.c_last_name, c.c_first_name, ca.ca_city,"
    + " SUM(ss.ss_coupon_amt) amt, SUM(ss.ss_net_profit) profit"
    + " FROM TPCDS.store_sales ss, TPCDS.date_dim d, TPCDS.store s,"
    + " TPCDS.household_demographics hd, TPCDS.customer_address ca, TPCDS.customer c"
    + " WHERE ss.ss_sold_date_sk = d.d_date_sk AND ss.ss_store_sk = s.s_store_sk"
    + " AND ss.ss_hdemo_sk = hd.hd_demo_sk AND ss.ss_addr_sk = ca.ca_address_sk"
    + " AND ss.ss_customer_sk = c.c_customer_sk"
    + " AND (hd.hd_dep_count = 2 OR hd.hd_vehicle_count = 1) AND d.d_year = 2000"
    + " AND s.s_city IN ('City0', 'City1', 'City2', 'City3', 'City4')"
    + " GROUP BY c.c_last_name, c.c_first_name, ca.ca_city"
    + " ORDER BY c.c_last_name, c.c_first_name, ca.ca_city, amt, profit LIMIT 100";

  // TPC-DS Q50: store return lag buckets by store (self-join on date_dim).
  static final String Q50 = "SELECT s.s_store_name, s.s_store_id,"
    + " SUM(CASE WHEN (d2.d_date_sk - d1.d_date_sk <= 1) THEN 1 ELSE 0 END) days_le_1,"
    + " SUM(CASE WHEN (d2.d_date_sk - d1.d_date_sk > 1 AND d2.d_date_sk - d1.d_date_sk <= 3)"
    + " THEN 1 ELSE 0 END) days_2_3,"
    + " SUM(CASE WHEN (d2.d_date_sk - d1.d_date_sk > 3) THEN 1 ELSE 0 END) days_gt_3"
    + " FROM TPCDS.store_sales ss, TPCDS.store_returns sr, TPCDS.date_dim d1,"
    + " TPCDS.date_dim d2, TPCDS.store s"
    + " WHERE ss.ss_ticket_number = sr.sr_ticket_number AND ss.ss_item_sk = sr.sr_item_sk"
    + " AND ss.ss_sold_date_sk = d1.d_date_sk AND sr.sr_returned_date_sk = d2.d_date_sk"
    + " AND ss.ss_store_sk = s.s_store_sk AND d2.d_year = 2000"
    + " GROUP BY s.s_store_name, s.s_store_id ORDER BY s.s_store_name, s.s_store_id LIMIT 100";

  // TPC-DS Q73: tickets per customer via a grouped derived table joined to customer.
  static final String Q73 = "SELECT c.c_last_name, c.c_first_name, dj.ss_ticket_number, dj.cnt"
    + " FROM (SELECT ss.ss_ticket_number ss_ticket_number, ss.ss_customer_sk ss_customer_sk,"
    + " COUNT(*) cnt FROM TPCDS.store_sales ss, TPCDS.date_dim d, TPCDS.store s,"
    + " TPCDS.household_demographics hd"
    + " WHERE ss.ss_sold_date_sk = d.d_date_sk AND ss.ss_store_sk = s.s_store_sk"
    + " AND ss.ss_hdemo_sk = hd.hd_demo_sk AND d.d_year = 2000 AND hd.hd_vehicle_count >= 0"
    + " AND s.s_company_id = 1 GROUP BY ss.ss_ticket_number, ss.ss_customer_sk) dj,"
    + " TPCDS.customer c" + " WHERE dj.ss_customer_sk = c.c_customer_sk AND dj.cnt BETWEEN 1 AND 5"
    + " ORDER BY dj.cnt DESC, c.c_last_name, c.c_first_name, dj.ss_ticket_number LIMIT 20";

  // TPC-DS Q82: items in a price range with on-hand inventory that were also sold in store.
  static final String Q82 = "SELECT i.i_item_id, i.i_item_desc, i.i_current_price"
    + " FROM TPCDS.item i, TPCDS.inventory inv, TPCDS.date_dim d, TPCDS.store_sales ss"
    + " WHERE i.i_current_price BETWEEN 20 AND 80 AND inv.inv_item_sk = i.i_item_sk"
    + " AND d.d_date_sk = inv.inv_date_sk AND inv.inv_quantity_on_hand BETWEEN 100 AND 500"
    + " AND i.i_manufact_id IN (1, 2, 3, 4, 5) AND inv.inv_item_sk = ss.ss_item_sk"
    + " GROUP BY i.i_item_id, i.i_item_desc, i.i_current_price ORDER BY i.i_item_id LIMIT 100";

  // TPC-DS Q21: inventory before/after a pivot date by warehouse/item (CASE pivot).
  static final String Q21 = "SELECT w.w_warehouse_name, i.i_item_id,"
    + " SUM(CASE WHEN d.d_date_sk < 12 THEN inv.inv_quantity_on_hand ELSE 0 END) inv_before,"
    + " SUM(CASE WHEN d.d_date_sk >= 12 THEN inv.inv_quantity_on_hand ELSE 0 END) inv_after"
    + " FROM TPCDS.inventory inv, TPCDS.warehouse w, TPCDS.item i, TPCDS.date_dim d"
    + " WHERE i.i_item_sk = inv.inv_item_sk AND inv.inv_warehouse_sk = w.w_warehouse_sk"
    + " AND inv.inv_date_sk = d.d_date_sk AND i.i_current_price BETWEEN 10 AND 100"
    + " AND i.i_manufact_id IN (1, 2, 3) AND w.w_warehouse_sk IN (1, 2)"
    + " GROUP BY w.w_warehouse_name, i.i_item_id"
    + " ORDER BY w.w_warehouse_name, i.i_item_id LIMIT 100";

  // TPC-DS Q26: average catalog sale measures by item for a gender/marital/promo segment.
  static final String Q26 = "SELECT i.i_item_id, AVG(cs.cs_quantity) agg1,"
    + " AVG(cs.cs_list_price) agg2, AVG(cs.cs_coupon_amt) agg3, AVG(cs.cs_sales_price) agg4"
    + " FROM TPCDS.catalog_sales cs, TPCDS.customer_demographics cd, TPCDS.date_dim d,"
    + " TPCDS.item i, TPCDS.promotion p"
    + " WHERE cs.cs_sold_date_sk = d.d_date_sk AND cs.cs_item_sk = i.i_item_sk"
    + " AND cs.cs_bill_cdemo_sk = cd.cd_demo_sk AND cs.cs_promo_sk = p.p_promo_sk"
    + " AND cd.cd_gender = 'F' AND cd.cd_marital_status = 'M'"
    + " AND (p.p_channel_email = 'N' OR p.p_channel_event = 'N') AND d.d_year = 2000"
    + " GROUP BY i.i_item_id ORDER BY i.i_item_id LIMIT 100";

  private static final String[][] Q03_EXPECTED =
    { { "2000", "1002", "brand#1002", "41" }, { "2000", "1000", "brand#1000", "37" },
      { "2000", "1005", "brand#1005", "36" }, { "2000", "1007", "brand#1007", "18" },
      { "2000", "1004", "brand#1004", "5" }, { "2001", "1004", "brand#1004", "36" },
      { "2001", "1002", "brand#1002", "21" }, { "2001", "1007", "brand#1007", "18" },
      { "2001", "1000", "brand#1000", "11" }, { "2001", "1005", "brand#1005", "7" }, };
  private static final String[][] Q07_EXPECTED = { { "ITEM0000000001", "3", "73.5", "16", "57.5" },
    { "ITEM0000000003", "13", "30.5", "2", "27.5" }, { "ITEM0000000005", "7", "37.5", "8", "26.5" },
    { "ITEM0000000007", "8", "72.5", "1", "67.5" },
    { "ITEM0000000009", "14", "92.5", "10", "81.5" },
    { "ITEM0000000011", "2", "106.5", "15", "98.5" },
    { "ITEM0000000013", "5", "66.5", "10", "55.5" }, { "ITEM0000000015", "9", "34.5", "2", "15.5" },
    { "ITEM0000000017", "8", "51.5", "14", "49.5" }, };
  private static final String[][] Q42_EXPECTED = { { "2000", "5", "Jewelry", "748" },
    { "2000", "3", "Electronics", "591" }, { "2000", "1", "Books", "61" }, };
  private static final String[][] Q52_EXPECTED = { { "2000", "1002", "brand#1002", "748" },
    { "2000", "1004", "brand#1004", "591" }, { "2000", "1000", "brand#1000", "61" }, };
  private static final String[][] Q55_EXPECTED = { { "1001", "brand#1001", "456.5" },
    { "1003", "brand#1003", "257.5" }, { "1005", "brand#1005", "82.5" }, };
  private static final String[][] Q43_EXPECTED =
    { { "Store 1", "STORE0000000001", "0", "0", "0", "0", "1638", "0", "0" },
      { "Store 5", "STORE0000000005", "1229", "0", "0", "0", "0", "0", "0" }, };
  private static final String[][] Q96_EXPECTED = { { "192" }, };
  private static final String[][] Q19_EXPECTED =
    { { "1002", "brand#1002", "1", "manufact#1", "748" },
      { "1004", "brand#1004", "1", "manufact#1", "591" },
      { "1000", "brand#1000", "1", "manufact#1", "61" }, };
  private static final String[][] Q46_EXPECTED = { { "Last10", "First10", "City2", "45", "2338.5" },
    { "Last14", "First14", "City6", "48", "2291.5" },
    { "Last15", "First15", "City0", "29", "2357" }, { "Last18", "First18", "City3", "31", "2724" },
    { "Last2", "First2", "City1", "24", "632" }, { "Last20", "First20", "City5", "45", "3686" },
    { "Last22", "First22", "City1", "35", "2991" }, { "Last26", "First26", "City5", "57", "3988" },
    { "Last27", "First27", "City6", "62", "1133" }, { "Last3", "First3", "City2", "26", "908.5" },
    { "Last30", "First30", "City2", "27", "542" }, { "Last6", "First6", "City5", "42", "1937.5" },
    { "Last8", "First8", "City0", "24", "701.5" }, };
  private static final String[][] Q50_EXPECTED = { { "Store 1", "STORE0000000001", "24", "0", "0" },
    { "Store 3", "STORE0000000003", "24", "0", "0" },
    { "Store 5", "STORE0000000005", "24", "0", "0" }, };
  private static final String[][] Q73_EXPECTED =
    { { "Last1", "First1", "222", "1" }, { "Last1", "First1", "245", "1" },
      { "Last1", "First1", "268", "1" }, { "Last10", "First10", "16", "1" },
      { "Last10", "First10", "39", "1" }, { "Last10", "First10", "62", "1" },
      { "Last10", "First10", "85", "1" }, { "Last11", "First11", "5", "1" },
      { "Last11", "First11", "28", "1" }, { "Last11", "First11", "51", "1" },
      { "Last11", "First11", "74", "1" }, { "Last11", "First11", "97", "1" },
      { "Last12", "First12", "17", "1" }, { "Last12", "First12", "40", "1" },
      { "Last12", "First12", "63", "1" }, { "Last12", "First12", "86", "1" },
      { "Last12", "First12", "109", "1" }, { "Last13", "First13", "6", "1" },
      { "Last13", "First13", "29", "1" }, { "Last13", "First13", "52", "1" }, };
  private static final String[][] Q82_EXPECTED =
    { { "ITEM0000000010", "Item description 10", "20.99" },
      { "ITEM0000000011", "Item description 11", "21.99" },
      { "ITEM0000000012", "Item description 12", "22.99" },
      { "ITEM0000000013", "Item description 13", "23.99" },
      { "ITEM0000000014", "Item description 14", "24.99" },
      { "ITEM0000000015", "Item description 15", "25.99" },
      { "ITEM0000000016", "Item description 16", "26.99" },
      { "ITEM0000000017", "Item description 17", "27.99" },
      { "ITEM0000000018", "Item description 18", "28.99" },
      { "ITEM0000000019", "Item description 19", "29.99" },
      { "ITEM0000000020", "Item description 20", "30.99" },
      { "ITEM0000000021", "Item description 21", "31.99" },
      { "ITEM0000000022", "Item description 22", "32.99" },
      { "ITEM0000000023", "Item description 23", "33.99" },
      { "ITEM0000000024", "Item description 24", "34.99" }, };
  private static final String[][] Q21_EXPECTED =
    { { "Warehouse 1", "ITEM0000000001", "1091", "1203" },
      { "Warehouse 1", "ITEM0000000002", "1053", "1257" },
      { "Warehouse 1", "ITEM0000000003", "1681", "1517" },
      { "Warehouse 1", "ITEM0000000006", "1631", "1011" },
      { "Warehouse 1", "ITEM0000000007", "1237", "1972" },
      { "Warehouse 1", "ITEM0000000008", "1187", "1271" },
      { "Warehouse 1", "ITEM0000000011", "1306", "1307" },
      { "Warehouse 1", "ITEM0000000012", "587", "1511" },
      { "Warehouse 1", "ITEM0000000013", "1212", "1372" },
      { "Warehouse 1", "ITEM0000000016", "1611", "1589" },
      { "Warehouse 1", "ITEM0000000017", "1242", "1589" },
      { "Warehouse 1", "ITEM0000000018", "1012", "1618" },
      { "Warehouse 1", "ITEM0000000021", "1582", "948" },
      { "Warehouse 1", "ITEM0000000022", "1074", "1872" },
      { "Warehouse 1", "ITEM0000000023", "1792", "1537" },
      { "Warehouse 2", "ITEM0000000001", "831", "1683" },
      { "Warehouse 2", "ITEM0000000002", "1462", "899" },
      { "Warehouse 2", "ITEM0000000003", "1873", "1837" },
      { "Warehouse 2", "ITEM0000000006", "1010", "1437" },
      { "Warehouse 2", "ITEM0000000007", "1107", "1515" },
      { "Warehouse 2", "ITEM0000000008", "1255", "1714" },
      { "Warehouse 2", "ITEM0000000011", "1073", "941" },
      { "Warehouse 2", "ITEM0000000012", "1936", "1490" },
      { "Warehouse 2", "ITEM0000000013", "1015", "2189" },
      { "Warehouse 2", "ITEM0000000016", "1331", "1388" },
      { "Warehouse 2", "ITEM0000000017", "1721", "1268" },
      { "Warehouse 2", "ITEM0000000018", "1434", "900" },
      { "Warehouse 2", "ITEM0000000021", "1306", "1858" },
      { "Warehouse 2", "ITEM0000000022", "946", "1806" },
      { "Warehouse 2", "ITEM0000000023", "1159", "1383" }, };
  private static final String[][] Q26_EXPECTED = { { "ITEM0000000003", "2", "109.5", "13", "92.5" },
    { "ITEM0000000006", "8", "77.5", "6", "68.5" },
    { "ITEM0000000009", "11", "76.5", "18", "57.5" }, { "ITEM0000000012", "16", "63.5", "9", "54" },
    { "ITEM0000000015", "6", "32.5", "6", "18.5" },
    { "ITEM0000000018", "18", "62.5", "11", "52.5" },
    { "ITEM0000000021", "18", "9.5", "13", "5.5" },
    { "ITEM0000000024", "17", "77.5", "12", "67.5" }, };

  /** Label for {@link TPCDSLikeExpectedRegenerator}. */
  public static Map<String, String> queries() {
    Map<String, String> q = new LinkedHashMap<>();
    q.put("Q03", Q03);
    q.put("Q07", Q07);
    q.put("Q42", Q42);
    q.put("Q52", Q52);
    q.put("Q55", Q55);
    q.put("Q43", Q43);
    q.put("Q96", Q96);
    q.put("Q19", Q19);
    q.put("Q46", Q46);
    q.put("Q50", Q50);
    q.put("Q73", Q73);
    q.put("Q82", Q82);
    q.put("Q21", Q21);
    q.put("Q26", Q26);
    return q;
  }

  @Test
  public void testQ03() throws SQLException {
    check("Q03", Q03, Q03_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "SS_I" });
  }

  @Test
  public void testQ07() throws SQLException {
    check("Q07", Q07, Q07_EXPECTED, "HASH BUILD");
  }

  @Test
  public void testQ42() throws SQLException {
    check("Q42", Q42, Q42_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "SS_I" });
  }

  @Test
  public void testQ52() throws SQLException {
    check("Q52", Q52, Q52_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "SS_I" });
  }

  @Test
  public void testQ55() throws SQLException {
    check("Q55", Q55, Q55_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "SS_I" });
  }

  @Test
  public void testQ43() throws SQLException {
    check("Q43", Q43, Q43_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "SS_I" });
  }

  @Test
  public void testQ96() throws SQLException {
    check("Q96", Q96, Q96_EXPECTED, "HASH BUILD");
  }

  @Test
  public void testQ19() throws SQLException {
    check("Q19", Q19, Q19_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "SS_I" });
  }

  @Test
  public void testQ46() throws SQLException {
    check("Q46", Q46, Q46_EXPECTED, "HASH BUILD");
  }

  @Test
  public void testQ50() throws SQLException {
    check("Q50", Q50, Q50_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "SS_I" });
  }

  @Test
  public void testQ73() throws SQLException {
    check("Q73", Q73, Q73_EXPECTED, "HASH BUILD");
  }

  @Test
  public void testQ82() throws SQLException {
    check("Q82", Q82, Q82_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "INV_I" });
  }

  @Test
  public void testQ21() throws SQLException {
    check("Q21", Q21, Q21_EXPECTED, new String[] { "HASH BUILD" }, new String[] { "INV_I" });
  }

  @Test
  public void testQ26() throws SQLException {
    check("Q26", Q26, Q26_EXPECTED, "HASH BUILD");
  }
}
