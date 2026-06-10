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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * Shared deterministic TPC-DS derived fixture for {@link TPCDSLikeSingleChannelIT} and
 * {@link TpcdsLikeCrossChannelIT}.
 * <p>
 * The schema is a trimmed subset of the TPC-DS v4.0.0 schema, only the tables and columns touched
 * by the adapted queries, with the following uniform Phoenix adaptation rules applied:
 * <ul>
 * <li>Surrogate keys ({@code *_sk}) become single-column {@code BIGINT NOT NULL PRIMARY KEY}
 * columns on the dimension tables.</li>
 * <li>Sales and returns fact tables use a composite primary key on
 * {@code (*_item_sk, *_ticket_number)} or {@code *_order_number}. {@code inventory} uses
 * {@code (inv_date_sk, inv_item_sk, inv_warehouse_sk)}.</li>
 * <li>Fact tables are salted ({@code SALT_BUCKETS=4}).</li>
 * <li>Foreign key columns are plain nullable columns. Referential consistency is guaranteed by this
 * loader, not by the schema.</li>
 * </ul>
 * <p>
 * The data is generated with a fixed {@link Random} seed so that every adapted query has a stable
 * reference answer.
 * <p>
 * The fixture is materialized into two schemas that hold identical data:
 * <ul>
 * <li>{@link #SCHEMA} ({@value #SCHEMA}) -- base tables, no secondary indexes.</li>
 * <li>{@link #SCHEMA_INDEXED} ({@value #SCHEMA_INDEXED}) -- the same, plus a covering global index
 * per fact table.</li>
 * </ul>
 */
public final class TPCDSLikeFixture {

  /** Base schema with no secondary indexes */
  public static final String SCHEMA = "TPCDS";
  /** Schema holding identical data plus a covering global index per fact table. */
  public static final String SCHEMA_INDEXED = "TPCDSI";

  private static final long SEED = 42L;

  private static volatile boolean loaded = false;

  private TPCDSLikeFixture() {
  }

  /** All base (dimension + fact) table short names, in dependency order, dimensions first. */
  static final String[] TABLES =
    { "DATE_DIM", "TIME_DIM", "ITEM", "CUSTOMER_ADDRESS", "CUSTOMER_DEMOGRAPHICS", "INCOME_BAND",
      "HOUSEHOLD_DEMOGRAPHICS", "CUSTOMER", "STORE", "CALL_CENTER", "CATALOG_PAGE", "WEB_SITE",
      "WEB_PAGE", "WAREHOUSE", "SHIP_MODE", "PROMOTION", "REASON", "STORE_SALES", "STORE_RETURNS",
      "CATALOG_SALES", "CATALOG_RETURNS", "WEB_SALES", "WEB_RETURNS", "INVENTORY" };

  /** Create both schemas and populate them with deterministic data. */
  public static synchronized void load(Connection conn) throws SQLException {
    if (loaded && tablesPopulated(conn)) {
      return;
    }
    conn.setAutoCommit(false);
    createTables(conn, SCHEMA);
    createTables(conn, SCHEMA_INDEXED);
    if (!tablesPopulated(conn)) {
      loadData(conn, SCHEMA);
      copyData(conn, SCHEMA, SCHEMA_INDEXED);
    }
    createIndexes(conn, SCHEMA_INDEXED);
    conn.commit();
    loaded = true;
  }

  private static boolean tablesPopulated(Connection conn) {
    try (Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + ".STORE_SALES")) {
      return rs.next() && rs.getInt(1) > 0;
    } catch (SQLException e) {
      return false;
    }
  }

  private static void createTables(Connection conn, String s) throws SQLException {
    try (Statement st = conn.createStatement()) {
      for (String ddl : ddl(s)) {
        st.execute(ddl);
      }
    }
  }

  private static void createIndexes(Connection conn, String s) throws SQLException {
    try (Statement st = conn.createStatement()) {
      for (String ddl : indexDdl(s)) {
        st.execute(ddl);
      }
    }
  }

  private static String[] ddl(String s) {
    return new String[] {
      // Dimensions
      "CREATE TABLE IF NOT EXISTS " + s + ".DATE_DIM (" + "d_date_sk BIGINT NOT NULL PRIMARY KEY,"
        + "d_date DATE, d_year INTEGER, d_moy INTEGER, d_dom INTEGER, d_qoy INTEGER,"
        + "d_dow INTEGER, d_week_seq INTEGER, d_day_name VARCHAR(16), d_quarter_name VARCHAR(8))",
      "CREATE TABLE IF NOT EXISTS " + s + ".TIME_DIM (" + "t_time_sk BIGINT NOT NULL PRIMARY KEY,"
        + "t_hour INTEGER, t_minute INTEGER, t_am_pm VARCHAR(2), t_shift VARCHAR(16),"
        + "t_meal_time VARCHAR(16))",
      "CREATE TABLE IF NOT EXISTS " + s + ".ITEM (" + "i_item_sk BIGINT NOT NULL PRIMARY KEY,"
        + "i_item_id VARCHAR(16), i_item_desc VARCHAR(100), i_current_price DECIMAL(7,2),"
        + "i_brand_id INTEGER, i_brand VARCHAR(50), i_class_id INTEGER, i_class VARCHAR(50),"
        + "i_category_id INTEGER, i_category VARCHAR(50), i_manufact_id INTEGER,"
        + "i_manufact VARCHAR(50), i_manager_id INTEGER, i_size VARCHAR(20), i_color VARCHAR(20),"
        + "i_units VARCHAR(10), i_product_name VARCHAR(50))",
      "CREATE TABLE IF NOT EXISTS " + s + ".CUSTOMER_ADDRESS ("
        + "ca_address_sk BIGINT NOT NULL PRIMARY KEY, ca_city VARCHAR(60), ca_county VARCHAR(30),"
        + "ca_state VARCHAR(2), ca_zip VARCHAR(10), ca_country VARCHAR(20),"
        + "ca_gmt_offset DECIMAL(5,2), ca_street_number VARCHAR(10), ca_street_name VARCHAR(60))",
      "CREATE TABLE IF NOT EXISTS " + s + ".CUSTOMER_DEMOGRAPHICS ("
        + "cd_demo_sk BIGINT NOT NULL PRIMARY KEY, cd_gender VARCHAR(1),"
        + "cd_marital_status VARCHAR(1), cd_education_status VARCHAR(20),"
        + "cd_purchase_estimate INTEGER, cd_credit_rating VARCHAR(10), cd_dep_count INTEGER,"
        + "cd_dep_employed_count INTEGER, cd_dep_college_count INTEGER)",
      "CREATE TABLE IF NOT EXISTS " + s + ".INCOME_BAND ("
        + "ib_income_band_sk BIGINT NOT NULL PRIMARY KEY, ib_lower_bound INTEGER,"
        + "ib_upper_bound INTEGER)",
      "CREATE TABLE IF NOT EXISTS " + s + ".HOUSEHOLD_DEMOGRAPHICS ("
        + "hd_demo_sk BIGINT NOT NULL PRIMARY KEY, hd_income_band_sk BIGINT,"
        + "hd_buy_potential VARCHAR(15), hd_dep_count INTEGER, hd_vehicle_count INTEGER)",
      "CREATE TABLE IF NOT EXISTS " + s + ".CUSTOMER ("
        + "c_customer_sk BIGINT NOT NULL PRIMARY KEY, c_customer_id VARCHAR(16),"
        + "c_current_cdemo_sk BIGINT, c_current_hdemo_sk BIGINT, c_current_addr_sk BIGINT,"
        + "c_first_name VARCHAR(20), c_last_name VARCHAR(30), c_birth_country VARCHAR(20),"
        + "c_birth_year INTEGER, c_preferred_cust_flag VARCHAR(1), c_salutation VARCHAR(10),"
        + "c_email_address VARCHAR(50))",
      "CREATE TABLE IF NOT EXISTS " + s + ".STORE (" + "s_store_sk BIGINT NOT NULL PRIMARY KEY,"
        + "s_store_id VARCHAR(16), s_store_name VARCHAR(50), s_company_id INTEGER,"
        + "s_company_name VARCHAR(50), s_state VARCHAR(2), s_zip VARCHAR(10),"
        + "s_gmt_offset DECIMAL(5,2), s_city VARCHAR(60), s_county VARCHAR(30),"
        + "s_manager VARCHAR(40), s_number_employees INTEGER)",
      "CREATE TABLE IF NOT EXISTS " + s + ".CALL_CENTER ("
        + "cc_call_center_sk BIGINT NOT NULL PRIMARY KEY, cc_call_center_id VARCHAR(16),"
        + "cc_name VARCHAR(50), cc_manager VARCHAR(40), cc_mkt_id INTEGER, cc_class VARCHAR(50),"
        + "cc_company INTEGER)",
      "CREATE TABLE IF NOT EXISTS " + s + ".CATALOG_PAGE ("
        + "cp_catalog_page_sk BIGINT NOT NULL PRIMARY KEY, cp_catalog_page_id VARCHAR(16),"
        + "cp_catalog_number INTEGER, cp_catalog_page_number INTEGER, cp_department VARCHAR(50))",
      "CREATE TABLE IF NOT EXISTS " + s + ".WEB_SITE (" + "web_site_sk BIGINT NOT NULL PRIMARY KEY,"
        + "web_site_id VARCHAR(16), web_name VARCHAR(50), web_company_id INTEGER,"
        + "web_company_name VARCHAR(50))",
      "CREATE TABLE IF NOT EXISTS " + s + ".WEB_PAGE ("
        + "wp_web_page_sk BIGINT NOT NULL PRIMARY KEY, wp_web_page_id VARCHAR(16),"
        + "wp_char_count INTEGER, wp_type VARCHAR(50))",
      "CREATE TABLE IF NOT EXISTS " + s + ".WAREHOUSE ("
        + "w_warehouse_sk BIGINT NOT NULL PRIMARY KEY, w_warehouse_id VARCHAR(16),"
        + "w_warehouse_name VARCHAR(50), w_state VARCHAR(2), w_country VARCHAR(20),"
        + "w_gmt_offset DECIMAL(5,2))",
      "CREATE TABLE IF NOT EXISTS " + s + ".SHIP_MODE ("
        + "sm_ship_mode_sk BIGINT NOT NULL PRIMARY KEY, sm_ship_mode_id VARCHAR(16),"
        + "sm_type VARCHAR(30), sm_carrier VARCHAR(20))",
      "CREATE TABLE IF NOT EXISTS " + s + ".PROMOTION ("
        + "p_promo_sk BIGINT NOT NULL PRIMARY KEY, p_promo_id VARCHAR(16),"
        + "p_channel_email VARCHAR(1), p_channel_event VARCHAR(1), p_channel_tv VARCHAR(1),"
        + "p_channel_dmail VARCHAR(1), p_channel_catalog VARCHAR(1))",
      "CREATE TABLE IF NOT EXISTS " + s + ".REASON (" + "r_reason_sk BIGINT NOT NULL PRIMARY KEY,"
        + "r_reason_id VARCHAR(16), r_reason_desc VARCHAR(100))",
      // Facts
      "CREATE TABLE IF NOT EXISTS " + s + ".STORE_SALES (" + "ss_item_sk BIGINT NOT NULL,"
        + "ss_ticket_number BIGINT NOT NULL, ss_sold_date_sk BIGINT, ss_sold_time_sk BIGINT,"
        + "ss_customer_sk BIGINT, ss_cdemo_sk BIGINT, ss_hdemo_sk BIGINT, ss_addr_sk BIGINT,"
        + "ss_store_sk BIGINT, ss_promo_sk BIGINT, ss_quantity INTEGER, ss_sales_price DECIMAL(7,2),"
        + "ss_ext_sales_price DECIMAL(7,2), ss_ext_discount_amt DECIMAL(7,2),"
        + "ss_list_price DECIMAL(7,2), ss_ext_list_price DECIMAL(7,2), ss_coupon_amt DECIMAL(7,2),"
        + "ss_net_profit DECIMAL(7,2), ss_net_paid DECIMAL(7,2) "
        + "CONSTRAINT pk PRIMARY KEY (ss_item_sk, ss_ticket_number)) SALT_BUCKETS=4",
      "CREATE TABLE IF NOT EXISTS " + s + ".STORE_RETURNS (" + "sr_item_sk BIGINT NOT NULL,"
        + "sr_ticket_number BIGINT NOT NULL, sr_returned_date_sk BIGINT, sr_customer_sk BIGINT,"
        + "sr_cdemo_sk BIGINT, sr_hdemo_sk BIGINT, sr_addr_sk BIGINT, sr_store_sk BIGINT,"
        + "sr_reason_sk BIGINT, sr_return_quantity INTEGER, sr_return_amt DECIMAL(7,2),"
        + "sr_net_loss DECIMAL(7,2), sr_fee DECIMAL(7,2) "
        + "CONSTRAINT pk PRIMARY KEY (sr_item_sk, sr_ticket_number)) SALT_BUCKETS=4",
      "CREATE TABLE IF NOT EXISTS " + s + ".CATALOG_SALES (" + "cs_item_sk BIGINT NOT NULL,"
        + "cs_order_number BIGINT NOT NULL, cs_sold_date_sk BIGINT, cs_bill_customer_sk BIGINT,"
        + "cs_bill_cdemo_sk BIGINT, cs_ship_customer_sk BIGINT, cs_ship_addr_sk BIGINT,"
        + "cs_call_center_sk BIGINT, cs_catalog_page_sk BIGINT, cs_warehouse_sk BIGINT,"
        + "cs_ship_mode_sk BIGINT, cs_promo_sk BIGINT, cs_quantity INTEGER,"
        + "cs_sales_price DECIMAL(7,2), cs_ext_sales_price DECIMAL(7,2),"
        + "cs_ext_discount_amt DECIMAL(7,2), cs_list_price DECIMAL(7,2),"
        + "cs_ext_list_price DECIMAL(7,2), cs_coupon_amt DECIMAL(7,2), cs_net_profit DECIMAL(7,2),"
        + "cs_net_paid DECIMAL(7,2) "
        + "CONSTRAINT pk PRIMARY KEY (cs_item_sk, cs_order_number)) SALT_BUCKETS=4",
      "CREATE TABLE IF NOT EXISTS " + s + ".CATALOG_RETURNS (" + "cr_item_sk BIGINT NOT NULL,"
        + "cr_order_number BIGINT NOT NULL, cr_returned_date_sk BIGINT,"
        + "cr_returning_customer_sk BIGINT, cr_call_center_sk BIGINT, cr_catalog_page_sk BIGINT,"
        + "cr_warehouse_sk BIGINT, cr_reason_sk BIGINT, cr_return_quantity INTEGER,"
        + "cr_return_amount DECIMAL(7,2), cr_net_loss DECIMAL(7,2) "
        + "CONSTRAINT pk PRIMARY KEY (cr_item_sk, cr_order_number)) SALT_BUCKETS=4",
      "CREATE TABLE IF NOT EXISTS " + s + ".WEB_SALES (" + "ws_item_sk BIGINT NOT NULL,"
        + "ws_order_number BIGINT NOT NULL, ws_sold_date_sk BIGINT, ws_sold_time_sk BIGINT,"
        + "ws_bill_customer_sk BIGINT, ws_ship_customer_sk BIGINT, ws_ship_addr_sk BIGINT,"
        + "ws_web_site_sk BIGINT, ws_web_page_sk BIGINT, ws_warehouse_sk BIGINT,"
        + "ws_ship_mode_sk BIGINT, ws_promo_sk BIGINT, ws_quantity INTEGER,"
        + "ws_sales_price DECIMAL(7,2), ws_ext_sales_price DECIMAL(7,2),"
        + "ws_ext_discount_amt DECIMAL(7,2), ws_list_price DECIMAL(7,2),"
        + "ws_ext_list_price DECIMAL(7,2), ws_coupon_amt DECIMAL(7,2), ws_net_profit DECIMAL(7,2),"
        + "ws_net_paid DECIMAL(7,2) "
        + "CONSTRAINT pk PRIMARY KEY (ws_item_sk, ws_order_number)) SALT_BUCKETS=4",
      "CREATE TABLE IF NOT EXISTS " + s + ".WEB_RETURNS (" + "wr_item_sk BIGINT NOT NULL,"
        + "wr_order_number BIGINT NOT NULL, wr_returned_date_sk BIGINT,"
        + "wr_returning_customer_sk BIGINT, wr_web_page_sk BIGINT, wr_reason_sk BIGINT,"
        + "wr_return_quantity INTEGER, wr_return_amt DECIMAL(7,2), wr_net_loss DECIMAL(7,2) "
        + "CONSTRAINT pk PRIMARY KEY (wr_item_sk, wr_order_number)) SALT_BUCKETS=4",
      "CREATE TABLE IF NOT EXISTS " + s + ".INVENTORY (" + "inv_date_sk BIGINT NOT NULL,"
        + "inv_item_sk BIGINT NOT NULL, inv_warehouse_sk BIGINT NOT NULL,"
        + "inv_quantity_on_hand INTEGER "
        + "CONSTRAINT pk PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)) SALT_BUCKETS=4" };
  }

  private static String[] indexDdl(String s) {
    String suffix = "_I";
    return new String[] {
      "CREATE INDEX IF NOT EXISTS SS" + suffix + " ON " + s
        + ".STORE_SALES (ss_sold_date_sk, ss_item_sk) INCLUDE (ss_store_sk, ss_customer_sk,"
        + " ss_quantity, ss_sales_price, ss_ext_sales_price, ss_ext_discount_amt, ss_list_price,"
        + " ss_net_profit, ss_net_paid)",
      "CREATE INDEX IF NOT EXISTS CS" + suffix + " ON " + s
        + ".CATALOG_SALES (cs_sold_date_sk, cs_item_sk) INCLUDE (cs_bill_customer_sk,"
        + " cs_warehouse_sk, cs_quantity, cs_sales_price, cs_ext_sales_price, cs_ext_discount_amt,"
        + " cs_list_price, cs_net_profit, cs_net_paid)",
      "CREATE INDEX IF NOT EXISTS WS" + suffix + " ON " + s
        + ".WEB_SALES (ws_sold_date_sk, ws_item_sk) INCLUDE (ws_bill_customer_sk, ws_warehouse_sk,"
        + " ws_quantity, ws_sales_price, ws_ext_sales_price, ws_ext_discount_amt, ws_list_price,"
        + " ws_net_profit, ws_net_paid)",
      "CREATE INDEX IF NOT EXISTS INV" + suffix + " ON " + s
        + ".INVENTORY (inv_item_sk, inv_date_sk) INCLUDE (inv_warehouse_sk, inv_quantity_on_hand)" };
  }

  static final int[] YEARS = { 2000, 2001 };
  static final String[] CATEGORIES =
    { "Books", "Home", "Electronics", "Sports", "Jewelry", "Music" };
  static final String[] STATES = { "CA", "TX", "NY", "WA", "OR" };
  static final String[] GENDERS = { "M", "F" };
  static final String[] MARITAL = { "M", "S", "D", "W", "U" };
  static final String[] EDUCATION =
    { "Primary", "Secondary", "College", "2 yr Degree", "4 yr Degree", "Advanced Degree" };
  static final String[] BUY_POTENTIAL =
    { ">10000", "5001-10000", "1001-5000", "501-1000", "0-500", "Unknown" };
  static final String[] DAY_NAMES =
    { "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };
  static final double[] GMT_OFFSETS = { -5, -6, -7, -8 };

  private static final int N_ITEMS = 24;
  private static final int N_CUSTOMERS = 30;
  private static final int N_ADDR = 20;
  private static final int N_CDEMO = 12;
  private static final int N_HDEMO = 12;
  private static final int N_STORES = 6;
  private static final int N_CALL_CENTERS = 4;
  private static final int N_CATALOG_PAGES = 6;
  private static final int N_WEB_SITES = 4;
  private static final int N_WEB_PAGES = 6;
  private static final int N_WAREHOUSES = 4;
  private static final int N_SHIP_MODES = 5;
  private static final int N_PROMOS = 10;
  private static final int N_REASONS = 6;

  // date_dim: 24 rows = 12 months x 2 years, d_date_sk = 1..24.
  private static final int N_DATES = YEARS.length * 12;
  private static final int N_TIMES = 12;

  private static void loadData(Connection conn, String s) throws SQLException {
    Random rnd = new Random(SEED);
    loadDateDim(conn, s);
    loadTimeDim(conn, s);
    loadItem(conn, s, rnd);
    loadCustomerAddress(conn, s, rnd);
    loadCustomerDemographics(conn, s);
    loadIncomeBand(conn, s);
    loadHouseholdDemographics(conn, s, rnd);
    loadCustomer(conn, s, rnd);
    loadStore(conn, s, rnd);
    loadCallCenter(conn, s);
    loadCatalogPage(conn, s);
    loadWebSite(conn, s);
    loadWebPage(conn, s);
    loadWarehouse(conn, s, rnd);
    loadShipMode(conn, s);
    loadPromotion(conn, s, rnd);
    loadReason(conn, s);
    conn.commit();
    loadStoreSales(conn, s, rnd);
    loadStoreReturns(conn, s, rnd);
    loadCatalogSales(conn, s, rnd);
    loadCatalogReturns(conn, s, rnd);
    loadWebSales(conn, s, rnd);
    loadWebReturns(conn, s, rnd);
    loadInventory(conn, s, rnd);
    conn.commit();
  }

  private static void copyData(Connection conn, String from, String to) throws SQLException {
    try (Statement st = conn.createStatement()) {
      for (String t : TABLES) {
        st.executeUpdate("UPSERT INTO " + to + "." + t + " SELECT * FROM " + from + "." + t);
      }
    }
    conn.commit();
  }

  private static void loadDateDim(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".DATE_DIM (d_date_sk, d_date, d_year, d_moy, d_dom, d_qoy,"
      + " d_dow, d_week_seq, d_day_name, d_quarter_name) VALUES (?,?,?,?,?,?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      int sk = 0;
      for (int yi = 0; yi < YEARS.length; yi++) {
        int year = YEARS[yi];
        for (int moy = 1; moy <= 12; moy++) {
          sk++;
          int dom = 15;
          int qoy = (moy - 1) / 3 + 1;
          int dow = (sk % 7);
          int weekSeq = (year - 2000) * 52 + moy * 4;
          ps.setLong(1, sk);
          ps.setDate(2, java.sql.Date.valueOf(String.format("%04d-%02d-%02d", year, moy, dom)));
          ps.setInt(3, year);
          ps.setInt(4, moy);
          ps.setInt(5, dom);
          ps.setInt(6, qoy);
          ps.setInt(7, dow);
          ps.setInt(8, weekSeq);
          ps.setString(9, DAY_NAMES[dow]);
          ps.setString(10, year + "Q" + qoy);
          ps.executeUpdate();
        }
      }
    }
  }

  private static void loadTimeDim(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".TIME_DIM (t_time_sk, t_hour, t_minute, t_am_pm, t_shift,"
      + " t_meal_time) VALUES (?,?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_TIMES; i++) {
        int hour = (i * 2) % 24;
        ps.setLong(1, i);
        ps.setInt(2, hour);
        ps.setInt(3, 0);
        ps.setString(4, hour < 12 ? "AM" : "PM");
        ps.setString(5, hour < 8 ? "third" : hour < 16 ? "first" : "second");
        ps.setString(6,
          hour >= 6 && hour < 10 ? "breakfast"
            : hour >= 11 && hour < 14 ? "lunch"
            : hour >= 17 && hour < 21 ? "dinner"
            : null);
        ps.executeUpdate();
      }
    }
  }

  private static void loadItem(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".ITEM (i_item_sk, i_item_id, i_item_desc, i_current_price,"
      + " i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id,"
      + " i_manufact, i_manager_id, i_size, i_color, i_units, i_product_name)"
      + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    String[] sizes = { "small", "medium", "large", "petite", "economy", "N/A" };
    String[] colors = { "red", "blue", "green", "white", "black", "almond" };
    String[] units = { "Each", "Dozen", "Case", "Box", "Pallet" };
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_ITEMS; i++) {
        int catId = (i - 1) % CATEGORIES.length;
        int classId = (i - 1) % 5 + 1;
        int brandId = 1000 + ((i - 1) % 8);
        int manufactId = (i - 1) % 5 + 1;
        int managerId = (i - 1) % 10 + 1;
        ps.setLong(1, i);
        ps.setString(2, String.format("ITEM%010d", i));
        ps.setString(3, "Item description " + i);
        ps.setBigDecimal(4, bd(10 + (i % 90) + 0.99));
        ps.setInt(5, brandId);
        ps.setString(6, "brand#" + brandId);
        ps.setInt(7, classId);
        ps.setString(8, "class#" + classId);
        ps.setInt(9, catId + 1);
        ps.setString(10, CATEGORIES[catId]);
        ps.setInt(11, manufactId);
        ps.setString(12, "manufact#" + manufactId);
        ps.setInt(13, managerId);
        ps.setString(14, sizes[i % sizes.length]);
        ps.setString(15, colors[i % colors.length]);
        ps.setString(16, units[i % units.length]);
        ps.setString(17, "product#" + i);
        ps.executeUpdate();
      }
    }
  }

  private static void loadCustomerAddress(Connection conn, String s, Random rnd)
    throws SQLException {
    String sql = "UPSERT INTO " + s + ".CUSTOMER_ADDRESS (ca_address_sk, ca_city, ca_county,"
      + " ca_state, ca_zip, ca_country, ca_gmt_offset, ca_street_number, ca_street_name)"
      + " VALUES (?,?,?,?,?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_ADDR; i++) {
        String state = STATES[(i - 1) % STATES.length];
        ps.setLong(1, i);
        ps.setString(2, "City" + ((i - 1) % 7));
        ps.setString(3, "County" + ((i - 1) % 4));
        ps.setString(4, state);
        ps.setString(5, String.format("%05d", 10000 + i));
        ps.setString(6, "United States");
        ps.setBigDecimal(7, bd(GMT_OFFSETS[(i - 1) % GMT_OFFSETS.length]));
        ps.setString(8, Integer.toString(100 + i));
        ps.setString(9, "Street " + i);
        ps.executeUpdate();
      }
    }
  }

  private static void loadCustomerDemographics(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".CUSTOMER_DEMOGRAPHICS (cd_demo_sk, cd_gender,"
      + " cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating,"
      + " cd_dep_count, cd_dep_employed_count, cd_dep_college_count) VALUES (?,?,?,?,?,?,?,?,?)";
    String[] credit = { "Low Risk", "Good", "High Risk", "Unknown" };
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_CDEMO; i++) {
        ps.setLong(1, i);
        ps.setString(2, GENDERS[(i - 1) % GENDERS.length]);
        ps.setString(3, MARITAL[(i - 1) % MARITAL.length]);
        ps.setString(4, EDUCATION[(i - 1) % EDUCATION.length]);
        ps.setInt(5, 500 * (((i - 1) % 6) + 1));
        ps.setString(6, credit[(i - 1) % credit.length]);
        ps.setInt(7, (i - 1) % 4);
        ps.setInt(8, (i - 1) % 3);
        ps.setInt(9, (i - 1) % 2);
        ps.executeUpdate();
      }
    }
  }

  private static void loadIncomeBand(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".INCOME_BAND (ib_income_band_sk, ib_lower_bound,"
      + " ib_upper_bound) VALUES (?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= 10; i++) {
        ps.setLong(1, i);
        ps.setInt(2, (i - 1) * 10000);
        ps.setInt(3, i * 10000);
        ps.executeUpdate();
      }
    }
  }

  private static void loadHouseholdDemographics(Connection conn, String s, Random rnd)
    throws SQLException {
    String sql = "UPSERT INTO " + s + ".HOUSEHOLD_DEMOGRAPHICS (hd_demo_sk, hd_income_band_sk,"
      + " hd_buy_potential, hd_dep_count, hd_vehicle_count) VALUES (?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_HDEMO; i++) {
        ps.setLong(1, i);
        ps.setLong(2, ((i - 1) % 10) + 1);
        ps.setString(3, BUY_POTENTIAL[(i - 1) % BUY_POTENTIAL.length]);
        ps.setInt(4, (i - 1) % 5);
        ps.setInt(5, (i - 1) % 4);
        ps.executeUpdate();
      }
    }
  }

  private static void loadCustomer(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".CUSTOMER (c_customer_sk, c_customer_id, c_current_cdemo_sk,"
      + " c_current_hdemo_sk, c_current_addr_sk, c_first_name, c_last_name, c_birth_country,"
      + " c_birth_year, c_preferred_cust_flag, c_salutation, c_email_address)"
      + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
    String[] countries = { "UNITED STATES", "CANADA", "MEXICO", "BRAZIL" };
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_CUSTOMERS; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("CUST%012d", i));
        ps.setLong(3, ((i - 1) % N_CDEMO) + 1);
        ps.setLong(4, ((i - 1) % N_HDEMO) + 1);
        ps.setLong(5, ((i - 1) % N_ADDR) + 1);
        ps.setString(6, "First" + i);
        ps.setString(7, "Last" + i);
        ps.setString(8, countries[(i - 1) % countries.length]);
        ps.setInt(9, 1950 + (i % 40));
        ps.setString(10, (i % 2 == 0) ? "Y" : "N");
        ps.setString(11, (i % 2 == 0) ? "Mr." : "Ms.");
        ps.setString(12, "cust" + i + "@example.com");
        ps.executeUpdate();
      }
    }
  }

  private static void loadStore(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".STORE (s_store_sk, s_store_id, s_store_name, s_company_id,"
      + " s_company_name, s_state, s_zip, s_gmt_offset, s_city, s_county, s_manager,"
      + " s_number_employees) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_STORES; i++) {
        String state = STATES[(i - 1) % STATES.length];
        ps.setLong(1, i);
        ps.setString(2, String.format("STORE%010d", i));
        ps.setString(3, "Store " + i);
        ps.setInt(4, 1);
        ps.setString(5, "company#1");
        ps.setString(6, state);
        ps.setString(7, String.format("%05d", 20000 + i));
        ps.setBigDecimal(8, bd(GMT_OFFSETS[(i - 1) % GMT_OFFSETS.length]));
        ps.setString(9, "City" + ((i - 1) % 7));
        ps.setString(10, "County" + ((i - 1) % 4));
        ps.setString(11, "Manager" + i);
        ps.setInt(12, 100 + i * 10);
        ps.executeUpdate();
      }
    }
  }

  private static void loadCallCenter(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".CALL_CENTER (cc_call_center_sk, cc_call_center_id, cc_name,"
      + " cc_manager, cc_mkt_id, cc_class, cc_company) VALUES (?,?,?,?,?,?,?)";
    String[] classes = { "small", "medium", "large" };
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_CALL_CENTERS; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("CC%014d", i));
        ps.setString(3, "Call Center " + i);
        ps.setString(4, "CCManager" + i);
        ps.setInt(5, (i - 1) % 3 + 1);
        ps.setString(6, classes[(i - 1) % classes.length]);
        ps.setInt(7, 1);
        ps.executeUpdate();
      }
    }
  }

  private static void loadCatalogPage(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".CATALOG_PAGE (cp_catalog_page_sk, cp_catalog_page_id,"
      + " cp_catalog_number, cp_catalog_page_number, cp_department) VALUES (?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_CATALOG_PAGES; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("CP%014d", i));
        ps.setInt(3, (i - 1) % 3 + 1);
        ps.setInt(4, i);
        ps.setString(5, "DEPARTMENT");
        ps.executeUpdate();
      }
    }
  }

  private static void loadWebSite(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".WEB_SITE (web_site_sk, web_site_id, web_name,"
      + " web_company_id, web_company_name) VALUES (?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_WEB_SITES; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("WS%014d", i));
        ps.setString(3, "Web Site " + i);
        ps.setInt(4, 1);
        ps.setString(5, "company#1");
        ps.executeUpdate();
      }
    }
  }

  private static void loadWebPage(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".WEB_PAGE (wp_web_page_sk, wp_web_page_id, wp_char_count,"
      + " wp_type) VALUES (?,?,?,?)";
    String[] types = { "welcome", "protected", "feedback", "general", "ad", "order" };
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_WEB_PAGES; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("WP%014d", i));
        ps.setInt(3, 1000 + i * 100);
        ps.setString(4, types[(i - 1) % types.length]);
        ps.executeUpdate();
      }
    }
  }

  private static void loadWarehouse(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".WAREHOUSE (w_warehouse_sk, w_warehouse_id,"
      + " w_warehouse_name, w_state, w_country, w_gmt_offset) VALUES (?,?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_WAREHOUSES; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("WH%014d", i));
        ps.setString(3, "Warehouse " + i);
        ps.setString(4, STATES[(i - 1) % STATES.length]);
        ps.setString(5, "United States");
        ps.setBigDecimal(6, bd(GMT_OFFSETS[(i - 1) % GMT_OFFSETS.length]));
        ps.executeUpdate();
      }
    }
  }

  private static void loadShipMode(Connection conn, String s) throws SQLException {
    String sql = "UPSERT INTO " + s + ".SHIP_MODE (sm_ship_mode_sk, sm_ship_mode_id, sm_type,"
      + " sm_carrier) VALUES (?,?,?,?)";
    String[] types = { "EXPRESS", "NEXT DAY", "OVERNIGHT", "TWO DAY", "LIBRARY" };
    String[] carriers = { "DHL", "FEDEX", "UPS", "USPS", "ZHOU" };
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_SHIP_MODES; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("SM%014d", i));
        ps.setString(3, types[(i - 1) % types.length]);
        ps.setString(4, carriers[(i - 1) % carriers.length]);
        ps.executeUpdate();
      }
    }
  }

  private static void loadPromotion(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".PROMOTION (p_promo_sk, p_promo_id, p_channel_email,"
      + " p_channel_event, p_channel_tv, p_channel_dmail, p_channel_catalog) VALUES (?,?,?,?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_PROMOS; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("PROMO%010d", i));
        ps.setString(3, (i % 2 == 0) ? "Y" : "N");
        ps.setString(4, (i % 3 == 0) ? "Y" : "N");
        ps.setString(5, (i % 2 == 1) ? "Y" : "N");
        ps.setString(6, (i % 4 == 0) ? "Y" : "N");
        ps.setString(7, (i % 5 == 0) ? "Y" : "N");
        ps.executeUpdate();
      }
    }
  }

  private static void loadReason(Connection conn, String s) throws SQLException {
    String sql =
      "UPSERT INTO " + s + ".REASON (r_reason_sk, r_reason_id, r_reason_desc)" + " VALUES (?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 1; i <= N_REASONS; i++) {
        ps.setLong(1, i);
        ps.setString(2, String.format("REASON%010d", i));
        ps.setString(3, "Reason " + i);
        ps.executeUpdate();
      }
    }
  }

  private static void loadStoreSales(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".STORE_SALES (ss_item_sk, ss_ticket_number, ss_sold_date_sk,"
      + " ss_sold_time_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk,"
      + " ss_promo_sk, ss_quantity, ss_sales_price, ss_ext_sales_price, ss_ext_discount_amt,"
      + " ss_list_price, ss_ext_list_price, ss_coupon_amt, ss_net_profit, ss_net_paid)"
      + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    long ticket = 0;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int item = 1; item <= N_ITEMS; item++) {
        // Each item sold on a handful of dates, giving broad coverage of (year, moy).
        for (int dateSk = 1; dateSk <= N_DATES; dateSk += 2) {
          ticket++;
          int qty = 1 + rnd.nextInt(20);
          double salesPrice = 5 + rnd.nextInt(95) + 0.5;
          double listPrice = salesPrice + rnd.nextInt(20);
          long cust = 1 + ((item + dateSk) % N_CUSTOMERS);
          ps.setLong(1, item);
          ps.setLong(2, ticket);
          ps.setLong(3, dateSk);
          ps.setLong(4, 1 + (ticket % N_TIMES));
          ps.setLong(5, cust);
          ps.setLong(6, 1 + ((cust - 1) % N_CDEMO));
          ps.setLong(7, 1 + ((cust - 1) % N_HDEMO));
          ps.setLong(8, 1 + ((cust - 1) % N_ADDR));
          ps.setLong(9, 1 + (int) (ticket % N_STORES));
          ps.setLong(10, 1 + (int) (ticket % N_PROMOS));
          ps.setInt(11, qty);
          ps.setBigDecimal(12, bd(salesPrice));
          ps.setBigDecimal(13, bd(salesPrice * qty));
          ps.setBigDecimal(14, bd(rnd.nextInt(50)));
          ps.setBigDecimal(15, bd(listPrice));
          ps.setBigDecimal(16, bd(listPrice * qty));
          ps.setBigDecimal(17, bd(rnd.nextInt(20)));
          ps.setBigDecimal(18, bd((salesPrice - 3) * qty));
          ps.setBigDecimal(19, bd(salesPrice * qty));
          ps.executeUpdate();
        }
      }
    }
    conn.commit();
  }

  private static void loadStoreReturns(Connection conn, String s, Random rnd) throws SQLException {
    // Return roughly every 4th store_sales row, reusing its (item, ticket) so the natural join key
    // lines up with store_sales.
    String sql = "UPSERT INTO " + s + ".STORE_RETURNS (sr_item_sk, sr_ticket_number,"
      + " sr_returned_date_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk,"
      + " sr_reason_sk, sr_return_quantity, sr_return_amt, sr_net_loss, sr_fee)"
      + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
    long ticket = 0;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int item = 1; item <= N_ITEMS; item++) {
        for (int dateSk = 1; dateSk <= N_DATES; dateSk += 2) {
          ticket++;
          if (ticket % 2 != 0) {
            continue;
          }
          long cust = 1 + ((item + dateSk) % N_CUSTOMERS);
          int retDate = Math.min(N_DATES, dateSk + 1);
          ps.setLong(1, item);
          ps.setLong(2, ticket);
          ps.setLong(3, retDate);
          ps.setLong(4, cust);
          ps.setLong(5, 1 + ((cust - 1) % N_CDEMO));
          ps.setLong(6, 1 + ((cust - 1) % N_HDEMO));
          ps.setLong(7, 1 + ((cust - 1) % N_ADDR));
          ps.setLong(8, 1 + (int) (ticket % N_STORES));
          ps.setLong(9, 1 + (int) (ticket % N_REASONS));
          ps.setInt(10, 1 + rnd.nextInt(5));
          ps.setBigDecimal(11, bd(5 + rnd.nextInt(50)));
          ps.setBigDecimal(12, bd(rnd.nextInt(30)));
          ps.setBigDecimal(13, bd(rnd.nextInt(10)));
          ps.executeUpdate();
        }
      }
    }
    conn.commit();
  }

  private static void loadCatalogSales(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".CATALOG_SALES (cs_item_sk, cs_order_number,"
      + " cs_sold_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_ship_customer_sk,"
      + " cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_warehouse_sk, cs_ship_mode_sk,"
      + " cs_promo_sk, cs_quantity, cs_sales_price, cs_ext_sales_price, cs_ext_discount_amt,"
      + " cs_list_price, cs_ext_list_price, cs_coupon_amt, cs_net_profit, cs_net_paid)"
      + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    long order = 0;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int item = 1; item <= N_ITEMS; item++) {
        for (int dateSk = 2; dateSk <= N_DATES; dateSk += 3) {
          order++;
          int qty = 1 + rnd.nextInt(20);
          double salesPrice = 5 + rnd.nextInt(95) + 0.5;
          double listPrice = salesPrice + rnd.nextInt(20);
          long cust = 1 + ((item * 2 + dateSk) % N_CUSTOMERS);
          ps.setLong(1, item);
          ps.setLong(2, order);
          ps.setLong(3, dateSk);
          ps.setLong(4, cust);
          ps.setLong(5, 1 + ((cust - 1) % N_CDEMO));
          ps.setLong(6, 1 + (cust % N_CUSTOMERS));
          ps.setLong(7, 1 + ((cust - 1) % N_ADDR));
          ps.setLong(8, 1 + (int) (order % N_CALL_CENTERS));
          ps.setLong(9, 1 + (int) (order % N_CATALOG_PAGES));
          ps.setLong(10, 1 + (int) (order % N_WAREHOUSES));
          ps.setLong(11, 1 + (int) (order % N_SHIP_MODES));
          ps.setLong(12, 1 + (int) (order % N_PROMOS));
          ps.setInt(13, qty);
          ps.setBigDecimal(14, bd(salesPrice));
          ps.setBigDecimal(15, bd(salesPrice * qty));
          ps.setBigDecimal(16, bd(rnd.nextInt(50)));
          ps.setBigDecimal(17, bd(listPrice));
          ps.setBigDecimal(18, bd(listPrice * qty));
          ps.setBigDecimal(19, bd(rnd.nextInt(20)));
          ps.setBigDecimal(20, bd((salesPrice - 3) * qty));
          ps.setBigDecimal(21, bd(salesPrice * qty));
          ps.executeUpdate();
        }
      }
    }
    conn.commit();
  }

  private static void loadCatalogReturns(Connection conn, String s, Random rnd)
    throws SQLException {
    String sql = "UPSERT INTO " + s + ".CATALOG_RETURNS (cr_item_sk, cr_order_number,"
      + " cr_returned_date_sk, cr_returning_customer_sk, cr_call_center_sk, cr_catalog_page_sk,"
      + " cr_warehouse_sk, cr_reason_sk, cr_return_quantity, cr_return_amount, cr_net_loss)"
      + " VALUES (?,?,?,?,?,?,?,?,?,?,?)";
    long order = 0;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int item = 1; item <= N_ITEMS; item++) {
        for (int dateSk = 2; dateSk <= N_DATES; dateSk += 3) {
          order++;
          if (order % 2 != 0) {
            continue;
          }
          long cust = 1 + ((item * 2 + dateSk) % N_CUSTOMERS);
          int retDate = Math.min(N_DATES, dateSk + 1);
          ps.setLong(1, item);
          ps.setLong(2, order);
          ps.setLong(3, retDate);
          ps.setLong(4, cust);
          ps.setLong(5, 1 + (int) (order % N_CALL_CENTERS));
          ps.setLong(6, 1 + (int) (order % N_CATALOG_PAGES));
          ps.setLong(7, 1 + (int) (order % N_WAREHOUSES));
          ps.setLong(8, 1 + (int) (order % N_REASONS));
          ps.setInt(9, 1 + rnd.nextInt(5));
          ps.setBigDecimal(10, bd(5 + rnd.nextInt(50)));
          ps.setBigDecimal(11, bd(rnd.nextInt(10)));
          ps.executeUpdate();
        }
      }
    }
    conn.commit();
  }

  private static void loadWebSales(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".WEB_SALES (ws_item_sk, ws_order_number, ws_sold_date_sk,"
      + " ws_sold_time_sk, ws_bill_customer_sk, ws_ship_customer_sk, ws_ship_addr_sk,"
      + " ws_web_site_sk, ws_web_page_sk, ws_warehouse_sk, ws_ship_mode_sk, ws_promo_sk,"
      + " ws_quantity, ws_sales_price, ws_ext_sales_price, ws_ext_discount_amt, ws_list_price,"
      + " ws_ext_list_price, ws_coupon_amt, ws_net_profit, ws_net_paid)"
      + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    long order = 0;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int item = 1; item <= N_ITEMS; item++) {
        for (int dateSk = 3; dateSk <= N_DATES; dateSk += 3) {
          order++;
          int qty = 1 + rnd.nextInt(20);
          double salesPrice = 5 + rnd.nextInt(95) + 0.5;
          double listPrice = salesPrice + rnd.nextInt(20);
          long cust = 1 + ((item * 3 + dateSk) % N_CUSTOMERS);
          ps.setLong(1, item);
          ps.setLong(2, order);
          ps.setLong(3, dateSk);
          ps.setLong(4, 1 + (order % N_TIMES));
          ps.setLong(5, cust);
          ps.setLong(6, 1 + (cust % N_CUSTOMERS));
          ps.setLong(7, 1 + ((cust - 1) % N_ADDR));
          ps.setLong(8, 1 + (int) (order % N_WEB_SITES));
          ps.setLong(9, 1 + (int) (order % N_WEB_PAGES));
          ps.setLong(10, 1 + (int) (order % N_WAREHOUSES));
          ps.setLong(11, 1 + (int) (order % N_SHIP_MODES));
          ps.setLong(12, 1 + (int) (order % N_PROMOS));
          ps.setInt(13, qty);
          ps.setBigDecimal(14, bd(salesPrice));
          ps.setBigDecimal(15, bd(salesPrice * qty));
          ps.setBigDecimal(16, bd(rnd.nextInt(50)));
          ps.setBigDecimal(17, bd(listPrice));
          ps.setBigDecimal(18, bd(listPrice * qty));
          ps.setBigDecimal(19, bd(rnd.nextInt(20)));
          ps.setBigDecimal(20, bd((salesPrice - 3) * qty));
          ps.setBigDecimal(21, bd(salesPrice * qty));
          ps.executeUpdate();
        }
      }
    }
    conn.commit();
  }

  private static void loadWebReturns(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".WEB_RETURNS (wr_item_sk, wr_order_number,"
      + " wr_returned_date_sk, wr_returning_customer_sk, wr_web_page_sk, wr_reason_sk,"
      + " wr_return_quantity, wr_return_amt, wr_net_loss) VALUES (?,?,?,?,?,?,?,?,?)";
    long order = 0;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int item = 1; item <= N_ITEMS; item++) {
        for (int dateSk = 3; dateSk <= N_DATES; dateSk += 3) {
          order++;
          if (order % 2 != 0) {
            continue;
          }
          long cust = 1 + ((item * 3 + dateSk) % N_CUSTOMERS);
          int retDate = Math.min(N_DATES, dateSk + 1);
          ps.setLong(1, item);
          ps.setLong(2, order);
          ps.setLong(3, retDate);
          ps.setLong(4, cust);
          ps.setLong(5, 1 + (int) (order % N_WEB_PAGES));
          ps.setLong(6, 1 + (int) (order % N_REASONS));
          ps.setInt(7, 1 + rnd.nextInt(5));
          ps.setBigDecimal(8, bd(5 + rnd.nextInt(50)));
          ps.setBigDecimal(9, bd(rnd.nextInt(10)));
          ps.executeUpdate();
        }
      }
    }
    conn.commit();
  }

  private static void loadInventory(Connection conn, String s, Random rnd) throws SQLException {
    String sql = "UPSERT INTO " + s + ".INVENTORY (inv_date_sk, inv_item_sk, inv_warehouse_sk,"
      + " inv_quantity_on_hand) VALUES (?,?,?,?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      // Weekly-ish snapshots: every 3rd date, all items, all warehouses.
      for (int dateSk = 1; dateSk <= N_DATES; dateSk += 3) {
        for (int item = 1; item <= N_ITEMS; item++) {
          for (int wh = 1; wh <= N_WAREHOUSES; wh++) {
            ps.setLong(1, dateSk);
            ps.setLong(2, item);
            ps.setLong(3, wh);
            ps.setInt(4, 50 + rnd.nextInt(600));
            ps.executeUpdate();
          }
        }
      }
    }
    conn.commit();
  }

  private static java.math.BigDecimal bd(double v) {
    return new java.math.BigDecimal(v).setScale(2, java.math.RoundingMode.HALF_UP);
  }
}
