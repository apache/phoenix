/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import static org.apache.phoenix.util.PhoenixRuntime.CURRENT_SCN_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.BTABLE_NAME;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.CUSTOM_ENTITY_DATA_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID1;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID2;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID3;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID4;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID5;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID6;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID7;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID8;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID9;
import static org.apache.phoenix.util.TestUtil.ENTITY_HISTORY_SALTED_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.ENTITY_HISTORY_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.FUNKY_NAME;
import static org.apache.phoenix.util.TestUtil.GROUPBYTEST_NAME;
import static org.apache.phoenix.util.TestUtil.HBASE_DYNAMIC_COLUMNS;
import static org.apache.phoenix.util.TestUtil.HBASE_NATIVE;
import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.INDEX_DATA_TABLE;
import static org.apache.phoenix.util.TestUtil.JOIN_COITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.KEYONLY_NAME;
import static org.apache.phoenix.util.TestUtil.MDTEST_NAME;
import static org.apache.phoenix.util.TestUtil.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.MULTI_CF_NAME;
import static org.apache.phoenix.util.TestUtil.MUTABLE_INDEX_DATA_TABLE;
import static org.apache.phoenix.util.TestUtil.PARENTID1;
import static org.apache.phoenix.util.TestUtil.PARENTID2;
import static org.apache.phoenix.util.TestUtil.PARENTID3;
import static org.apache.phoenix.util.TestUtil.PARENTID4;
import static org.apache.phoenix.util.TestUtil.PARENTID5;
import static org.apache.phoenix.util.TestUtil.PARENTID6;
import static org.apache.phoenix.util.TestUtil.PARENTID7;
import static org.apache.phoenix.util.TestUtil.PARENTID8;
import static org.apache.phoenix.util.TestUtil.PARENTID9;
import static org.apache.phoenix.util.TestUtil.PRODUCT_METRICS_NAME;
import static org.apache.phoenix.util.TestUtil.PTSDB2_NAME;
import static org.apache.phoenix.util.TestUtil.PTSDB3_NAME;
import static org.apache.phoenix.util.TestUtil.PTSDB_NAME;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.STABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_ARRAY;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_SALTING;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.LocalIndexMerger;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ConfigUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 
 * Base class that contains all the methods needed by
 * client-time and hbase-time managed tests.
 * 
 * For tests needing connectivity to a cluster, please use
 * {@link BaseHBaseManagedTimeIT} or {@link BaseClientManagedTimeIT}. 
 * 
 * In the rare case when a test can't share the same mini cluster as the 
 * ones used by {@link BaseHBaseManagedTimeIT} or {@link BaseClientManagedTimeIT}
 * one could extend this class and spin up your own mini cluster. Please 
 * make sure to shutdown the mini cluster in a method annotated by @AfterClass.  
 *
 */
public abstract class BaseTest {
    private static final Map<String,String> tableDDLMap;
    private static final Logger logger = LoggerFactory.getLogger(BaseTest.class);

    static {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();
        builder.put(ENTITY_HISTORY_TABLE_NAME,"create table " + ENTITY_HISTORY_TABLE_NAME +
                "   (organization_id char(15) not null,\n" +
                "    parent_id char(15) not null,\n" +
                "    created_date date not null,\n" +
                "    entity_history_id char(15) not null,\n" +
                "    old_value varchar,\n" +
                "    new_value varchar,\n" + //create table shouldn't blow up if the last column definition ends with a comma.
                "    CONSTRAINT pk PRIMARY KEY (organization_id, parent_id, created_date, entity_history_id)\n" +
                ")");
        builder.put(ENTITY_HISTORY_SALTED_TABLE_NAME,"create table " + ENTITY_HISTORY_SALTED_TABLE_NAME +
                "   (organization_id char(15) not null,\n" +
                "    parent_id char(15) not null,\n" +
                "    created_date date not null,\n" +
                "    entity_history_id char(15) not null,\n" +
                "    old_value varchar,\n" +
                "    new_value varchar\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, parent_id, created_date, entity_history_id))\n" +
                "    SALT_BUCKETS = 4");
        builder.put(ATABLE_NAME,"create table " + ATABLE_NAME +
                "   (organization_id char(15) not null, \n" +
                "    entity_id char(15) not null,\n" +
                "    a_string varchar(100),\n" +
                "    b_string varchar(100),\n" +
                "    a_integer integer,\n" +
                "    a_date date,\n" +
                "    a_time time,\n" +
                "    a_timestamp timestamp,\n" +
                "    x_decimal decimal(31,10),\n" +
                "    x_long bigint,\n" +
                "    x_integer integer,\n" +
                "    y_integer integer,\n" +
                "    a_byte tinyint,\n" +
                "    a_short smallint,\n" +
                "    a_float float,\n" +
                "    a_double double,\n" +
                "    a_unsigned_float unsigned_float,\n" +
                "    a_unsigned_double unsigned_double\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n" +
                ") ");
        builder.put(TABLE_WITH_ARRAY, "create table "
				+ TABLE_WITH_ARRAY
				+ "   (organization_id char(15) not null, \n"
				+ "    entity_id char(15) not null,\n"
				+ "    a_string_array varchar(100) array[],\n"
				+ "    b_string varchar(100),\n"
				+ "    a_integer integer,\n"
				+ "    a_date date,\n"
				+ "    a_time time,\n"
				+ "    a_timestamp timestamp,\n"
				+ "    x_decimal decimal(31,10),\n"
				+ "    x_long_array bigint array[],\n"
				+ "    x_integer integer,\n"
				+ "    a_byte_array tinyint array[],\n"
				+ "    a_short smallint,\n"
				+ "    a_float float,\n"
				+ "    a_double_array double array[],\n"
				+ "    a_unsigned_float unsigned_float,\n"
				+ "    a_unsigned_double unsigned_double \n"
				+ "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n"
				+ ")");
        builder.put(BTABLE_NAME,"create table " + BTABLE_NAME +
                "   (a_string varchar not null, \n" +
                "    a_id char(3) not null,\n" +
                "    b_string varchar not null, \n" +
                "    a_integer integer not null, \n" +
                "    c_string varchar(2) null,\n" +
                "    b_integer integer,\n" +
                "    c_integer integer,\n" +
                "    d_string varchar(3),\n" +
                "    e_string char(10)\n" +
                "    CONSTRAINT my_pk PRIMARY KEY (a_string,a_id,b_string,a_integer,c_string))");
        builder.put(TABLE_WITH_SALTING,"create table " + TABLE_WITH_SALTING +
                "   (a_integer integer not null, \n" +
                "    a_string varchar not null, \n" +
                "    a_id char(3) not null,\n" +
                "    b_string varchar, \n" +
                "    b_integer integer \n" +
                "    CONSTRAINT pk PRIMARY KEY (a_integer, a_string, a_id))\n" +
                "    SALT_BUCKETS = 4");
        builder.put(STABLE_NAME,"create table " + STABLE_NAME +
                "   (id char(1) not null primary key,\n" +
                "    \"value\" integer)");
        builder.put(PTSDB_NAME,"create table " + PTSDB_NAME +
                "   (inst varchar null,\n" +
                "    host varchar null,\n" +
                "    date date not null,\n" +
                "    val decimal(31,10)\n" +
                "    CONSTRAINT pk PRIMARY KEY (inst, host, date))");
        builder.put(PTSDB2_NAME,"create table " + PTSDB2_NAME +
                "   (inst varchar(10) not null,\n" +
                "    date date not null,\n" +
                "    val1 decimal,\n" +
                "    val2 decimal(31,10),\n" +
                "    val3 decimal\n" +
                "    CONSTRAINT pk PRIMARY KEY (inst, date))");
        builder.put(PTSDB3_NAME,"create table " + PTSDB3_NAME +
                "   (host varchar(10) not null,\n" +
                "    date date not null,\n" +
                "    val1 decimal,\n" +
                "    val2 decimal(31,10),\n" +
                "    val3 decimal\n" +
                "    CONSTRAINT pk PRIMARY KEY (host DESC, date DESC))");
        builder.put(FUNKY_NAME,"create table " + FUNKY_NAME +
                "   (\"foo!\" varchar not null primary key,\n" +
                "    \"1\".\"#@$\" varchar, \n" +
                "    \"1\".\"foo.bar-bas\" varchar, \n" +
                "    \"1\".\"Value\" integer,\n" +
                "    \"1\".\"VALUE\" integer,\n" +
                "    \"1\".\"value\" integer,\n" +
                "    \"1\".\"_blah^\" varchar)"
                );
        builder.put(KEYONLY_NAME,"create table " + KEYONLY_NAME +
                "   (i1 integer not null, i2 integer not null\n" +
                "    CONSTRAINT pk PRIMARY KEY (i1,i2))");
        builder.put(MDTEST_NAME,"create table " + MDTEST_NAME +
                "   (id char(1) primary key,\n" +
                "    a.col1 integer,\n" +
                "    b.col2 bigint,\n" +
                "    b.col3 decimal,\n" +
                "    b.col4 decimal(5),\n" +
                "    b.col5 decimal(6,3))\n" +
                "    a." + HConstants.VERSIONS + "=" + 1 + "," + "a." + HColumnDescriptor.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE +  "'");
        builder.put(MULTI_CF_NAME,"create table " + MULTI_CF_NAME +
                "   (id char(15) not null primary key,\n" +
                "    a.unique_user_count integer,\n" +
                "    b.unique_org_count integer,\n" +
                "    c.db_cpu_utilization decimal(31,10),\n" +
                "    d.transaction_count bigint,\n" +
                "    e.cpu_utilization decimal(31,10),\n" +
                "    f.response_time bigint,\n" +
                "    g.response_time bigint)");
        builder.put(GROUPBYTEST_NAME,"create table " + GROUPBYTEST_NAME +
                "   (id varchar not null primary key,\n" +
                "    uri varchar, appcpu integer)");
        builder.put(HBASE_NATIVE,"create table " + HBASE_NATIVE +
                "   (uint_key unsigned_int not null," +
                "    ulong_key unsigned_long not null," +
                "    string_key varchar not null,\n" +
                "    \"1\".uint_col unsigned_int," +
                "    \"1\".ulong_col unsigned_long" +
                "    CONSTRAINT pk PRIMARY KEY (uint_key, ulong_key, string_key))\n" +
                     HColumnDescriptor.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE + "'");
        builder.put(HBASE_DYNAMIC_COLUMNS,"create table " + HBASE_DYNAMIC_COLUMNS + 
                "   (entry varchar not null," +
                "    F varchar," +
                "    A.F1v1 varchar," +
                "    A.F1v2 varchar," +
                "    B.F2v1 varchar" +
                "    CONSTRAINT pk PRIMARY KEY (entry))\n");
        builder.put(PRODUCT_METRICS_NAME,"create table " + PRODUCT_METRICS_NAME +
                "   (organization_id char(15) not null," +
                "    date date not null," +
                "    feature char(1) not null," +
                "    unique_users integer not null,\n" +
                "    db_utilization decimal(31,10),\n" +
                "    transactions bigint,\n" +
                "    cpu_utilization decimal(31,10),\n" +
                "    response_time bigint,\n" +
                "    io_time bigint,\n" +
                "    region varchar,\n" +
                "    unset_column decimal(31,10)\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, DATe, feature, UNIQUE_USERS))");
        builder.put(CUSTOM_ENTITY_DATA_FULL_NAME,"create table " + CUSTOM_ENTITY_DATA_FULL_NAME +
                "   (organization_id char(15) not null, \n" +
                "    key_prefix char(3) not null,\n" +
                "    custom_entity_data_id char(12) not null,\n" +
                "    created_by varchar,\n" +
                "    created_date date,\n" +
                "    currency_iso_code char(3),\n" +
                "    deleted char(1),\n" +
                "    division decimal(31,10),\n" +
                "    last_activity date,\n" +
                "    last_update date,\n" +
                "    last_update_by varchar,\n" +
                "    name varchar(240),\n" +
                "    owner varchar,\n" +
                "    record_type_id char(15),\n" +
                "    setup_owner varchar,\n" +
                "    system_modstamp date,\n" +
                "    b.val0 varchar,\n" +
                "    b.val1 varchar,\n" +
                "    b.val2 varchar,\n" +
                "    b.val3 varchar,\n" +
                "    b.val4 varchar,\n" +
                "    b.val5 varchar,\n" +
                "    b.val6 varchar,\n" +
                "    b.val7 varchar,\n" +
                "    b.val8 varchar,\n" +
                "    b.val9 varchar\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, key_prefix, custom_entity_data_id))");
        builder.put("IntKeyTest","create table IntKeyTest" +
                "   (i integer not null primary key)");
        builder.put("IntIntKeyTest","create table IntIntKeyTest" +
                "   (i integer not null primary key, j integer)");
        builder.put("LongInKeyTest","create table LongInKeyTest" +
                "   (l bigint not null primary key)");
        builder.put("PKIntValueTest", "create table PKIntValueTest" +
                "   (pk integer not null primary key)");
        builder.put("PKBigIntValueTest", "create table PKBigIntValueTest" +
                "   (pk bigint not null primary key)");
        builder.put("PKUnsignedIntValueTest", "create table PKUnsignedIntValueTest" +
                "   (pk unsigned_int not null primary key)");
        builder.put("PKUnsignedLongValueTest", "create table PKUnsignedLongValueTest" +
                "   (pk unsigned_long not null\n" +
                "    CONSTRAINT pk PRIMARY KEY (pk))");
        builder.put("KVIntValueTest", "create table KVIntValueTest" +
                "   (pk integer not null primary key,\n" +
                "    kv integer)\n");
        builder.put("KVBigIntValueTest", "create table KVBigIntValueTest" +
                "   (pk integer not null primary key,\n" +
                "    kv bigint)\n");
        builder.put(INDEX_DATA_TABLE, "create table " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + "(" +
                "   varchar_pk VARCHAR NOT NULL, " +
                "   char_pk CHAR(6) NOT NULL, " +
                "   int_pk INTEGER NOT NULL, "+ 
                "   long_pk BIGINT NOT NULL, " +
                "   decimal_pk DECIMAL(31, 10) NOT NULL, " +
                "   date_pk DATE NOT NULL, " +
                "   a.varchar_col1 VARCHAR, " +
                "   a.char_col1 CHAR(10), " +
                "   a.int_col1 INTEGER, " +
                "   a.long_col1 BIGINT, " +
                "   a.decimal_col1 DECIMAL(31, 10), " +
                "   a.date1 DATE, " +
                "   b.varchar_col2 VARCHAR, " +
                "   b.char_col2 CHAR(10), " +
                "   b.int_col2 INTEGER, " +
                "   b.long_col2 BIGINT, " +
                "   b.decimal_col2 DECIMAL(31, 10), " +
                "   b.date2 DATE " +
                "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk, date_pk)) " +
                "IMMUTABLE_ROWS=true");
        builder.put(MUTABLE_INDEX_DATA_TABLE, "create table " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + "(" +
                "   varchar_pk VARCHAR NOT NULL, " +
                "   char_pk CHAR(6) NOT NULL, " +
                "   int_pk INTEGER NOT NULL, "+ 
                "   long_pk BIGINT NOT NULL, " +
                "   decimal_pk DECIMAL(31, 10) NOT NULL, " +
                "   date_pk DATE NOT NULL, " +
                "   a.varchar_col1 VARCHAR, " +
                "   a.char_col1 CHAR(10), " +
                "   a.int_col1 INTEGER, " +
                "   a.long_col1 BIGINT, " +
                "   a.decimal_col1 DECIMAL(31, 10), " +
                "   a.date1 DATE, " +
                "   b.varchar_col2 VARCHAR, " +
                "   b.char_col2 CHAR(10), " +
                "   b.int_col2 INTEGER, " +
                "   b.long_col2 BIGINT, " +
                "   b.decimal_col2 DECIMAL(31, 10), " +
                "   b.date2 DATE " +
                "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk, date_pk)) "
                );
        builder.put("SumDoubleTest","create table SumDoubleTest" +
                "   (id varchar not null primary key, d DOUBLE, f FLOAT, ud UNSIGNED_DOUBLE, uf UNSIGNED_FLOAT, i integer, de decimal)");
        builder.put(JOIN_ORDER_TABLE_FULL_NAME, "create table " + JOIN_ORDER_TABLE_FULL_NAME +
                "   (\"order_id\" varchar(15) not null primary key, " +
                "    \"customer_id\" varchar(10), " +
                "    \"item_id\" varchar(10), " +
                "    price integer, " +
                "    quantity integer, " +
                "    date timestamp)");
        builder.put(JOIN_CUSTOMER_TABLE_FULL_NAME, "create table " + JOIN_CUSTOMER_TABLE_FULL_NAME +
                "   (\"customer_id\" varchar(10) not null primary key, " +
                "    name varchar, " +
                "    phone varchar(12), " +
                "    address varchar, " +
                "    loc_id varchar(5), " +
                "    date date)");
        builder.put(JOIN_ITEM_TABLE_FULL_NAME, "create table " + JOIN_ITEM_TABLE_FULL_NAME +
                "   (\"item_id\" varchar(10) not null primary key, " +
                "    name varchar, " +
                "    price integer, " +
                "    discount1 integer, " +
                "    discount2 integer, " +
                "    \"supplier_id\" varchar(10), " +
                "    description varchar)");
        builder.put(JOIN_SUPPLIER_TABLE_FULL_NAME, "create table " + JOIN_SUPPLIER_TABLE_FULL_NAME +
                "   (\"supplier_id\" varchar(10) not null primary key, " +
                "    name varchar, " +
                "    phone varchar(12), " +
                "    address varchar, " +
                "    loc_id varchar(5))");
        builder.put(JOIN_COITEM_TABLE_FULL_NAME, "create table " + JOIN_COITEM_TABLE_FULL_NAME +
                "   (item_id varchar(10) NOT NULL, " +
                "    item_name varchar NOT NULL, " +
                "    co_item_id varchar(10), " +
                "    co_item_name varchar " +
                "   CONSTRAINT pk PRIMARY KEY (item_id, item_name)) " +
                "   SALT_BUCKETS=4");
        tableDDLMap = builder.build();
    }
    
    private static final String ORG_ID = "00D300000000XHP";
    protected static int NUM_SLAVES_BASE = 1;
    private static final String DEFAULT_SERVER_RPC_CONTROLLER_FACTORY = ServerRpcControllerFactory.class.getName();
    private static final String DEFAULT_RPC_SCHEDULER_FACTORY = PhoenixRpcSchedulerFactory.class.getName();
    
    protected static String getZKClientPort(Configuration conf) {
        return conf.get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
    }
    
    private static String url;
    protected static PhoenixTestDriver driver;
    private static boolean clusterInitialized = false;
    private static HBaseTestingUtility utility;
    protected static final Configuration config = HBaseConfiguration.create(); 
    
    protected static String getUrl() {
        if (!clusterInitialized) {
            throw new IllegalStateException("Cluster must be initialized before attempting to get the URL");
        }
        return url;
    }
    
    protected static String checkClusterInitialized(ReadOnlyProps overrideProps) {
        if (!clusterInitialized) {
            url = setUpTestCluster(config, overrideProps);
            clusterInitialized = true;
        }
        return url;
    }
    
    /**
     * Set up the test hbase cluster.
     * @return url to be used by clients to connect to the cluster.
     */
    protected static String setUpTestCluster(@Nonnull Configuration conf, ReadOnlyProps overrideProps) {
        boolean isDistributedCluster = isDistributedClusterModeEnabled(conf);
        if (!isDistributedCluster) {
            return initMiniCluster(conf, overrideProps);
       } else {
            return initClusterDistributedMode(conf, overrideProps);
        }
    }
    
    protected static void destroyDriver() throws Exception {
        if (driver != null) {
            try {
                assertTrue(destroyDriver(driver));
            } finally {
                driver = null;
            }
        }
    }
    
    protected static void dropNonSystemTables() throws Exception {
        try {
            disableAndDropNonSystemTables();
        } finally {
            destroyDriver();
        }
    }

    protected static void tearDownMiniCluster() throws Exception {
        try {
            destroyDriver();
        } finally {
            try {
                if (utility != null) {
                    utility.shutdownMiniCluster();
                }
            } finally {
                utility = null;
                clusterInitialized = false;
            }
        }
    }
            
    protected static void setUpTestDriver(ReadOnlyProps props) throws Exception {
        setUpTestDriver(props, props);
    }
    
    protected static void setUpTestDriver(ReadOnlyProps serverProps, ReadOnlyProps clientProps) throws Exception {
        String url = checkClusterInitialized(serverProps);
        if (driver == null) {
            driver = initAndRegisterDriver(url, clientProps);
        }
    }

    private static boolean isDistributedClusterModeEnabled(Configuration conf) {
        boolean isDistributedCluster = false;
        //check if the distributed mode was specified as a system property.
        isDistributedCluster = Boolean.parseBoolean(System.getProperty(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER, "false"));
        if (!isDistributedCluster) {
           //fall back on hbase-default.xml or hbase-site.xml to check for distributed mode              
           isDistributedCluster = conf.getBoolean(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER, false);
        }
        return isDistributedCluster;
    }
    
    /**
     * Initialize the mini cluster using phoenix-test specific configuration.
     * @param overrideProps TODO
     * @return url to be used by clients to connect to the mini cluster.
     */
    private static String initMiniCluster(Configuration conf, ReadOnlyProps overrideProps) {
        setUpConfigForMiniCluster(conf, overrideProps);
        utility = new HBaseTestingUtility(conf);
        try {
            utility.startMiniCluster(NUM_SLAVES_BASE);
            // add shutdown hook to kill the mini cluster
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        if (utility != null) utility.shutdownMiniCluster();
                    } catch (Exception e) {
                        logger.warn("Exception caught when shutting down mini cluster", e);
                    }
                }
            });
            return getLocalClusterUrl(utility);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    protected static String getLocalClusterUrl(HBaseTestingUtility util) throws Exception {
        String url = QueryUtil.getConnectionUrl(new Properties(), util.getConfiguration());
        return url + PHOENIX_TEST_DRIVER_URL_PARAM;
    }
    
    /**
     * Initialize the cluster in distributed mode
     * @param overrideProps TODO
     * @return url to be used by clients to connect to the mini cluster.
     */
    private static String initClusterDistributedMode(Configuration conf, ReadOnlyProps overrideProps) {
        setTestConfigForDistribuedCluster(conf, overrideProps);
        try {
            IntegrationTestingUtility util =  new IntegrationTestingUtility(conf);
            utility = util;
            util.initializeCluster(NUM_SLAVES_BASE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return JDBC_PROTOCOL + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    }

    private static void setTestConfigForDistribuedCluster(Configuration conf, ReadOnlyProps overrideProps) {
        setDefaultTestConfig(conf, overrideProps);
    }
    
    private static void setDefaultTestConfig(Configuration conf, ReadOnlyProps overrideProps) {
        ConfigUtil.setReplicationConfigIfAbsent(conf);
        QueryServices services = new PhoenixTestDriver().getQueryServices();
        for (Entry<String,String> entry : services.getProps()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        //no point doing sanity checks when running tests.
        conf.setBoolean("hbase.table.sanity.checks", false);
        // set the server rpc controller and rpc scheduler factory, used to configure the cluster
        conf.set(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY, DEFAULT_SERVER_RPC_CONTROLLER_FACTORY);
        conf.set(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS, DEFAULT_RPC_SCHEDULER_FACTORY);
        
        // override any defaults based on overrideProps
        for (Entry<String,String> entry : overrideProps) {
            conf.set(entry.getKey(), entry.getValue());
        }
    }
    
    public static Configuration setUpConfigForMiniCluster(Configuration conf) {
        return setUpConfigForMiniCluster(conf, ReadOnlyProps.EMPTY_PROPS);
    }
    
    public static Configuration setUpConfigForMiniCluster(Configuration conf, ReadOnlyProps overrideProps) {
        assertNotNull(conf);
        setDefaultTestConfig(conf, overrideProps);
        /*
         * The default configuration of mini cluster ends up spawning a lot of threads
         * that are not really needed by phoenix for test purposes. Limiting these threads
         * helps us in running several mini clusters at the same time without hitting 
         * the threads limit imposed by the OS. 
         */
        conf.setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 5);
        conf.setInt("hbase.regionserver.metahandler.count", 2);
        conf.setInt(HConstants.MASTER_HANDLER_COUNT, 2);
        conf.setClass("hbase.coprocessor.regionserver.classes", LocalIndexMerger.class,
            RegionServerObserver.class);
        conf.setInt("dfs.namenode.handler.count", 2);
        conf.setInt("dfs.namenode.service.handler.count", 2);
        conf.setInt("dfs.datanode.handler.count", 2);
        conf.setInt("ipc.server.read.threadpool.size", 2);
        conf.setInt("ipc.server.handler.threadpool.size", 2);
        conf.setInt("hbase.hconnection.threads.max", 2);
        conf.setInt("hbase.hconnection.threads.core", 2);
        conf.setInt("hbase.htable.threads.max", 2);
        conf.setInt("hbase.regionserver.hlog.syncer.count", 2);
        conf.setInt("hbase.hlog.asyncer.number", 2);
        conf.setInt("hbase.assignment.zkevent.workers", 5);
        conf.setInt("hbase.assignment.threads.max", 5);
        conf.setInt("hbase.catalogjanitor.interval", 5000);
        return conf;
    }

    /**
     * Create a {@link PhoenixTestDriver} and register it.
     * @return an initialized and registered {@link PhoenixTestDriver} 
     */
    protected static PhoenixTestDriver initAndRegisterDriver(String url, ReadOnlyProps props) throws Exception {
        PhoenixTestDriver newDriver = new PhoenixTestDriver(props);
        DriverManager.registerDriver(newDriver);
        Driver oldDriver = DriverManager.getDriver(url); 
        if (oldDriver != newDriver) {
            destroyDriver(oldDriver);
        }
        Connection conn = newDriver.connect(url, PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.close();
        return newDriver;
    }
    
    //Close and unregister the driver.
    protected static boolean destroyDriver(Driver driver) {
        if (driver != null) {
            assert(driver instanceof PhoenixEmbeddedDriver);
            PhoenixEmbeddedDriver pdriver = (PhoenixEmbeddedDriver)driver;
            try {
                try {
                    pdriver.close();
                    return true;
                } finally {
                    DriverManager.deregisterDriver(driver);
                }
            } catch (Exception e) {
                logger.warn("Unable to close registered driver: " + driver, e);
            }
        }
        return false;
    }
    
    protected static String getOrganizationId() {
        return ORG_ID;
    }

    private static long timestamp;

    public static long nextTimestamp() {
        timestamp += 100;
        return timestamp;
    }

    protected static void ensureTableCreated(String url, String tableName) throws SQLException {
        ensureTableCreated(url, tableName, null, null);
    }

    protected static void ensureTableCreated(String url, String tableName, byte[][] splits) throws SQLException {
        ensureTableCreated(url, tableName, splits, null);
    }

    protected static void ensureTableCreated(String url, String tableName, Long ts) throws SQLException {
        ensureTableCreated(url, tableName, null, ts);
    }

    protected static void ensureTableCreated(String url, String tableName, byte[][] splits, Long ts) throws SQLException {
        String ddl = tableDDLMap.get(tableName);
        createTestTable(url, ddl, splits, ts);
    }

    protected static void createTestTable(String url, String ddl) throws SQLException {
        createTestTable(url, ddl, null, null);
    }

    protected static void createTestTable(String url, String ddl, byte[][] splits, Long ts) throws SQLException {
        createTestTable(url, ddl, splits, ts, true);
    }
    
    protected static void createTestTable(String url, String ddl, byte[][] splits, Long ts, boolean swallowTableAlreadyExistsException) throws SQLException {
        assertNotNull(ddl);
        StringBuilder buf = new StringBuilder(ddl);
        if (splits != null) {
            buf.append(" SPLIT ON (");
            for (int i = 0; i < splits.length; i++) {
                buf.append("?,");
            }
            buf.setCharAt(buf.length()-1, ')');
        }
        ddl = buf.toString();
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        }
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement stmt = conn.prepareStatement(ddl);
            if (splits != null) {
                for (int i = 0; i < splits.length; i++) {
                    stmt.setBytes(i+1, splits[i]);
                }
            }
            stmt.execute(ddl);
        } catch (TableAlreadyExistsException e) {
            if (! swallowTableAlreadyExistsException) {
                throw e;
            }
        } finally {
            conn.close();
        }
    }
    
    protected static byte[][] getDefaultSplits(String tenantId) {
        return new byte[][] { 
            Bytes.toBytes(tenantId + "00A"),
            Bytes.toBytes(tenantId + "00B"),
            Bytes.toBytes(tenantId + "00C"),
            };
    }
    
    protected static void deletePriorTables(long ts, String url) throws Exception {
        deletePriorTables(ts, (String)null, url);
    }
    
    protected static void deletePriorTables(long ts, String tenantId, String url) throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1024));
        if (ts != HConstants.LATEST_TIMESTAMP) {
            props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(ts));
        }
        Connection conn = DriverManager.getConnection(url, props);
        try {
            deletePriorTables(ts, conn, url);
            deletePriorSequences(ts, conn);
        }
        finally {
            conn.close();
        }
    }
    
    private static void deletePriorTables(long ts, Connection globalConn, String url) throws Exception {
        DatabaseMetaData dbmd = globalConn.getMetaData();
        // Drop VIEWs first, as we don't allow a TABLE with views to be dropped
        // Tables are sorted by TENANT_ID
        List<String[]> tableTypesList = Arrays.asList(new String[] {PTableType.VIEW.toString()}, new String[] {PTableType.TABLE.toString()});
        for (String[] tableTypes: tableTypesList) {
            ResultSet rs = dbmd.getTables(null, null, null, tableTypes);
            String lastTenantId = null;
            Connection conn = globalConn;
            while (rs.next()) {
                String fullTableName = SchemaUtil.getEscapedTableName(
                        rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM),
                        rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
                String ddl = "DROP " + rs.getString(PhoenixDatabaseMetaData.TABLE_TYPE) + " " + fullTableName;
                String tenantId = rs.getString(1);
                if (tenantId != null && !tenantId.equals(lastTenantId))  {
                    if (lastTenantId != null) {
                        conn.close();
                    }
                    // Open tenant-specific connection when we find a new one
                    Properties props = new Properties(globalConn.getClientInfo());
                    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                    conn = DriverManager.getConnection(url, props);
                    lastTenantId = tenantId;
                }
                try {
                    conn.createStatement().executeUpdate(ddl);
                } catch (NewerTableAlreadyExistsException ex) {
                    logger.info("Newer table " + fullTableName + " or its delete marker exists. Ignore current deletion");
                } catch (TableNotFoundException ex) {
                    logger.info("Table " + fullTableName + " is already deleted.");
                }
            }
            if (lastTenantId != null) {
                conn.close();
            }
        }
    }
    
    private static void deletePriorSequences(long ts, Connection globalConn) throws Exception {
        // TODO: drop tenant-specific sequences too
        ResultSet rs = globalConn.createStatement().executeQuery("SELECT " 
                + PhoenixDatabaseMetaData.TENANT_ID + ","
                + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + "," 
                + PhoenixDatabaseMetaData.SEQUENCE_NAME 
                + " FROM " + PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_ESCAPED);
        String lastTenantId = null;
        Connection conn = globalConn;
        while (rs.next()) {
            String tenantId = rs.getString(1);
            if (tenantId != null && !tenantId.equals(lastTenantId))  {
                if (lastTenantId != null) {
                    conn.close();
                }
                // Open tenant-specific connection when we find a new one
                Properties props = new Properties(globalConn.getClientInfo());
                props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                conn = DriverManager.getConnection(url, props);
                lastTenantId = tenantId;
            }

            logger.info("DROP SEQUENCE STATEMENT: DROP SEQUENCE " + SchemaUtil.getEscapedTableName(rs.getString(2), rs.getString(3)));
            conn.createStatement().execute("DROP SEQUENCE " + SchemaUtil.getEscapedTableName(rs.getString(2), rs.getString(3)));
        }
    }
    
    protected static void initSumDoubleValues(byte[][] splits, String url) throws Exception {
        ensureTableCreated(url, "SumDoubleTest", splits);
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(url, props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                    "SumDoubleTest(" +
                    "    id, " +
                    "    d, " +
                    "    f, " +
                    "    ud, " +
                    "    uf) " +
                    "VALUES (?, ?, ?, ?, ?)");
            stmt.setString(1, "1");
            stmt.setDouble(2, 0.001);
            stmt.setFloat(3, 0.01f);
            stmt.setDouble(4, 0.001);
            stmt.setFloat(5, 0.01f);
            stmt.execute();
                
            stmt.setString(1, "2");
            stmt.setDouble(2, 0.002);
            stmt.setFloat(3, 0.02f);
            stmt.setDouble(4, 0.002);
            stmt.setFloat(5, 0.02f);
            stmt.execute();
                
            stmt.setString(1, "3");
            stmt.setDouble(2, 0.003);
            stmt.setFloat(3, 0.03f);
            stmt.setDouble(4, 0.003);
            stmt.setFloat(5, 0.03f);
            stmt.execute();
                
            stmt.setString(1, "4");
            stmt.setDouble(2, 0.004);
            stmt.setFloat(3, 0.04f);
            stmt.setDouble(4, 0.004);
            stmt.setFloat(5, 0.04f);
            stmt.execute();
                
            stmt.setString(1, "5");
            stmt.setDouble(2, 0.005);
            stmt.setFloat(3, 0.05f);
            stmt.setDouble(4, 0.005);
            stmt.setFloat(5, 0.05f);
            stmt.execute();
                
            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits, String url) throws Exception {
        initATableValues(tenantId, splits, null, url);
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits, Date date, String url) throws Exception {
        initATableValues(tenantId, splits, date, null, url);
    }
    
    
    
    protected static void initATableValues(String tenantId, byte[][] splits, Date date, Long ts, String url) throws Exception {
        if (ts == null) {
            ensureTableCreated(url, ATABLE_NAME, splits);
        } else {
            ensureTableCreated(url, ATABLE_NAME, splits, ts-5);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(ts-3));
        }
        Connection conn = DriverManager.getConnection(url, props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                    "ATABLE(" +
                    "    ORGANIZATION_ID, " +
                    "    ENTITY_ID, " +
                    "    A_STRING, " +
                    "    B_STRING, " +
                    "    A_INTEGER, " +
                    "    A_DATE, " +
                    "    X_DECIMAL, " +
                    "    X_LONG, " +
                    "    X_INTEGER," +
                    "    Y_INTEGER," +
                    "    A_BYTE," +
                    "    A_SHORT," +
                    "    A_FLOAT," +
                    "    A_DOUBLE," +
                    "    A_UNSIGNED_FLOAT," +
                    "    A_UNSIGNED_DOUBLE)" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 1);
            stmt.setDate(6, date);
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)1);
            stmt.setShort(12, (short) 128);
            stmt.setFloat(13, 0.01f);
            stmt.setDouble(14, 0.0001);
            stmt.setFloat(15, 0.01f);
            stmt.setDouble(16, 0.0001);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW2);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, C_VALUE);
            stmt.setInt(5, 2);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)2);
            stmt.setShort(12, (short) 129);
            stmt.setFloat(13, 0.02f);
            stmt.setDouble(14, 0.0002);
            stmt.setFloat(15, 0.02f);
            stmt.setDouble(16, 0.0002);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW3);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, E_VALUE);
            stmt.setInt(5, 3);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)3);
            stmt.setShort(12, (short) 130);
            stmt.setFloat(13, 0.03f);
            stmt.setDouble(14, 0.0003);
            stmt.setFloat(15, 0.03f);
            stmt.setDouble(16, 0.0003);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW4);
            stmt.setString(3, A_VALUE);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 4);
            stmt.setDate(6, date == null ? null : date);
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)4);
            stmt.setShort(12, (short) 131);
            stmt.setFloat(13, 0.04f);
            stmt.setDouble(14, 0.0004);
            stmt.setFloat(15, 0.04f);
            stmt.setDouble(16, 0.0004);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW5);
            stmt.setString(3, B_VALUE);
            stmt.setString(4, C_VALUE);
            stmt.setInt(5, 5);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)5);
            stmt.setShort(12, (short) 132);
            stmt.setFloat(13, 0.05f);
            stmt.setDouble(14, 0.0005);
            stmt.setFloat(15, 0.05f);
            stmt.setDouble(16, 0.0005);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW6);
            stmt.setString(3, B_VALUE);
            stmt.setString(4, E_VALUE);
            stmt.setInt(5, 6);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
            stmt.setBigDecimal(7, null);
            stmt.setNull(8, Types.BIGINT);
            stmt.setNull(9, Types.INTEGER);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)6);
            stmt.setShort(12, (short) 133);
            stmt.setFloat(13, 0.06f);
            stmt.setDouble(14, 0.0006);
            stmt.setFloat(15, 0.06f);
            stmt.setDouble(16, 0.0006);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW7);
            stmt.setString(3, B_VALUE);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 7);
            stmt.setDate(6, date == null ? null : date);
            stmt.setBigDecimal(7, BigDecimal.valueOf(0.1));
            stmt.setLong(8, 5L);
            stmt.setInt(9, 5);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)7);
            stmt.setShort(12, (short) 134);
            stmt.setFloat(13, 0.07f);
            stmt.setDouble(14, 0.0007);
            stmt.setFloat(15, 0.07f);
            stmt.setDouble(16, 0.0007);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW8);
            stmt.setString(3, B_VALUE);
            stmt.setString(4, C_VALUE);
            stmt.setInt(5, 8);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 1));
            stmt.setBigDecimal(7, BigDecimal.valueOf(3.9));
            long l = Integer.MIN_VALUE - 1L;
            assert(l < Integer.MIN_VALUE);
            stmt.setLong(8, l);
            stmt.setInt(9, 4);
            stmt.setNull(10, Types.INTEGER);
            stmt.setByte(11, (byte)8);
            stmt.setShort(12, (short) 135);
            stmt.setFloat(13, 0.08f);
            stmt.setDouble(14, 0.0008);
            stmt.setFloat(15, 0.08f);
            stmt.setDouble(16, 0.0008);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW9);
            stmt.setString(3, C_VALUE);
            stmt.setString(4, E_VALUE);
            stmt.setInt(5, 9);
            stmt.setDate(6, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY * 2));
            stmt.setBigDecimal(7, BigDecimal.valueOf(3.3));
            l = Integer.MAX_VALUE + 1L;
            assert(l > Integer.MAX_VALUE);
            stmt.setLong(8, l);
            stmt.setInt(9, 3);
            stmt.setInt(10, 300);
            stmt.setByte(11, (byte)9);
            stmt.setShort(12, (short) 0);
            stmt.setFloat(13, 0.09f);
            stmt.setDouble(14, 0.0009);
            stmt.setFloat(15, 0.09f);
            stmt.setDouble(16, 0.0009);
            stmt.execute();
                
            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        initATableValues(tenantId, splits, date, ts, getUrl());
    }
    
    protected static void initEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        initEntityHistoryTableValues(tenantId, splits, date, ts, getUrl());
    }
    
    protected static void initSaltedEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        initSaltedEntityHistoryTableValues(tenantId, splits, date, ts, getUrl());
    }
        
    protected static void initEntityHistoryTableValues(String tenantId, byte[][] splits, String url) throws Exception {
        initEntityHistoryTableValues(tenantId, splits, null, null, url);
    }
    
    protected static void initEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, String url) throws Exception {
        initEntityHistoryTableValues(tenantId, splits, date, null, url);
    }
    
    private static void initEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts, String url) throws Exception {
        if (ts == null) {
            ensureTableCreated(url, ENTITY_HISTORY_TABLE_NAME, splits);
        } else {
            ensureTableCreated(url, ENTITY_HISTORY_TABLE_NAME, splits, ts-2);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(url, props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                    ENTITY_HISTORY_TABLE_NAME+
                    "(" +
                    "    ORGANIZATION_ID, " +
                    "    PARENT_ID, " +
                    "    CREATED_DATE, " +
                    "    ENTITY_HISTORY_ID, " +
                    "    OLD_VALUE, " +
                    "    NEW_VALUE) " +
                    "VALUES (?, ?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID1);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID1);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID2);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID2);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
                
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID3);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID3);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID4);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID4);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID5);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID5);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID6);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID6);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID7);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID7);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID8);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID8);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID9);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID9);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    protected static void initSaltedEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts, String url) throws Exception {
        if (ts == null) {
            ensureTableCreated(url, ENTITY_HISTORY_SALTED_TABLE_NAME, splits);
        } else {
            ensureTableCreated(url, ENTITY_HISTORY_SALTED_TABLE_NAME, splits, ts-2);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(url, props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                    ENTITY_HISTORY_SALTED_TABLE_NAME+
                    "(" +
                    "    ORGANIZATION_ID, " +
                    "    PARENT_ID, " +
                    "    CREATED_DATE, " +
                    "    ENTITY_HISTORY_ID, " +
                    "    OLD_VALUE, " +
                    "    NEW_VALUE) " +
                    "VALUES (?, ?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID1);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID1);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
                
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID2);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID2);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
                
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID3);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID3);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID4);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID4);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID5);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID5);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID6);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID6);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID7);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID7);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID8);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID8);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            stmt.setString(1, tenantId);
            stmt.setString(2, PARENTID9);
            stmt.setDate(3, date);
            stmt.setString(4, ENTITYHISTID9);
            stmt.setString(5,  A_VALUE);
            stmt.setString(6,  B_VALUE);
            stmt.execute();
            
            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    protected static void initJoinTableValues(String url, byte[][] splits, Long ts) throws Exception {
        if (ts == null) {
            ensureTableCreated(url, JOIN_CUSTOMER_TABLE_FULL_NAME, splits);
            ensureTableCreated(url, JOIN_ITEM_TABLE_FULL_NAME, splits);
            ensureTableCreated(url, JOIN_SUPPLIER_TABLE_FULL_NAME, splits);
            ensureTableCreated(url, JOIN_ORDER_TABLE_FULL_NAME, splits);
        } else {
            ensureTableCreated(url, JOIN_CUSTOMER_TABLE_FULL_NAME, splits, ts - 2);
            ensureTableCreated(url, JOIN_ITEM_TABLE_FULL_NAME, splits, ts - 2);
            ensureTableCreated(url, JOIN_SUPPLIER_TABLE_FULL_NAME, splits, ts - 2);
            ensureTableCreated(url, JOIN_ORDER_TABLE_FULL_NAME, splits, ts - 2);
        }
        
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute("CREATE SEQUENCE my.seq");
            // Insert into customer table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + JOIN_CUSTOMER_TABLE_FULL_NAME +
                    "   (\"customer_id\", " +
                    "    NAME, " +
                    "    PHONE, " +
                    "    ADDRESS, " +
                    "    LOC_ID, " +
                    "    DATE) " +
                    "values (?, ?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "C1");
            stmt.setString(3, "999-999-1111");
            stmt.setString(4, "101 XXX Street");
            stmt.setString(5, "10001");
            stmt.setDate(6, new Date(format.parse("2013-11-01 10:20:36").getTime()));
            stmt.execute();
                
            stmt.setString(1, "0000000002");
            stmt.setString(2, "C2");
            stmt.setString(3, "999-999-2222");
            stmt.setString(4, "202 XXX Street");
            stmt.setString(5, null);
            stmt.setDate(6, new Date(format.parse("2013-11-25 16:45:07").getTime()));
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "C3");
            stmt.setString(3, "999-999-3333");
            stmt.setString(4, "303 XXX Street");
            stmt.setString(5, null);
            stmt.setDate(6, new Date(format.parse("2013-11-25 10:06:29").getTime()));
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "C4");
            stmt.setString(3, "999-999-4444");
            stmt.setString(4, "404 XXX Street");
            stmt.setString(5, "10004");
            stmt.setDate(6, new Date(format.parse("2013-11-22 14:22:56").getTime()));
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "C5");
            stmt.setString(3, "999-999-5555");
            stmt.setString(4, "505 XXX Street");
            stmt.setString(5, "10005");
            stmt.setDate(6, new Date(format.parse("2013-11-27 09:37:50").getTime()));
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "C6");
            stmt.setString(3, "999-999-6666");
            stmt.setString(4, "606 XXX Street");
            stmt.setString(5, "10001");
            stmt.setDate(6, new Date(format.parse("2013-11-01 10:20:36").getTime()));
            stmt.execute();
            
            // Insert into item table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_ITEM_TABLE_FULL_NAME +
                    "   (\"item_id\", " +
                    "    NAME, " +
                    "    PRICE, " +
                    "    DISCOUNT1, " +
                    "    DISCOUNT2, " +
                    "    \"supplier_id\", " +
                    "    DESCRIPTION) " +
                    "values (?, ?, ?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "T1");
            stmt.setInt(3, 100);
            stmt.setInt(4, 5);
            stmt.setInt(5, 10);
            stmt.setString(6, "0000000001");
            stmt.setString(7, "Item T1");
            stmt.execute();

            stmt.setString(1, "0000000002");
            stmt.setString(2, "T2");
            stmt.setInt(3, 200);
            stmt.setInt(4, 5);
            stmt.setInt(5, 8);
            stmt.setString(6, "0000000001");
            stmt.setString(7, "Item T2");
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "T3");
            stmt.setInt(3, 300);
            stmt.setInt(4, 8);
            stmt.setInt(5, 12);
            stmt.setString(6, "0000000002");
            stmt.setString(7, "Item T3");
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "T4");
            stmt.setInt(3, 400);
            stmt.setInt(4, 6);
            stmt.setInt(5, 10);
            stmt.setString(6, "0000000002");
            stmt.setString(7, "Item T4");
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "T5");
            stmt.setInt(3, 500);
            stmt.setInt(4, 8);
            stmt.setInt(5, 15);
            stmt.setString(6, "0000000005");
            stmt.setString(7, "Item T5");
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "T6");
            stmt.setInt(3, 600);
            stmt.setInt(4, 8);
            stmt.setInt(5, 15);
            stmt.setString(6, "0000000006");
            stmt.setString(7, "Item T6");
            stmt.execute();
            
            stmt.setString(1, "invalid001");
            stmt.setString(2, "INVALID-1");
            stmt.setInt(3, 0);
            stmt.setInt(4, 0);
            stmt.setInt(5, 0);
            stmt.setString(6, "0000000000");
            stmt.setString(7, "Invalid item for join test");
            stmt.execute();

            // Insert into supplier table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_SUPPLIER_TABLE_FULL_NAME +
                    "   (\"supplier_id\", " +
                    "    NAME, " +
                    "    PHONE, " +
                    "    ADDRESS, " +
                    "    LOC_ID) " +
                    "values (?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "S1");
            stmt.setString(3, "888-888-1111");
            stmt.setString(4, "101 YYY Street");
            stmt.setString(5, "10001");
            stmt.execute();
                
            stmt.setString(1, "0000000002");
            stmt.setString(2, "S2");
            stmt.setString(3, "888-888-2222");
            stmt.setString(4, "202 YYY Street");
            stmt.setString(5, "10002");
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "S3");
            stmt.setString(3, "888-888-3333");
            stmt.setString(4, "303 YYY Street");
            stmt.setString(5, null);
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "S4");
            stmt.setString(3, "888-888-4444");
            stmt.setString(4, "404 YYY Street");
            stmt.setString(5, null);
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "S5");
            stmt.setString(3, "888-888-5555");
            stmt.setString(4, "505 YYY Street");
            stmt.setString(5, "10005");
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "S6");
            stmt.setString(3, "888-888-6666");
            stmt.setString(4, "606 YYY Street");
            stmt.setString(5, "10006");
            stmt.execute();

            // Insert into order table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_ORDER_TABLE_FULL_NAME +
                    "   (\"order_id\", " +
                    "    \"customer_id\", " +
                    "    \"item_id\", " +
                    "    PRICE, " +
                    "    QUANTITY," +
                    "    DATE) " +
                    "values (?, ?, ?, ?, ?, ?)");
            stmt.setString(1, "000000000000001");
            stmt.setString(2, "0000000004");
            stmt.setString(3, "0000000001");
            stmt.setInt(4, 100);
            stmt.setInt(5, 1000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-22 14:22:56").getTime()));
            stmt.execute();

            stmt.setString(1, "000000000000002");
            stmt.setString(2, "0000000003");
            stmt.setString(3, "0000000006");
            stmt.setInt(4, 552);
            stmt.setInt(5, 2000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-25 10:06:29").getTime()));
            stmt.execute();

            stmt.setString(1, "000000000000003");
            stmt.setString(2, "0000000002");
            stmt.setString(3, "0000000002");
            stmt.setInt(4, 190);
            stmt.setInt(5, 3000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-25 16:45:07").getTime()));
            stmt.execute();

            stmt.setString(1, "000000000000004");
            stmt.setString(2, "0000000004");
            stmt.setString(3, "0000000006");
            stmt.setInt(4, 510);
            stmt.setInt(5, 4000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-26 13:26:04").getTime()));
            stmt.execute();

            stmt.setString(1, "000000000000005");
            stmt.setString(2, "0000000005");
            stmt.setString(3, "0000000003");
            stmt.setInt(4, 264);
            stmt.setInt(5, 5000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-27 09:37:50").getTime()));
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    /**
     * Disable and drop all the tables except SYSTEM.CATALOG and SYSTEM.SEQUENCE
     */
    private static void disableAndDropNonSystemTables() throws Exception {
        HBaseAdmin admin = driver.getConnectionQueryServices(null, null).getAdmin();
        try {
            HTableDescriptor[] tables = admin.listTables();
            for (HTableDescriptor table : tables) {
                String schemaName = SchemaUtil.getSchemaNameFromFullName(table.getName());
                if (!QueryConstants.SYSTEM_SCHEMA_NAME.equals(schemaName)) {
                    admin.disableTable(table.getName());
                    admin.deleteTable(table.getName());
                }
            }
        } finally {
            admin.close();
        }
    }

    public static void assertOneOfValuesEqualsResultSet(ResultSet rs, List<List<Object>>... expectedResultsArray) throws SQLException {
        List<List<Object>> results = Lists.newArrayList();
        while (rs.next()) {
            List<Object> result = Lists.newArrayList();
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                result.add(rs.getObject(i+1));
            }
            results.add(result);
        }
        for (int j = 0; j < expectedResultsArray.length; j++) {
            List<List<Object>> expectedResults = expectedResultsArray[j];
            Set<List<Object>> expectedResultsSet = Sets.newHashSet(expectedResults);
            Iterator<List<Object>> iterator = results.iterator();
            while (iterator.hasNext()) {
                if (expectedResultsSet.contains(iterator.next())) {
                    iterator.remove();
                }
            }
        }
        if (results.isEmpty()) return;
        fail("Unable to find " + results + " in " + Arrays.asList(expectedResultsArray));
    }

    /**
     * Asserts that we find the expected values in the result set. We don't know the order, since we don't always
     * have an order by and we're going through indexes, but we assert that each expected result occurs once as
     * expected (in any order).
     */
    public static void assertValuesEqualsResultSet(ResultSet rs, List<List<Object>> expectedResults) throws SQLException {
        int expectedCount = expectedResults.size();
        int count = 0;
        List<List<Object>> actualResults = Lists.newArrayList();
        List<Object> errorResult = null;
        while (rs.next() && errorResult == null) {
            List<Object> result = Lists.newArrayList();
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                result.add(rs.getObject(i+1));
            }
            if (!expectedResults.contains(result)) {
                errorResult = result;
            }
            actualResults.add(result);
            count++;
        }
        assertTrue("Could not find " + errorResult + " in expected results: " + expectedResults + " with actual results: " + actualResults, errorResult == null);
        assertEquals(expectedCount, count);
    }
    
    public HBaseTestingUtility getUtility() {
        return utility;
    }

    protected static void createMultiCFTestTable(String tableName) throws SQLException {
        String ddl = "create table if not exists " + tableName + "(" +
                "   varchar_pk VARCHAR NOT NULL, " +
                "   char_pk CHAR(5) NOT NULL, " +
                "   int_pk INTEGER NOT NULL, "+ 
                "   long_pk BIGINT NOT NULL, " +
                "   decimal_pk DECIMAL(31, 10) NOT NULL, " +
                "   a.varchar_col1 VARCHAR, " +
                "   a.char_col1 CHAR(5), " +
                "   a.int_col1 INTEGER, " +
                "   a.long_col1 BIGINT, " +
                "   a.decimal_col1 DECIMAL(31, 10), " +
                "   b.varchar_col2 VARCHAR, " +
                "   b.char_col2 CHAR(5), " +
                "   b.int_col2 INTEGER, " +
                "   b.long_col2 BIGINT, " +
                "   b.decimal_col2 DECIMAL, " +
                "   b.date_col DATE " + 
                "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk))";
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute(ddl);
            conn.close();
    }
    
    // Populate the test table with data.
    protected static void populateMultiCFTestTable(String tableName) throws SQLException {
        populateMultiCFTestTable(tableName, null);
    }
    
    // Populate the test table with data.
    protected static void populateMultiCFTestTable(String tableName, Date date) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String upsert = "UPSERT INTO " + tableName
                    + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "varchar1");
            stmt.setString(2, "char1");
            stmt.setInt(3, 1);
            stmt.setLong(4, 1L);
            stmt.setBigDecimal(5, new BigDecimal("1.1"));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 2);
            stmt.setLong(9, 2L);
            stmt.setBigDecimal(10, new BigDecimal("2.1"));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 3);
            stmt.setLong(14, 3L);
            stmt.setBigDecimal(15, new BigDecimal("3.1"));
            stmt.setDate(16, date == null ? null : new Date(date.getTime() + TestUtil.MILLIS_IN_DAY));
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar2");
            stmt.setString(2, "char2");
            stmt.setInt(3, 2);
            stmt.setLong(4, 2L);
            stmt.setBigDecimal(5, new BigDecimal("2.2"));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 3);
            stmt.setLong(9, 3L);
            stmt.setBigDecimal(10, new BigDecimal("3.2"));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 4);
            stmt.setLong(14, 4L);
            stmt.setBigDecimal(15, new BigDecimal("4.2"));
            stmt.setDate(16, date);
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar3");
            stmt.setString(2, "char3");
            stmt.setInt(3, 3);
            stmt.setLong(4, 3L);
            stmt.setBigDecimal(5, new BigDecimal("3.3"));
            stmt.setString(6, "varchar_a");
            stmt.setString(7, "chara");
            stmt.setInt(8, 4);
            stmt.setLong(9, 4L);
            stmt.setBigDecimal(10, new BigDecimal("4.3"));
            stmt.setString(11, "varchar_b");
            stmt.setString(12, "charb");
            stmt.setInt(13, 5);
            stmt.setLong(14, 5L);
            stmt.setBigDecimal(15, new BigDecimal("5.3"));
            stmt.setDate(16, date == null ? null : new Date(date.getTime() + 2 * TestUtil.MILLIS_IN_DAY));
            stmt.executeUpdate();
            
            conn.commit();
        } finally {
            conn.close();
        }
    }  
}
