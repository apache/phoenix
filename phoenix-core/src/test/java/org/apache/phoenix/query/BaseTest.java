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

import static org.apache.phoenix.hbase.index.write.ParallelWriterIndexCommitter.NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY;
import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.PhoenixRuntime.CURRENT_SCN_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.BINARY_NAME;
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
import static org.apache.phoenix.util.TestUtil.HBASE_DYNAMIC_COLUMNS;
import static org.apache.phoenix.util.TestUtil.MULTI_CF_NAME;
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
import static org.apache.phoenix.util.TestUtil.SUM_DOUBLE_NAME;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_ARRAY;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_SALTING;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ConfigUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionFactory;
import org.apache.phoenix.util.TestUtil;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
    public static final String DRIVER_CLASS_NAME_ATTRIB = "phoenix.driver.class.name";
    
    private static final Map<String,String> tableDDLMap;
    private static final Logger logger = LoggerFactory.getLogger(BaseTest.class);
    @ClassRule
    public static TemporaryFolder tmpFolder = new TemporaryFolder();
    private static final int dropTableTimeout = 300; // 5 mins should be long enough.
    private static final ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("DROP-TABLE-BASETEST" + "-thread-%s").build();
    private static final ExecutorService dropHTableService = Executors
            .newSingleThreadExecutor(factory);

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
        builder.put(MULTI_CF_NAME,"create table " + MULTI_CF_NAME +
                "   (id char(15) not null primary key,\n" +
                "    a.unique_user_count integer,\n" +
                "    b.unique_org_count integer,\n" +
                "    c.db_cpu_utilization decimal(31,10),\n" +
                "    d.transaction_count bigint,\n" +
                "    e.cpu_utilization decimal(31,10),\n" +
                "    f.response_time bigint,\n" +
                "    g.response_time bigint)");
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
                "    CONSTRAINT pk PRIMARY KEY (organization_id, \"DATE\", feature, UNIQUE_USERS))");
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
        builder.put(SUM_DOUBLE_NAME,"create table SumDoubleTest" +
                "   (id varchar not null primary key, d DOUBLE, f FLOAT, ud UNSIGNED_DOUBLE, uf UNSIGNED_FLOAT, i integer, de decimal)");
        builder.put(BINARY_NAME,"create table " + BINARY_NAME +
            "   (a_binary BINARY(16) not null, \n" +
            "    b_binary BINARY(16), \n" +
            "    a_varbinary VARBINARY, \n" +
            "    b_varbinary VARBINARY, \n" +
            "    CONSTRAINT pk PRIMARY KEY (a_binary)\n" +
            ") ");
        tableDDLMap = builder.build();
    }
    
    private static final String ORG_ID = "00D300000000XHP";
    protected static int NUM_SLAVES_BASE = 1;
    private static final String DEFAULT_RPC_SCHEDULER_FACTORY = PhoenixRpcSchedulerFactory.class.getName();
    
    protected static String getZKClientPort(Configuration conf) {
        return conf.get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
    }
    
    protected static String url;
    protected static PhoenixTestDriver driver;
    protected static boolean clusterInitialized = false;
    protected static HBaseTestingUtility utility;
    protected static final Configuration config = HBaseConfiguration.create();

    private static class TearDownMiniClusterThreadFactory implements ThreadFactory {
        private static final AtomicInteger threadNumber = new AtomicInteger(1);
        private static final String NAME_PREFIX = "PHOENIX-TEARDOWN-MINICLUSTER-thread-";

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, NAME_PREFIX + threadNumber.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }

    private static ExecutorService tearDownClusterService =
            Executors.newSingleThreadExecutor(new TearDownMiniClusterThreadFactory());

    protected static String getUrl() {
        if (!clusterInitialized) {
            throw new IllegalStateException("Cluster must be initialized before attempting to get the URL");
        }
        return url;
    }
    
    private static String checkClusterInitialized(ReadOnlyProps serverProps) throws Exception {
        if (!clusterInitialized) {
            url = setUpTestCluster(config, serverProps);
            clusterInitialized = true;
        }
        return url;
    }

    /**
     * Set up the test hbase cluster.
     * @return url to be used by clients to connect to the cluster.
     * @throws IOException 
     */
    protected static String setUpTestCluster(@Nonnull Configuration conf, ReadOnlyProps overrideProps) throws Exception {
        boolean isDistributedCluster = isDistributedClusterModeEnabled(conf);
        if (!isDistributedCluster) {
            return initMiniCluster(conf, overrideProps);
       } else {
            return initClusterDistributedMode(conf, overrideProps);
        }
    }
    
    protected static void destroyDriver() {
        if (driver != null) {
            try {
                assertTrue(destroyDriver(driver));
            } catch (Throwable t) {
                logger.error("Exception caught when destroying phoenix test driver", t);
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

    public static void tearDownMiniClusterAsync(final int numTables) {
        final HBaseTestingUtility u = utility;
        try {
            destroyDriver();
            utility = null;
            clusterInitialized = false;
        } finally {
            tearDownClusterService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    long startTime = System.currentTimeMillis();
                    if (u != null) {
                        try {
                            u.shutdownMiniMapReduceCluster();
                        } catch (Throwable t) {
                            logger.error(
                                "Exception caught when shutting down mini map reduce cluster", t);
                        } finally {
                            try {
                                u.shutdownMiniCluster();
                            } catch (Throwable t) {
                                logger.error("Exception caught when shutting down mini cluster", t);
                            } finally {
                                try {
                                    ConnectionFactory.shutdown();
                                } finally {
                                    logger.info(
                                        "Time in seconds spent in shutting down mini cluster with "
                                                + numTables + " tables: "
                                                + (System.currentTimeMillis() - startTime) / 1000);
                                }
                            }
                        }
                    }
                    return null;
                }
            });
        }
    }

    protected static void setUpTestDriver(ReadOnlyProps props) throws Exception {
        setUpTestDriver(props, props);
    }
    
    protected static void setUpTestDriver(ReadOnlyProps serverProps, ReadOnlyProps clientProps) throws Exception {
        if (driver == null) {
            String url = checkClusterInitialized(serverProps);
            driver = initAndRegisterTestDriver(url, clientProps);
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
     * @throws Exception 
     */
    private static String initMiniCluster(Configuration conf, ReadOnlyProps overrideProps) throws Exception {
        setUpConfigForMiniCluster(conf, overrideProps);
        utility = new HBaseTestingUtility(conf);
        try {
            utility.startMiniCluster(NUM_SLAVES_BASE);
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
     * @throws Exception 
     */
    private static String initClusterDistributedMode(Configuration conf, ReadOnlyProps overrideProps) throws Exception {
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

    private static void setTestConfigForDistribuedCluster(Configuration conf, ReadOnlyProps overrideProps) throws Exception {
        setDefaultTestConfig(conf, overrideProps);
    }
    
    private static void setDefaultTestConfig(Configuration conf, ReadOnlyProps overrideProps) throws Exception {
        ConfigUtil.setReplicationConfigIfAbsent(conf);
        QueryServices services = newTestDriver(overrideProps).getQueryServices();
        for (Entry<String,String> entry : services.getProps()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        //no point doing sanity checks when running tests.
        conf.setBoolean("hbase.table.sanity.checks", false);
        // set the server rpc controller and rpc scheduler factory, used to configure the cluster
        conf.set(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS, DEFAULT_RPC_SCHEDULER_FACTORY);
        conf.setLong(HConstants.ZK_SESSION_TIMEOUT, 10 * HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
        conf.setLong(HConstants.ZOOKEEPER_TICK_TIME, 6 * 1000);
        
        // override any defaults based on overrideProps
        for (Entry<String,String> entry : overrideProps) {
            conf.set(entry.getKey(), entry.getValue());
        }
    }
    
    public static Configuration setUpConfigForMiniCluster(Configuration conf) throws Exception {
        return setUpConfigForMiniCluster(conf, ReadOnlyProps.EMPTY_PROPS);
    }
    
    public static Configuration setUpConfigForMiniCluster(Configuration conf, ReadOnlyProps overrideProps) throws Exception {
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
        conf.setInt("dfs.namenode.handler.count", 2);
        conf.setInt("dfs.namenode.service.handler.count", 2);
        conf.setInt("dfs.datanode.handler.count", 2);
        conf.setInt("ipc.server.read.threadpool.size", 2);
        conf.setInt("ipc.server.handler.threadpool.size", 2);
        conf.setInt("hbase.regionserver.hlog.syncer.count", 2);
        conf.setInt("hbase.hlog.asyncer.number", 2);
        conf.setInt("hbase.assignment.zkevent.workers", 5);
        conf.setInt("hbase.assignment.threads.max", 5);
        conf.setInt("hbase.catalogjanitor.interval", 5000);
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
        conf.setInt(NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY, 1);
        return conf;
    }

    private static PhoenixTestDriver newTestDriver(ReadOnlyProps props) throws Exception {
        PhoenixTestDriver newDriver;
        String driverClassName = props.get(DRIVER_CLASS_NAME_ATTRIB);
        if (driverClassName == null) {
            newDriver = new PhoenixTestDriver(props);
        } else {
            Class<?> clazz = Class.forName(driverClassName);
            Constructor constr = clazz.getConstructor(ReadOnlyProps.class);
            newDriver = (PhoenixTestDriver)constr.newInstance(props);
        }
        return newDriver;
    }
    /**
     * Create a {@link PhoenixTestDriver} and register it.
     * @return an initialized and registered {@link PhoenixTestDriver} 
     */
    public static PhoenixTestDriver initAndRegisterTestDriver(String url, ReadOnlyProps props) throws Exception {
        PhoenixTestDriver newDriver = newTestDriver(props);
        DriverManager.registerDriver(newDriver);
        Driver oldDriver = DriverManager.getDriver(url); 
        if (oldDriver != newDriver) {
            destroyDriver(oldDriver);
        }
        Properties driverProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = newDriver.connect(url, driverProps);
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
        ensureTableCreated(url, tableName, tableName, null, null, null);
    }

    protected static void ensureTableCreated(String url, String tableName, String tableDDLType) throws SQLException {
        ensureTableCreated(url, tableName, tableDDLType, null, null, null);
    }

    public static void ensureTableCreated(String url, String tableName, String tableDDLType, byte[][] splits, String tableDDLOptions) throws SQLException {
        ensureTableCreated(url, tableName, tableDDLType, splits, null, tableDDLOptions);
    }

    protected static void ensureTableCreated(String url, String tableName, String tableDDLType, Long ts) throws SQLException {
        ensureTableCreated(url, tableName, tableDDLType, null, ts, null);
    }

    protected static void ensureTableCreated(String url, String tableName, String tableDDLType, byte[][] splits, Long ts, String tableDDLOptions) throws SQLException {
        String ddl = tableDDLMap.get(tableDDLType);
        if(!tableDDLType.equals(tableName)) {
           ddl =  ddl.replace(tableDDLType, tableName);
        }
        if (tableDDLOptions!=null) {
            ddl += tableDDLOptions;
        }
        createSchema(url,tableName, ts);
        createTestTable(url, ddl, splits, ts);
    }

    private static AtomicInteger NAME_SUFFIX = new AtomicInteger(0);
    private static final int MAX_SUFFIX_VALUE = 1000000;

    /**
     * Counter to track number of tables we have created. This isn't really accurate since this
     * counter will be incremented when we call {@link #generateUniqueName()}for getting unique
     * schema and sequence names too. But this will have to do.
     */
    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);
    /*
     * Threshold to monitor if we need to restart mini-cluster since we created too many tables.
     * Note, we can't have this value too high since we don't want the shutdown to take too
     * long a time either.
     */
    private static final int TEARDOWN_THRESHOLD = 30;

    public static String generateUniqueName() {
        int nextName = NAME_SUFFIX.incrementAndGet();
        if (nextName >= MAX_SUFFIX_VALUE) {
            throw new IllegalStateException("Used up all unique names");
        }
        TABLE_COUNTER.incrementAndGet();
        return "N" + Integer.toString(MAX_SUFFIX_VALUE + nextName).substring(1);
    }
    
    private static AtomicInteger SEQ_NAME_SUFFIX = new AtomicInteger(0);
    private static final int MAX_SEQ_SUFFIX_VALUE = 1000000;

    private static final AtomicInteger SEQ_COUNTER = new AtomicInteger(0);

    public static String generateUniqueSequenceName() {
        int nextName = SEQ_NAME_SUFFIX.incrementAndGet();
        if (nextName >= MAX_SEQ_SUFFIX_VALUE) {
            throw new IllegalStateException("Used up all unique sequence names");
        }
        SEQ_COUNTER.incrementAndGet();
        return "S" + Integer.toString(MAX_SEQ_SUFFIX_VALUE + nextName).substring(1);
    }

    public static void tearDownMiniClusterIfBeyondThreshold() throws Exception {
        if (TABLE_COUNTER.get() > TEARDOWN_THRESHOLD) {
            int numTables = TABLE_COUNTER.get();
            TABLE_COUNTER.set(0);
            logger.info(
                "Shutting down mini cluster because number of tables on this mini cluster is likely greater than "
                        + TEARDOWN_THRESHOLD);
            tearDownMiniClusterAsync(numTables);
        }
    }

    protected static void createTestTable(String url, String ddl) throws SQLException {
        createTestTable(url, ddl, null, null);
    }

    protected static void createTestTable(String url, String ddl, byte[][] splits, Long ts) throws SQLException {
        createTestTable(url, ddl, splits, ts, true);
    }

    public static void createSchema(String url, String tableName, Long ts) throws SQLException {
        String schema = SchemaUtil.getSchemaNameFromFullName(tableName);
        if (!schema.equals("")) {
            Properties props = new Properties();
            if (ts != null) {
                props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
            }
            try (Connection conn = DriverManager.getConnection(url, props);) {
                if (SchemaUtil.isNamespaceMappingEnabled(null,
                        conn.unwrap(PhoenixConnection.class).getQueryServices().getProps())) {
                    conn.createStatement().executeUpdate("CREATE SCHEMA IF NOT EXISTS " + schema);
                }
            }
        }
    }

    protected static void createTestTable(String url, String ddl, byte[][] splits, Long ts, boolean swallowTableAlreadyExistsException) throws SQLException {
        assertNotNull(ddl);
        StringBuilder buf = new StringBuilder(ddl);
        if (splits != null) {
            buf.append(" SPLIT ON (");
            for (int i = 0; i < splits.length; i++) {
                buf.append("'").append(Bytes.toString(splits[i])).append("'").append(",");
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
            conn.createStatement().execute(ddl);
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

    private static void deletePriorSchemas(long ts, String url) throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1024));
        if (ts != HConstants.LATEST_TIMESTAMP) {
            props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(ts));
        }
        try (Connection conn = DriverManager.getConnection(url, props)) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getSchemas();
            while (rs.next()) {
                String schemaName = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM);
                if (schemaName.equals(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME)) {
                    continue;
                }
                schemaName = SchemaUtil.getEscapedArgument(schemaName);

                String ddl = "DROP SCHEMA " + schemaName;
                conn.createStatement().executeUpdate(ddl);
            }
            rs.close();
        }
        // Make sure all schemas have been dropped
        props.remove(CURRENT_SCN_ATTRIB);
        try (Connection seeLatestConn = DriverManager.getConnection(url, props)) {
            DatabaseMetaData dbmd = seeLatestConn.getMetaData();
            ResultSet rs = dbmd.getSchemas();
            boolean hasSchemas = rs.next();
            if (hasSchemas) {
                String schemaName = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM);
                if (schemaName.equals(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME)) {
                    hasSchemas = rs.next();
                }
            }
            if (hasSchemas) {
                fail("The following schemas are not dropped that should be:" + getSchemaNames(rs));
            }
        }
    }

    protected static void deletePriorMetaData(long ts, String url) throws Exception {
        deletePriorTables(ts, url);
        if (ts != HConstants.LATEST_TIMESTAMP) {
            ts = nextTimestamp() - 1;
        }
        deletePriorSchemas(ts, url);
    }

    private static void deletePriorTables(long ts, String url) throws Exception {
        deletePriorTables(ts, (String)null, url);
    }
    
    private static void deletePriorTables(long ts, String tenantId, String url) throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1024));
        if (ts != HConstants.LATEST_TIMESTAMP) {
            props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(ts));
        }
        Connection conn = DriverManager.getConnection(url, props);
        try {
            deletePriorTables(ts, conn, url);
            deletePriorSequences(ts, conn);
            
            // Make sure all tables and views have been dropped
            props.remove(CURRENT_SCN_ATTRIB);
            try (Connection seeLatestConn = DriverManager.getConnection(url, props)) {
                DatabaseMetaData dbmd = seeLatestConn.getMetaData();
                ResultSet rs = dbmd.getTables(null, null, null, new String[]{PTableType.VIEW.toString(), PTableType.TABLE.toString()});
                while (rs.next()) {
                    String fullTableName = SchemaUtil.getEscapedTableName(
                            rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM),
                            rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
                    try {
                        PhoenixRuntime.getTable(conn, fullTableName);
                        fail("The following tables are not deleted that should be:" + getTableNames(rs));
                    } catch (TableNotFoundException e) {
                    }
                }
            }
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
                    Properties props = PropertiesUtil.deepCopy(globalConn.getClientInfo());
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
            rs.close();
            if (lastTenantId != null) {
                conn.close();
            }
        }
    }
    
    private static String getTableNames(ResultSet rs) throws SQLException {
        StringBuilder buf = new StringBuilder();
        do {
            buf.append(" ");
            buf.append(SchemaUtil.getTableName(rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM), rs.getString(PhoenixDatabaseMetaData.TABLE_NAME)));
        } while (rs.next());
        return buf.toString();
    }

    private static String getSchemaNames(ResultSet rs) throws SQLException {
        StringBuilder buf = new StringBuilder();
        do {
            buf.append(" ");
            buf.append(rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM));
        } while (rs.next());
        return buf.toString();
    }

    private static void deletePriorSequences(long ts, Connection globalConn) throws Exception {
        // TODO: drop tenant-specific sequences too
        ResultSet rs = globalConn.createStatement().executeQuery("SELECT " 
                + PhoenixDatabaseMetaData.TENANT_ID + ","
                + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + "," 
                + PhoenixDatabaseMetaData.SEQUENCE_NAME 
                + " FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE);
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
        rs.close();
    }

    protected static void initSumDoubleValues(byte[][] splits, String url) throws Exception {
        initSumDoubleValues(SUM_DOUBLE_NAME, splits, url);
    }

    protected static void initSumDoubleValues(String tableName, byte[][] splits, String url) throws Exception {
        ensureTableCreated(url, tableName, SUM_DOUBLE_NAME, splits, null);
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(url, props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tableName +
                    "(" +
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

    protected static String initATableValues(String tenantId, byte[][] splits) throws Exception {
        return initATableValues(tenantId, splits, null, null, getUrl());
    }
    
    protected static String initATableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        return initATableValues(tenantId, splits, date, ts, getUrl());
    }
    
    protected static String initATableValues(String tenantId, byte[][] splits, String url) throws Exception {
        return initATableValues(tenantId, splits, null, url);
    }
    
    protected static String initATableValues(String tenantId, byte[][] splits, Date date, String url) throws Exception {
        return initATableValues(tenantId, splits, date, null, url);
    }

    protected static String initATableValues(String tenantId, byte[][] splits, Date date, Long ts, String url) throws Exception {
        return initATableValues(null, tenantId, splits, date, ts, url, null);
    }

    protected static String initATableValues(String tenantId, byte[][] splits, Date date, Long ts, String url, String tableDDLOptions) throws Exception {
        return initATableValues(null, tenantId, splits, date, ts, url, tableDDLOptions);
    }

    protected static String initATableValues(String tableName, String tenantId, byte[][] splits, Date date, Long ts, String url, String tableDDLOptions) throws Exception {
        if(tableName == null) {
            tableName = generateUniqueName();
        }
        String tableDDLType = ATABLE_NAME;
        if (ts == null) {
            ensureTableCreated(url, tableName, tableDDLType, splits, null, tableDDLOptions);
        } else {
            ensureTableCreated(url, tableName, tableDDLType, splits, ts-5, tableDDLOptions);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(ts-3));
        }
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tableName +
                    "(" +
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
        }
        return tableName;
    }

    
    protected static String initEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        return initEntityHistoryTableValues(ENTITY_HISTORY_TABLE_NAME, tenantId, splits, date, ts, getUrl());
    }
    
    protected static String initEntityHistoryTableValues(String tableName, String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        return initEntityHistoryTableValues(tableName, tenantId, splits, date, ts, getUrl());
    }
    
    protected static String initSaltedEntityHistoryTableValues(String tableName, String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        return initSaltedEntityHistoryTableValues(tableName, tenantId, splits, date, ts, getUrl());
    }

    protected static String initEntityHistoryTableValues(String tableName, String tenantId, byte[][] splits, String url) throws Exception {
        return initEntityHistoryTableValues(tableName, tenantId, splits, null, null, url);
    }
    
    private static String initEntityHistoryTableValues(String tableName, String tenantId, byte[][] splits, Date date, Long ts, String url) throws Exception {
        if (tableName == null) {
            tableName = generateUniqueName();
        }
        
        if (ts == null) {
            ensureTableCreated(url, tableName, ENTITY_HISTORY_TABLE_NAME, splits, null);
        } else {
            ensureTableCreated(url, tableName, ENTITY_HISTORY_TABLE_NAME, splits, ts-2, null);
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
                    tableName +
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
        
        return tableName;
    }
    
    protected static String initSaltedEntityHistoryTableValues(String tableName, String tenantId, byte[][] splits, Date date, Long ts, String url) throws Exception {
        if (tableName == null) {
            tableName = generateUniqueName();
        }
        
        if (ts == null) {
            ensureTableCreated(url, tableName, ENTITY_HISTORY_SALTED_TABLE_NAME, splits, null);
        } else {
            ensureTableCreated(url, tableName, ENTITY_HISTORY_SALTED_TABLE_NAME, splits, ts-2, null);
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
                    tableName +
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
        
        return tableName;
    }
    
    /**
     * Disable and drop all the tables except SYSTEM.CATALOG and SYSTEM.SEQUENCE
     */
    private static void disableAndDropNonSystemTables() throws Exception {
        if (driver == null) return;
        Admin admin = driver.getConnectionQueryServices(null, null).getAdmin();
        try {
            TableDescriptor[] tables = admin.listTables();
            for (TableDescriptor table : tables) {
                String schemaName = SchemaUtil.getSchemaNameFromFullName(table.getTableName().getName());
                if (!QueryConstants.SYSTEM_SCHEMA_NAME.equals(schemaName)) {
                    disableAndDropTable(admin, table.getTableName());
                }
            }
        } finally {
            admin.close();
        }
    }
    
    private static void disableAndDropTable(final Admin admin, final TableName tableName)
            throws Exception {
        Future<Void> future = null;
        boolean success = false;
        try {
            try {
                future = dropHTableService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (admin.isTableEnabled(tableName)) {
                            admin.disableTable(tableName);
                            admin.deleteTable(tableName);
                        }
                        return null;
                    }
                });
                future.get(dropTableTimeout, TimeUnit.SECONDS);
                success = true;
            } catch (TimeoutException e) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.OPERATION_TIMED_OUT)
                .setMessage(
                    "Not able to disable and delete table " + tableName.getNameAsString()
                    + " in " + dropTableTimeout + " seconds.").build().buildException();
            } catch (Exception e) {
                throw e;
            }
        } finally { 
            if (future != null && !success) {
                future.cancel(true);
            }
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

    protected void assertValueEqualsResultSet(ResultSet rs, List<Object> expectedResults) throws SQLException {
        List<List<Object>> nestedExpectedResults = Lists.newArrayListWithExpectedSize(expectedResults.size());
        for (Object expectedResult : expectedResults) {
            nestedExpectedResults.add(Arrays.asList(expectedResult));
        }
        assertValuesEqualsResultSet(rs, nestedExpectedResults); 
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
    
    public static HBaseTestingUtility getUtility() {
        return utility;
    }
    
    public static void upsertRows(Connection conn, String fullTableName, int numRows) throws SQLException {
        for (int i=1; i<=numRows; ++i) {
            upsertRow(conn, fullTableName, i, false);
        }
    }

    public static void upsertRow(Connection conn, String fullTableName, int index, boolean firstRowInBatch) throws SQLException {
        String upsert = "UPSERT INTO " + fullTableName
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(upsert);
        stmt.setString(1, firstRowInBatch ? "firstRowInBatch_" : "" + "varchar"+index);
        stmt.setString(2, "char"+index);
        stmt.setInt(3, index);
        stmt.setLong(4, index);
        stmt.setBigDecimal(5, new BigDecimal(index));
        Date date = DateUtil.parseDate("2015-01-01 00:00:00");
        stmt.setDate(6, date);
        stmt.setString(7, "varchar_a");
        stmt.setString(8, "chara");
        stmt.setInt(9, index+1);
        stmt.setLong(10, index+1);
        stmt.setBigDecimal(11, new BigDecimal(index+1));
        stmt.setDate(12, date);
        stmt.setString(13, "varchar_b");
        stmt.setString(14, "charb");
        stmt.setInt(15, index+2);
        stmt.setLong(16, index+2);
        stmt.setBigDecimal(17, new BigDecimal(index+2));
        stmt.setDate(18, date);
        stmt.executeUpdate();
    }

    // Populate the test table with data.
    public static void populateTestTable(String fullTableName) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            upsertRows(conn, fullTableName, 3);
            conn.commit();
        }
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
            stmt.setDate(16, date == null ? null : new Date(date.getTime() + MILLIS_IN_DAY));
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
            stmt.setDate(16, date == null ? null : new Date(date.getTime() + 2 * MILLIS_IN_DAY));
            stmt.executeUpdate();
            
            conn.commit();
        } finally {
            conn.close();
        }
    }
    protected static void verifySequenceNotExists(String tenantID, String sequenceName, String sequenceSchemaName) throws SQLException {
        verifySequence(tenantID, sequenceName, sequenceSchemaName, false, 0);
    }
    
    protected static void verifySequenceValue(String tenantID, String sequenceName, String sequenceSchemaName, long value) throws SQLException {
        verifySequence(tenantID, sequenceName, sequenceSchemaName, true, value);
    }
    
    private  static void verifySequence(String tenantID, String sequenceName, String sequenceSchemaName, boolean exists, long value) throws SQLException {

        PhoenixConnection phxConn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        String ddl = "SELECT "
                + PhoenixDatabaseMetaData.TENANT_ID + ","
                + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + ","
                + PhoenixDatabaseMetaData.SEQUENCE_NAME + ","
                + PhoenixDatabaseMetaData.CURRENT_VALUE
                + " FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE
                + " WHERE ";

        ddl += " TENANT_ID  " + ((tenantID == null ) ? "IS NULL " : " = '" + tenantID + "'");
        ddl += " AND SEQUENCE_NAME " + ((sequenceName == null) ? "IS NULL " : " = '" +  sequenceName + "'");
        ddl += " AND SEQUENCE_SCHEMA " + ((sequenceSchemaName == null) ? "IS NULL " : " = '" + sequenceSchemaName + "'" );

        ResultSet rs = phxConn.createStatement().executeQuery(ddl);

        if(exists) {
            assertTrue(rs.next());
            assertEquals(value, rs.getLong(4));
        } else {
            assertFalse(rs.next());
        }
        phxConn.close();
    }
    

    /**
     *  Split SYSTEM.CATALOG at the given split point 
     */
    protected static void splitRegion(byte[] splitPoint) throws SQLException, IOException, InterruptedException {
        Admin admin =
                driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        admin.split(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME, splitPoint);
        // make sure the split finishes (there's no synchronous splitting before HBase 2.x)
        admin.disableTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
        admin.enableTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
    }
    
    /**
     * Returns true if the region contains atleast one of the metadata rows we are interested in
     */
    protected static boolean regionContainsMetadataRows(HRegionInfo regionInfo,
            List<byte[]> metadataRowKeys) {
        for (byte[] rowKey : metadataRowKeys) {
            if (regionInfo.containsRow(rowKey)) {
                return true;
            }
        }
        return false;
    }

    protected static void splitTable(TableName fullTableName, List<byte[]> splitPoints) throws Exception {
        Admin admin =
                driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        assertTrue("Needs at least two split points ", splitPoints.size() > 1);
        assertTrue(
                "Number of split points should be less than or equal to the number of region servers ",
                splitPoints.size() <= NUM_SLAVES_BASE);
        HBaseTestingUtility util = getUtility();
        MiniHBaseCluster cluster = util.getHBaseCluster();
        HMaster master = cluster.getMaster();
        AssignmentManager am = master.getAssignmentManager();
        // No need to split on the first splitPoint since the end key of region boundaries are exclusive
        for (int i=1; i<splitPoints.size(); ++i) {
            splitRegion(splitPoints.get(i));
        }
        HashMap<ServerName, List<HRegionInfo>> serverToRegionsList = Maps.newHashMapWithExpectedSize(NUM_SLAVES_BASE);
        Deque<ServerName> availableRegionServers = new ArrayDeque<ServerName>(NUM_SLAVES_BASE);
        for (int i=0; i<NUM_SLAVES_BASE; ++i) {
            availableRegionServers.push(util.getHBaseCluster().getRegionServer(i).getServerName());
        }
        List<HRegionInfo> tableRegions =
                admin.getTableRegions(fullTableName);
        for (HRegionInfo hRegionInfo : tableRegions) {
            // filter on regions we are interested in
            if (regionContainsMetadataRows(hRegionInfo, splitPoints)) {
                ServerName serverName = am.getRegionStates().getRegionServerOfRegion(hRegionInfo);
                if (!serverToRegionsList.containsKey(serverName)) {
                    serverToRegionsList.put(serverName, new ArrayList<HRegionInfo>());
                }
                serverToRegionsList.get(serverName).add(hRegionInfo);
                availableRegionServers.remove(serverName);
            }
        }
        assertTrue("No region servers available to move regions on to ", !availableRegionServers.isEmpty());
        for (Entry<ServerName, List<HRegionInfo>> entry : serverToRegionsList.entrySet()) {
            List<HRegionInfo> regions = entry.getValue();
            if (regions.size()>1) {
                for (int i=1; i< regions.size(); ++i) {
                    moveRegion(regions.get(i), entry.getKey(), availableRegionServers.pop());
                }
            }
        }

        // verify each region is on its own region server
        tableRegions =
                admin.getTableRegions(fullTableName);
        Set<ServerName> serverNames = Sets.newHashSet();
        for (HRegionInfo hRegionInfo : tableRegions) {
            // filter on regions we are interested in
            if (regionContainsMetadataRows(hRegionInfo, splitPoints)) {
                ServerName serverName = am.getRegionStates().getRegionServerOfRegion(hRegionInfo);
                if (!serverNames.contains(serverName)) {
                    serverNames.add(serverName);
                }
                else {
                    fail("Multiple regions on "+serverName.getServerName());
                }
            }
        }
    }

    /**
     * Splits SYSTEM.CATALOG into multiple regions based on the table or view names passed in.
     * Metadata for each table or view is moved to a separate region,
     * @param tenantToTableAndViewMap map from tenant to tables and views owned by the tenant
     */
    protected static void splitSystemCatalog(Map<String, List<String>> tenantToTableAndViewMap) throws Exception  {
        List<byte[]> splitPoints = Lists.newArrayListWithExpectedSize(5);
        // add the rows keys of the table or view metadata rows
        Set<String> schemaNameSet=Sets.newHashSetWithExpectedSize(15);
        for (Entry<String, List<String>> entrySet : tenantToTableAndViewMap.entrySet()) {
            String tenantId = entrySet.getKey();
            for (String fullName : entrySet.getValue()) {
                String schemaName = SchemaUtil.getSchemaNameFromFullName(fullName);
                // we don't allow SYSTEM.CATALOG to split within a schema, so to ensure each table
                // or view is on a separate region they need to have a unique tenant and schema name
                assertTrue("Schema names of tables/view must be unique ", schemaNameSet.add(tenantId+"."+schemaName));
                String tableName = SchemaUtil.getTableNameFromFullName(fullName);
                splitPoints.add(
                    SchemaUtil.getTableKey(tenantId, "".equals(schemaName) ? null : schemaName, tableName));
            }
        }
        Collections.sort(splitPoints, Bytes.BYTES_COMPARATOR);

        splitTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME, splitPoints);
    }
    
    /**
     * Ensures each region of SYSTEM.CATALOG is on a different region server
     */
    private static void moveRegion(HRegionInfo regionInfo, ServerName srcServerName, ServerName dstServerName) throws Exception  {
        Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        HBaseTestingUtility util = getUtility();
        MiniHBaseCluster cluster = util.getHBaseCluster();
        HMaster master = cluster.getMaster();
        AssignmentManager am = master.getAssignmentManager();
   
        HRegionServer dstServer = util.getHBaseCluster().getRegionServer(dstServerName);
        HRegionServer srcServer = util.getHBaseCluster().getRegionServer(srcServerName);
        byte[] encodedRegionNameInBytes = regionInfo.getEncodedNameAsBytes();
        admin.move(encodedRegionNameInBytes, Bytes.toBytes(dstServer.getServerName().getServerName()));
        while (dstServer.getOnlineRegion(regionInfo.getRegionName()) == null
                || dstServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes)
                || srcServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes)) {
            // wait for the move to be finished
            Thread.sleep(100);
        }
    }
}
