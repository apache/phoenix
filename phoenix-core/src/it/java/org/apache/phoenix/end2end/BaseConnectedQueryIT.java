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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.PhoenixRuntime.CURRENT_SCN_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
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
import static org.apache.phoenix.util.TestUtil.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.PARENTID1;
import static org.apache.phoenix.util.TestUtil.PARENTID2;
import static org.apache.phoenix.util.TestUtil.PARENTID3;
import static org.apache.phoenix.util.TestUtil.PARENTID4;
import static org.apache.phoenix.util.TestUtil.PARENTID5;
import static org.apache.phoenix.util.TestUtil.PARENTID6;
import static org.apache.phoenix.util.TestUtil.PARENTID7;
import static org.apache.phoenix.util.TestUtil.PARENTID8;
import static org.apache.phoenix.util.TestUtil.PARENTID9;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;

/**
 * 
 * Base class for tests that need to be connected to an HBase server
 *
 * 
 * @since 0.1
 */
public abstract class BaseConnectedQueryIT extends BaseTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BaseConnectedQueryIT.class);
    
    protected static byte[][] getDefaultSplits(String tenantId) {
        return new byte[][] { 
            Bytes.toBytes(tenantId + "00A"),
            Bytes.toBytes(tenantId + "00B"),
            Bytes.toBytes(tenantId + "00C"),
            };
    }
    
    protected static String getUrl() {
      Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
      boolean isDistributedCluster = false;
      isDistributedCluster =
          Boolean.parseBoolean(System.getProperty(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER,
            "false"));
      if (!isDistributedCluster) {
        isDistributedCluster =
            conf.getBoolean(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER, false);
      }
      // reconstruct url when running against a live cluster
      if (isDistributedCluster) {
        // Get all info from hbase-site.xml
        return JDBC_PROTOCOL + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;      
      } else {
        return TestUtil.PHOENIX_JDBC_URL;
      }
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        startServer(getUrl());
    }
    
    protected static void deletePriorTables(long ts) throws Exception {
        deletePriorTables(ts, (String)null);
    }
    
    protected static void deletePriorTables(long ts, String tenantId) throws Exception {
        Properties props = new Properties();
        if (ts != HConstants.LATEST_TIMESTAMP) {
            props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(ts));
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            deletePriorTables(ts, conn);
            deletePriorSequences(ts, conn);
        }
        finally {
            conn.close();
        }
    }
    
    private static void deletePriorTables(long ts, Connection globalConn) throws Exception {
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
                    conn = DriverManager.getConnection(getUrl(), props);
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
    
    private static void deletePriorSequences(long ts, Connection conn) throws Exception {
        // TODO: drop tenant-specific sequences too
        ResultSet rs = conn.createStatement().executeQuery("SELECT " 
                + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + "," 
                + PhoenixDatabaseMetaData.SEQUENCE_NAME 
                + " FROM " + PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME);
        while (rs.next()) {
            conn.createStatement().execute("DROP SEQUENCE " + SchemaUtil.getTableName(rs.getString(1), rs.getString(2)));
        }
    }
    
    protected static void initSumDoubleValues(byte[][] splits) throws Exception {
        ensureTableCreated(getUrl(), "SumDoubleTest", splits);
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
    
    protected static void initATableValues(String tenantId, byte[][] splits) throws Exception {
        initATableValues(tenantId, splits, null);
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits, Date date) throws Exception {
        initATableValues(tenantId, splits, date, null);
    }
    
    protected static void initTablesWithArrays(String tenantId, Date date, Long ts, boolean useNull) throws Exception {
    	 Properties props = new Properties();
         if (ts != null) {
             props.setProperty(CURRENT_SCN_ATTRIB, ts.toString());
         }
         Connection conn = DriverManager.getConnection(getUrl(), props);
         try {
             // Insert all rows at ts
             PreparedStatement stmt = conn.prepareStatement(
                     "upsert into " +
                     "TABLE_WITH_ARRAY(" +
                     "    ORGANIZATION_ID, " +
                     "    ENTITY_ID, " +
                     "    a_string_array, " +
                     "    B_STRING, " +
                     "    A_INTEGER, " +
                     "    A_DATE, " +
                     "    X_DECIMAL, " +
                     "    x_long_array, " +
                     "    X_INTEGER," +
                     "    a_byte_array," +
                     "    A_SHORT," +
                     "    A_FLOAT," +
                     "    a_double_array," +
                     "    A_UNSIGNED_FLOAT," +
                     "    A_UNSIGNED_DOUBLE)" +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
             stmt.setString(1, tenantId);
             stmt.setString(2, ROW1);
             // Need to support primitive
             String[] strArr =  new String[4];
             strArr[0] = "ABC";
			if (useNull) {
				strArr[1] = null;
			} else {
				strArr[1] = "CEDF";
			}
             strArr[2] = "XYZWER";
             strArr[3] = "AB";
             Array array = conn.createArrayOf("VARCHAR", strArr);
             stmt.setArray(3, array);
             stmt.setString(4, B_VALUE);
             stmt.setInt(5, 1);
             stmt.setDate(6, date);
             stmt.setBigDecimal(7, null);
             // Need to support primitive
             Long[] longArr =  new Long[2];
             longArr[0] = 25l;
             longArr[1] = 36l;
             array = conn.createArrayOf("BIGINT", longArr);
             stmt.setArray(8, array);
             stmt.setNull(9, Types.INTEGER);
             // Need to support primitive
             Byte[] byteArr =  new Byte[2];
             byteArr[0] = 25;
             byteArr[1] = 36;
             array = conn.createArrayOf("TINYINT", byteArr);
             stmt.setArray(10, array);
             stmt.setShort(11, (short) 128);
             stmt.setFloat(12, 0.01f);
             // Need to support primitive
             Double[] doubleArr =  new Double[4];
             doubleArr[0] = 25.343;
             doubleArr[1] = 36.763;
             doubleArr[2] = 37.56;
             doubleArr[3] = 386.63;
             array = conn.createArrayOf("DOUBLE", doubleArr);
             stmt.setArray(13, array);
             stmt.setFloat(14, 0.01f);
             stmt.setDouble(15, 0.0001);
             stmt.execute();
                 
             conn.commit();
         } finally {
             conn.close();
         }
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        if (ts == null) {
            ensureTableCreated(getUrl(), ATABLE_NAME, splits);
        } else {
            ensureTableCreated(getUrl(), ATABLE_NAME, splits, ts-5);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(ts-3));
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
    protected static void initEntityHistoryTableValues(String tenantId, byte[][] splits) throws Exception {
        initEntityHistoryTableValues(tenantId, splits, null);
    }
    
    protected static void initEntityHistoryTableValues(String tenantId, byte[][] splits, Date date) throws Exception {
        initEntityHistoryTableValues(tenantId, splits, date, null);
    }
    
    protected static void initEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        if (ts == null) {
            ensureTableCreated(getUrl(), ENTITY_HISTORY_TABLE_NAME, splits);
        } else {
            ensureTableCreated(getUrl(), ENTITY_HISTORY_TABLE_NAME, splits, ts-2);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
    
    protected static void initSaltedEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        if (ts == null) {
            ensureTableCreated(getUrl(), ENTITY_HISTORY_SALTED_TABLE_NAME, splits);
        } else {
            ensureTableCreated(getUrl(), ENTITY_HISTORY_SALTED_TABLE_NAME, splits, ts-2);
        }
        
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
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

}
