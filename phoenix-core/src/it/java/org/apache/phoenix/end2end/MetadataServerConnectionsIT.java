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

import static org.apache.phoenix.query.QueryServices.DISABLE_VIEW_SUBTREE_VALIDATION;
import static org.apache.phoenix.query.QueryServicesTestImpl.DEFAULT_HCONNECTION_POOL_CORE_SIZE;
import static org.apache.phoenix.query.QueryServicesTestImpl.DEFAULT_HCONNECTION_POOL_MAX_SIZE;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionImplementation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Tests to ensure connection creation by metadata coproc does not need to make RPC call to metada
 * coproc internally.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class MetadataServerConnectionsIT extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataServerConnectionsIT.class);

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
    props.put(DISABLE_VIEW_SUBTREE_VALIDATION, "true");
    setUpTestDriver(new ReadOnlyProps(props));
  }

  public static class TestMetaDataEndpointImpl extends MetaDataEndpointImpl {
    private RegionCoprocessorEnvironment env;

    public static void setTestCreateView(boolean testCreateView) {
      TestMetaDataEndpointImpl.testCreateView = testCreateView;
    }

    private static volatile boolean testCreateView = false;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      super.start(env);
      if (env instanceof RegionCoprocessorEnvironment) {
        this.env = (RegionCoprocessorEnvironment) env;
      } else {
        throw new CoprocessorException("Must be loaded on a table region!");
      }
    }

    @Override
    public void createTable(RpcController controller, MetaDataProtos.CreateTableRequest request,
            RpcCallback<MetaDataProtos.MetaDataResponse> done) {
      // Invoke the actual create table routine
      super.createTable(controller, request, done);

      byte[][] rowKeyMetaData = new byte[3][];
      byte[] schemaName = null;
      byte[] tableName = null;
      String fullTableName = null;

      // Get the singleton connection for testing
      org.apache.hadoop.hbase.client.Connection
              conn = ServerUtil.ConnectionFactory.getConnection(
                      ServerUtil.ConnectionType.DEFAULT_SERVER_CONNECTION, env);
      try {
        // Get the current table creation details
        List<Mutation> tableMetadata = ProtobufUtil.getMutations(request);
        MetaDataUtil.getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
        schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);

        ThreadPoolExecutor ctpe = null;
        ThreadPoolExecutor htpe = null;

        // Get the thread pool executor from the connection.
        if (conn instanceof ConnectionImplementation) {
          ConnectionImplementation connImpl = ((ConnectionImplementation) conn);
          Field props = null;
          props = ConnectionImplementation.class.getDeclaredField("batchPool");
          props.setAccessible(true);
          ctpe = (ThreadPoolExecutor) props.get(connImpl);
          LOGGER.debug("ConnectionImplementation Thread pool info :" + ctpe.toString());

        }

        // Get the thread pool executor from the HTable.
        Table hTable = ServerUtil.getHTableForCoprocessorScan(env, TableName.valueOf(fullTableName));
        if (hTable instanceof HTable) {
          HTable testTable = (HTable) hTable;
          Field props = testTable.getClass().getDeclaredField("pool");
          props.setAccessible(true);
          htpe = ((ThreadPoolExecutor) props.get(hTable));
          LOGGER.debug("HTable Thread pool info :" + htpe.toString());
          // Assert the HTable thread pool config match the Connection pool configs.
          // Since we are not overriding any defaults, it should match the defaults.
          assertEquals(htpe.getMaximumPoolSize(), DEFAULT_HCONNECTION_POOL_MAX_SIZE);
          assertEquals(htpe.getCorePoolSize(), DEFAULT_HCONNECTION_POOL_CORE_SIZE);
          LOGGER.debug("HTable threadpool info {}, {}, {}, {}",
                  htpe.getCorePoolSize(),
                  htpe.getMaximumPoolSize(),
                  htpe.getQueue().remainingCapacity(),
                  htpe.getKeepAliveTime(TimeUnit.SECONDS));

          int count = Thread.activeCount();
          Thread[] th = new Thread[count];
          // returns the number of threads put into the array
          Thread.enumerate(th);
          long hTablePoolCount = Arrays.stream(th).filter(
                  s -> s.getName().equals("htable-pool-0")).count();
          // Assert no default HTable threadpools are created.
          assertEquals(0, hTablePoolCount);
          LOGGER.debug("htable-pool-0 threads {}", hTablePoolCount);
        }
        // Assert that the threadpool from Connection and HTable are the same.
        assertEquals(ctpe, htpe);
      } catch (RuntimeException | NoSuchFieldException | IllegalAccessException | IOException t) {
        // handle cases that an IOE is wrapped inside a RuntimeException
        // like HTableInterface#createHTableInterface
        MetaDataProtos.MetaDataResponse.Builder builder =
                MetaDataProtos.MetaDataResponse.newBuilder();

        LOGGER.error("This is unexpected");
        ProtobufUtil.setControllerException(controller,
                ClientUtil.createIOException(
                        SchemaUtil.getPhysicalTableName(
                                        PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                                        false)
                                .toString(),
                        new DoNotRetryIOException("Not allowed")));
        done.run(builder.build());

      }

    }


    @Override
    public void getVersion(RpcController controller, MetaDataProtos.GetVersionRequest request,
      RpcCallback<MetaDataProtos.GetVersionResponse> done) {
      MetaDataProtos.GetVersionResponse.Builder builder =
        MetaDataProtos.GetVersionResponse.newBuilder();
      LOGGER.error("This is unexpected");
      ProtobufUtil.setControllerException(controller,
        ClientUtil.createIOException(
          SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                          false)
            .toString(),
          new DoNotRetryIOException("Not allowed")));
      builder.setVersion(-1);
      done.run(builder.build());
    }
  }

  @Test
  public void testViewCreationAndServerConnections() throws Throwable {
    final String tableName = generateUniqueName();
    final String view01 = "v01_" + tableName;
    final String view02 = "v02_" + tableName;
    final String index_view01 = "idx_v01_" + tableName;
    final String index_view02 = "idx_v02_" + tableName;
    final String index_view03 = "idx_v03_" + tableName;
    final String index_view04 = "idx_v04_" + tableName;
    final int NUM_VIEWS = 50;

    TestMetaDataEndpointImpl.setTestCreateView(true);
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
      TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);

      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName
              + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
              + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))"
              + " UPDATE_CACHE_FREQUENCY=ALWAYS, MULTI_TENANT=true");
      conn.commit();

      for (int i=0; i< NUM_VIEWS; i++) {
        Properties props = new Properties();
        String viewTenantId = String.format("00T%012d", i);
        props.setProperty(TENANT_ID_ATTRIB, viewTenantId);
        // Create multilevel tenant views
        try (Connection tConn = DriverManager.getConnection(getUrl(), props)) {
          final Statement viewStmt = tConn.createStatement();
          viewStmt.execute("CREATE VIEW " + view01
                  + " (VCOL1 CHAR(8), COL5 VARCHAR) AS SELECT * FROM " + tableName
                  + " WHERE COL2 = 'col2'");

          viewStmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)"
                  + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
          tConn.commit();

          // Create multilevel tenant indexes
          final Statement indexStmt = tConn.createStatement();
          indexStmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                  + "(COL1, COL2, COL3)");
          indexStmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                  + "(COL1, COL2, COL3)");
          indexStmt.execute("CREATE INDEX " + index_view03 + " ON " + view01 + " (COL5) INCLUDE "
                  + "(COL2, COL1)");
          indexStmt.execute("CREATE INDEX " + index_view04 + " ON " + view02 + " (COL6) INCLUDE "
                  + "(COL2, COL1)");

          tConn.commit();

        }

      }

      TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
      TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
    }
  }

  @Test
  public void testConnectionFromMetadataServer() throws Throwable {
    final String tableName = generateUniqueName();
    final String view01 = "v01_" + tableName;
    final String view02 = "v02_" + tableName;
    final String index_view01 = "idx_v01_" + tableName;
    final String index_view02 = "idx_v02_" + tableName;
    final String index_view03 = "idx_v03_" + tableName;
    final String index_view04 = "idx_v04_" + tableName;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName
        + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
        + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))"
        + " UPDATE_CACHE_FREQUENCY=ALWAYS");
      stmt.execute("CREATE VIEW " + view01 + " (VCOL1 CHAR(8), COL5 VARCHAR) AS SELECT * FROM "
        + tableName + " WHERE COL1 = 'col1'");
      stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)"
        + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");

      conn.commit();

      TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
      TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);

      stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
        + "(COL1, COL2, COL3)");
      stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
        + "(COL1, COL2, COL3)");
      stmt.execute(
        "CREATE INDEX " + index_view03 + " ON " + view01 + " (COL5) INCLUDE " + "(COL2, COL1)");
      stmt.execute(
        "CREATE INDEX " + index_view04 + " ON " + view02 + " (COL6) INCLUDE " + "(COL2, COL1)");

      stmt.execute("UPSERT INTO " + view02
        + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', " + "'col6_01')");
      stmt.execute("UPSERT INTO " + view02
        + " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
      stmt.execute("UPSERT INTO " + view02
        + " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
        + "('0004', 'vcol2', 'col3_04', 'col4_04', 'col5_04')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
        + "('0005', 'vcol-2', 'col3_05', 'col4_05', 'col5_05')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
        + "('0006', 'vcol-1', 'col3_06', 'col4_06', 'col5_06')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
        + "('0007', 'vcol1', 'col3_07', 'col4_07', 'col5_07')");
      stmt.execute("UPSERT INTO " + view02
        + " (col2, vcol2, col5, col6) values ('0008', 'vcol2_08', 'col5_08', 'col6_02')");
      conn.commit();

      final Statement statement = conn.createStatement();
      ResultSet rs = statement.executeQuery("SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " +
              view02);
      // No need to verify each row result as the primary focus of this test is to ensure
      // no RPC call from MetaDataEndpointImpl to MetaDataEndpointImpl is done while
      // creating server side connection.
      Assert.assertTrue(rs.next());
      Assert.assertTrue(rs.next());
      Assert.assertTrue(rs.next());
      Assert.assertTrue(rs.next());
      Assert.assertTrue(rs.next());
      Assert.assertFalse(rs.next());

      TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
      TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
    }
  }

}
