/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static org.apache.phoenix.query.QueryServices.DISABLE_VIEW_SUBTREE_VALIDATION;

/**
 * Tests to ensure connection creation by metadata coproc does not need to make
 * RPC call to metada coproc internally.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class MetadataServerConnectionsIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataServerConnectionsIT.class);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        props.put(DISABLE_VIEW_SUBTREE_VALIDATION, "true");
        setUpTestDriver(new ReadOnlyProps(props));
    }

    public static class TestMetaDataEndpointImpl extends MetaDataEndpointImpl {

        @Override
        public void getVersion(RpcController controller, MetaDataProtos.GetVersionRequest request,
                               RpcCallback<MetaDataProtos.GetVersionResponse> done) {
            MetaDataProtos.GetVersionResponse.Builder builder =
                    MetaDataProtos.GetVersionResponse.newBuilder();
            LOGGER.error("This is unexpected");
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(
                            SchemaUtil.getPhysicalTableName(
                                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, false)
                                    .toString(),
                            new DoNotRetryIOException("Not allowed")));
            builder.setVersion(-1);
            done.run(builder.build());
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
            stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8), COL5 VARCHAR) AS SELECT * FROM " + tableName
                    + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)"
                    + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");

            conn.commit();

            TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
            TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);

            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                    + "(COL1, COL2, COL3)");
            stmt.execute("CREATE INDEX " + index_view03 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL2, COL1)");
            stmt.execute("CREATE INDEX " + index_view04 + " ON " + view02 + " (COL6) INCLUDE "
                    + "(COL2, COL1)");

            stmt.execute("UPSERT INTO " + view02
                    + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', " +
                    "'col6_01')");
            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0004', 'vcol2', 'col3_04', 'col4_04', 'col5_04')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0005', 'vcol-2', 'col3_05', 'col4_05', 'col5_05')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0006', 'vcol-1', 'col3_06', 'col4_06', 'col5_06')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0007', 'vcol1', 'col3_07', 'col4_07', 'col5_07')");
            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0008', 'vcol2_08', 'col5_08', 'col6_02')");
            conn.commit();

            final Statement statement = conn.createStatement();
            ResultSet rs =
                    statement.executeQuery(
                            "SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02);
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
