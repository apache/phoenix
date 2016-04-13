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
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class ViewIndexIT extends BaseHBaseManagedTimeIT {

    private String VIEW_NAME = "MY_VIEW";
    private String schemaName="TEST";
    private boolean isNamespaceMapped;
    private String tableName = schemaName + ".T";
    private String indexName = "I";
    private String viewIndexPhysicalTableName;
    private TableName physicalTableName;

    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Drop the HBase table metadata for this test to confirm that view index table dropped
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Parameters(name = "isNamespaceMapped = {0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(true, false);
    }

    private void createBaseTable(String tableName, Integer saltBuckets, String splits) throws SQLException {
        Connection conn = getConnection();
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n"
                        + (saltBuckets == null || splits != null ? "" : (",salt_buckets=" + saltBuckets)
                        + (saltBuckets != null || splits == null ? "" : ",splits=" + splits));
        conn.createStatement().execute(ddl);
        conn.close();
    }

    public Connection getConnection() throws SQLException{
        Properties props = new Properties();
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        props.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return DriverManager.getConnection(getUrl(),props);
    }
    
    public ViewIndexIT(boolean isNamespaceMapped) {
        this.isNamespaceMapped = isNamespaceMapped;
        this.physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        this.viewIndexPhysicalTableName = MetaDataUtil.getLocalIndexTableName(physicalTableName.getNameAsString());
    }

    @Test
    public void testDeleteViewIndexSequences() throws Exception {
        createBaseTable(tableName, null, null);
        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        conn1.createStatement().execute("CREATE VIEW " + VIEW_NAME + " AS SELECT * FROM " + tableName);
        conn1.createStatement().execute("CREATE INDEX " + indexName + " ON " + VIEW_NAME + " (v1)");
        conn2.createStatement().executeQuery("SELECT * FROM " + tableName).next();
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        conn1.createStatement().execute("DROP VIEW " + VIEW_NAME);
        conn1.createStatement().execute("DROP TABLE "+ tableName);
        admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        assertFalse("View index table should be deleted.", admin.tableExists(TableName.valueOf(viewIndexPhysicalTableName)));
        ResultSet rs = conn2.createStatement().executeQuery("SELECT "
                + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + ","
                + PhoenixDatabaseMetaData.SEQUENCE_NAME
                + " FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE);
        assertFalse("View index sequences should be deleted.", rs.next());
    }
}