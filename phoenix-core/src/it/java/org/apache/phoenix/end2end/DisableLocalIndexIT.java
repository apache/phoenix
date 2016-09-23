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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

public class DisableLocalIndexIT extends ParallelStatsDisabledIT {

    @Test
    public void testDisabledLocalIndexes() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(QueryServices.ALLOW_LOCAL_INDEX_ATTRIB, Boolean.FALSE.toString());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String baseName = generateUniqueName();
        String tableName = baseName+ "_TABLE";
        String viewName = baseName + "_VIEW";
        String indexName1 = baseName + "_INDEX1";
        String indexName2 = baseName + "_INDEX2";
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT PK PRIMARY KEY(K1,K2)) MULTI_TENANT=true");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('t1','x')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('t2','y')");
        HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        assertFalse(admin.tableExists(Bytes.toBytes(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + tableName)));
        admin.close();
        try {
            HTableInterface t = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + tableName));
            t.getTableDescriptor(); // Exception no longer thrown by getTable, but instead need to force an RPC
            fail("Local index table should not have been created");
        } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            //expected
        } finally {
            admin.close();
        }
        
        Properties tsconnProps = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        tsconnProps.setProperty(QueryServices.ALLOW_LOCAL_INDEX_ATTRIB, Boolean.FALSE.toString());
        tsconnProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "t1");
        Connection tsconn = DriverManager.getConnection(getUrl(), tsconnProps);
        
        tsconn.createStatement().execute("CREATE VIEW " + viewName + "(V1 VARCHAR) AS SELECT * FROM " + tableName);
        tsconn.createStatement().execute("CREATE INDEX " + indexName1 + " ON " + viewName + "(V1)");
        tsconn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(MetaDataUtil.VIEW_INDEX_TABLE_PREFIX + tableName));

        try {
            conn.createStatement().execute("CREATE LOCAL INDEX " + indexName2 + " ON " + tableName + "(k2)");
            fail("Should not allow creation of local index");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UNALLOWED_LOCAL_INDEXES.getErrorCode(), e.getErrorCode());
        }
        try {
            tsconn.createStatement().execute("CREATE LOCAL INDEX " + indexName2 + " ON " + viewName + "(k2, v1)");
            fail("Should not allow creation of local index");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UNALLOWED_LOCAL_INDEXES.getErrorCode(), e.getErrorCode());
        }
    }

}
