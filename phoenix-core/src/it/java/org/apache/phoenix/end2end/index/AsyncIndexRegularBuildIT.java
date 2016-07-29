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

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.phoenix.end2end.BaseOwnClusterHBaseManagedTimeIT;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class AsyncIndexRegularBuildIT extends BaseOwnClusterHBaseManagedTimeIT {

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put("phoenix.async.index.automatic.build", Boolean.toString(false));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()));
    }
    
    @Test
    public void testAsyncIndexRegularBuild() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        String tableName = "TBL_" + generateRandomString();
        String indexName = "IND_" + generateRandomString();
        AsyncIndexTestUtil.createTableAndLoadData(stmt, tableName);
        AsyncIndexTestUtil.createAsyncIndex(stmt, indexName, tableName);

        String personTableAsyncIndexInfoQuery = AsyncIndexTestUtil.getPersonTableAsyncIndexInfoQuery(tableName);
        ResultSet rs = stmt.executeQuery(personTableAsyncIndexInfoQuery);
        assertTrue(rs.next());

        AsyncIndexTestUtil.retryWithSleep(tableName, 4, 5, stmt);

        rs = stmt.executeQuery(personTableAsyncIndexInfoQuery);
        assertTrue(rs.next());
    }
}
