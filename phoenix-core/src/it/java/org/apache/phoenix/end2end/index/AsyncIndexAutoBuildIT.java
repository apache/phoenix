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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.junit.Test;

public class AsyncIndexAutoBuildIT extends BaseHBaseManagedTimeIT {
    
    @Test
    public void testAsyncIndexAutoBuild() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        AsyncIndexTestUtil.createTableAndLoadData(stmt);
        AsyncIndexTestUtil.createAsyncIndex(stmt);

        ResultSet rs = stmt.executeQuery(AsyncIndexTestUtil.PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        assertTrue(rs.next());

        AsyncIndexTestUtil.retryWithSleep(4, 5, stmt);

        rs = stmt.executeQuery(AsyncIndexTestUtil.PERSON_TABLE_ASYNC_INDEX_INFO_QUERY);
        assertFalse(rs.next());
    }
}
