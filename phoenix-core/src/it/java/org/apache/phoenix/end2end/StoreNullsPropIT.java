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

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.junit.Test;

public class StoreNullsPropIT extends ParallelStatsDisabledIT {

    @Test
    public void testSetStoreNullsDefaultViaConfig() throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.DEFAULT_STORE_NULLS_ATTRIB, "true");
        Connection storeNullsConn = DriverManager.getConnection(getUrl(), props);

        Statement stmt = storeNullsConn.createStatement();
        stmt.execute("CREATE TABLE with_nulls_default (" +
                "id smallint primary key," +
                "name varchar)");

        ResultSet rs = stmt.executeQuery("SELECT store_nulls FROM SYSTEM.CATALOG " +
                "WHERE table_name = 'WITH_NULLS_DEFAULT' AND store_nulls is not null");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }
    
}
