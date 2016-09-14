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
package org.apache.phoenix.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.phoenix.util.TestUtil;
import org.junit.Before;

public class BaseCalciteIndexIT extends BaseCalciteIT {
    
    private final boolean localIndex;
    
    public BaseCalciteIndexIT(boolean localIndex) {
        this.localIndex = localIndex;
    }
    
    @Before
    public void initTable() throws Exception {
        final String url = getOldUrl();
        final String index = localIndex ? "LOCAL INDEX" : "INDEX";
        initATableValues(TestUtil.ATABLE_NAME, getOrganizationId(), null, null, null, url);
        initSaltedTables(url, index);
        initMultiTenantTables(url, index);
        Connection connection = DriverManager.getConnection(url);
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX1 ON aTable (a_string) INCLUDE (b_string, x_integer)");
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX2 ON aTable (b_string) INCLUDE (a_string, y_integer)");
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX_FULL ON aTable (b_string) INCLUDE (a_string, a_integer, a_date, a_time, a_timestamp, x_decimal, x_long, x_integer, y_integer, a_byte, a_short, a_float, a_double, a_unsigned_float, a_unsigned_double)");
        connection.createStatement().execute("UPDATE STATISTICS ATABLE");
        connection.createStatement().execute("UPDATE STATISTICS " + NOSALT_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + SALTED_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + MULTI_TENANT_TABLE);
        connection.close();
        
        Properties props = new Properties();
        props.setProperty("TenantId", "10");
        connection = DriverManager.getConnection(url, props);
        connection.createStatement().execute("UPDATE STATISTICS " + MULTI_TENANT_VIEW1);
        connection.close();
        
        props.setProperty("TenantId", "20");
        connection = DriverManager.getConnection(url, props);
        connection.createStatement().execute("UPDATE STATISTICS " + MULTI_TENANT_VIEW2);
        connection.close();
    }

}
