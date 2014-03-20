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

import static org.apache.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import org.apache.phoenix.util.PhoenixRuntime;


public class CoalesceFunctionIT extends BaseClientManagedTimeIT {
    @Test
    public void testCoalesce() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT entity_id, a_integer + COALESCE(x_integer,1) FROM ATABLE WHERE organization_id = ? AND a_integer >= 6 AND a_integer <= 7";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        
        assertTrue (rs.next());
        assertEquals(ROW6, rs.getString(1));
        assertEquals(7, rs.getInt(2));
        
        assertTrue (rs.next());
        assertEquals(ROW7, rs.getString(1));
        assertEquals(12, rs.getInt(2));
        
        assertFalse(rs.next());
        conn.close();
    }

}
