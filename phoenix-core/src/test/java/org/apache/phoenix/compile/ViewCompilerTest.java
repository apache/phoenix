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
package org.apache.phoenix.compile;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class ViewCompilerTest extends BaseConnectionlessQueryTest {
    @Test
    public void testViewTypeCalculation() throws Exception {
        assertViewType(new String[] {
            "CREATE VIEW v1 AS SELECT * FROM t WHERE k1 = 1 AND k2 = 'foo'",
            "CREATE VIEW v2 AS SELECT * FROM t WHERE k2 = 'foo'",
            "CREATE VIEW v3 AS SELECT * FROM t WHERE v = 'bar'||'bas'",
            "CREATE VIEW v4 AS SELECT * FROM t WHERE 'bar'=v and 5+3/2 = k1",
        }, ViewType.UPDATABLE);
        assertViewType(new String[] {
                "CREATE VIEW v1 AS SELECT * FROM t WHERE k1 < 1 AND k2 = 'foo'",
                "CREATE VIEW v2 AS SELECT * FROM t WHERE substr(k2,0,3) = 'foo'",
                "CREATE VIEW v3 AS SELECT * FROM t WHERE v = TO_CHAR(CURRENT_DATE())",
                "CREATE VIEW v4 AS SELECT * FROM t WHERE 'bar'=v or 3 = k1",
            }, ViewType.READ_ONLY);
    }
    
    public void assertViewType(String[] views, ViewType viewType) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        String ct = "CREATE TABLE t (k1 INTEGER NOT NULL, k2 VARCHAR, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2))";
        conn.createStatement().execute(ct);
        
        for (String view : views) {
            conn.createStatement().execute(view);
        }
        
        StringBuilder buf = new StringBuilder();
        int count = 0;
        for (PTable table : conn.getMetaDataCache()) {
            if (table.getType() == PTableType.VIEW) {
                assertEquals(viewType, table.getViewType());
                conn.createStatement().execute("DROP VIEW " + table.getName().getString());
                buf.append(' ');
                buf.append(table.getName().getString());
                count++;
            }
        }
        assertEquals("Expected " + views.length + ", but got " + count + ":"+ buf.toString(), views.length, count);
    }

    @Test
    public void testViewInvalidation() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        String ct = "CREATE TABLE s1.t (k1 INTEGER NOT NULL, k2 VARCHAR, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2))";
        conn.createStatement().execute(ct);
        conn.createStatement().execute("CREATE VIEW s2.v3 AS SELECT * FROM s1.t WHERE v = 'bar'");
        
        // TODO: should it be an error to remove columns from a VIEW that we're defined there?
        // TOOD: should we require an ALTER VIEW instead of ALTER TABLE?
        conn.createStatement().execute("ALTER VIEW s2.v3 DROP COLUMN v");
        try {
            conn.createStatement().executeQuery("SELECT * FROM s2.v3");
            fail();
        } catch (ColumnNotFoundException e) {
            
        }
        
        // No error, as v still exists in t
        conn.createStatement().execute("CREATE VIEW s2.v4 AS SELECT * FROM s1.t WHERE v = 'bas'");

        // No error, even though view is invalid
        conn.createStatement().execute("DROP VIEW s2.v3");
    }


    @Test
    public void testInvalidUpsertSelect() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        conn.createStatement().execute("CREATE TABLE t1 (k1 INTEGER NOT NULL, k2 VARCHAR, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2))");
        conn.createStatement().execute("CREATE TABLE t2 (k3 INTEGER NOT NULL, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k3))");
        conn.createStatement().execute("CREATE VIEW v1 AS SELECT * FROM t1 WHERE k1 = 1");
        
        try {
            conn.createStatement().executeUpdate("UPSERT INTO v1 SELECT k3,'foo',v FROM t2");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }
    }
}
