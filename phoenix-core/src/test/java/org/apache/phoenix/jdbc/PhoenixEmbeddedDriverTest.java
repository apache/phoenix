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

package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.junit.Test;

public class PhoenixEmbeddedDriverTest {
    @Test
    public void testGetConnectionInfo() throws SQLException {
        String[] urls = new String[] {
            "jdbc:phoenix",
            "jdbc:phoenix;test=true",
            "jdbc:phoenix:localhost",
            "jdbc:phoenix:localhost:123",
            "jdbc:phoenix:localhost:123;foo=bar",
            "jdbc:phoenix:localhost:123:/hbase",
            "jdbc:phoenix:localhost:123:/hbase;foo=bas",
            "jdbc:phoenix:localhost:/hbase",
            "jdbc:phoenix:localhost:/hbase;test=true",
            "jdbc:phoenix:v1,v2,v3",
            "jdbc:phoenix:v1,v2,v3;",
            "jdbc:phoenix:v1,v2,v3;test=true",
            "jdbc:phoenix:v1,v2,v3:/hbase",
            "jdbc:phoenix:v1,v2,v3:/hbase;test=true",
            "jdbc:phoenix:v1,v2,v3:123:/hbase",
            "jdbc:phoenix:v1,v2,v3:123:/hbase;test=false",
            "jdbc:phoenix:v1,v2,v3:123:/hbase:user/principal:/user.keytab;test=false",
            "jdbc:phoenix:v1,v2,v3:123:user/principal:/user.keytab;test=false",
            "jdbc:phoenix:v1,v2,v3:user/principal:/user.keytab;test=false",
            "jdbc:phoenix:v1,v2,v3:/hbase:user/principal:/user.keytab;test=false",
            "jdbc:phoenix:v1,v2,v3:LongRunningQueries;test=false",
            "jdbc:phoenix:v1,v2,v3:345:LongRunningQueries;test=false",
        };
        ConnectionInfo[] infos = new ConnectionInfo[] {
            new ConnectionInfo(null,null,null),
            new ConnectionInfo(null,null,null),
            new ConnectionInfo("localhost",null,null),
            new ConnectionInfo("localhost",123,null),
            new ConnectionInfo("localhost",123,null),
            new ConnectionInfo("localhost",123,"/hbase"),
            new ConnectionInfo("localhost",123,"/hbase"),
            new ConnectionInfo("localhost",null,"/hbase"),
            new ConnectionInfo("localhost",null,"/hbase"),
            new ConnectionInfo("v1,v2,v3",null,null),
            new ConnectionInfo("v1,v2,v3",null,null),
            new ConnectionInfo("v1,v2,v3",null,null),
            new ConnectionInfo("v1,v2,v3",null,"/hbase"),
            new ConnectionInfo("v1,v2,v3",null,"/hbase"),
            new ConnectionInfo("v1,v2,v3",123,"/hbase"),
            new ConnectionInfo("v1,v2,v3",123,"/hbase"),
            new ConnectionInfo("v1,v2,v3",123,"/hbase","user/principal", "/user.keytab" ),
            new ConnectionInfo("v1,v2,v3",123, null,"user/principal", "/user.keytab" ),
            new ConnectionInfo("v1,v2,v3", null, null,"user/principal", "/user.keytab" ),
            new ConnectionInfo("v1,v2,v3",null,"/hbase","user/principal", "/user.keytab" ),
            new ConnectionInfo("v1,v2,v3",null,null,"LongRunningQueries", null ),
            new ConnectionInfo("v1,v2,v3",345,null,"LongRunningQueries", null ),
        };
        assertEquals(urls.length,infos.length);
        for (int i = 0; i < urls.length; i++) {
            try {
                ConnectionInfo info = ConnectionInfo.create(urls[i]);
                assertEquals(infos[i], info);
            } catch (AssertionError e) {
                throw new AssertionError("For \"" + urls[i] + "\": " + e.getMessage());
            }
        }
    }
    @Test
    public void testNegativeGetConnectionInfo() throws SQLException {
        String[] urls = new String[] {
            "jdbc:phoenix::",
            "jdbc:phoenix:;",
            "jdbc:phoenix:v1:1,v2:2,v3:3",
            "jdbc:phoenix:v1:1,v2:2,v3:3;test=true",
            "jdbc:phoenix:v1,v2,v3:-1:/hbase;test=true",
            "jdbc:phoenix:v1,v2,v3:-1",
            "jdbc:phoenix:v1,v2,v3:123::/hbase",
            "jdbc:phoenix:v1,v2,v3:123::/hbase;test=false",
        };
        for (String url : urls) {
            try {
                ConnectionInfo.create(url);
                throw new AssertionError("Expected exception for \"" + url + "\"");
            } catch (SQLException e) {
                try {
                    assertEquals(SQLExceptionCode.MALFORMED_CONNECTION_URL.getSQLState(), e.getSQLState());
                } catch (AssertionError ae) {
                    throw new AssertionError("For \"" + url + "\": " + ae.getMessage());
                }
            }
        }
    }
    
    @Test
    public void testNotAccept() throws Exception {
        Driver driver = new PhoenixDriver();
        assertFalse(driver.acceptsURL("jdbc:phoenix://localhost"));
        assertFalse(driver.acceptsURL("jdbc:phoenix:localhost;test=true;bar=foo"));
        assertFalse(driver.acceptsURL("jdbc:phoenix:localhost;test=true"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123;untest=true"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123;untest=true;foo=bar"));
        DriverManager.deregisterDriver(driver);
    }
}
