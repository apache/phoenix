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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Types;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class QueryUtilTest {

    private static final ColumnInfo ID_COLUMN = new ColumnInfo("ID", Types.BIGINT);
    private static final ColumnInfo NAME_COLUMN = new ColumnInfo("NAME", Types.VARCHAR);

    @Test
    public void testConstructUpsertStatement_ColumnInfos() {
        assertEquals(
                "UPSERT  INTO MYTAB (\"ID\", \"NAME\") VALUES (?, ?)",
                QueryUtil.constructUpsertStatement("MYTAB", ImmutableList.of(ID_COLUMN, NAME_COLUMN)));

    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructUpsertStatement_ColumnInfos_NoColumns() {
        QueryUtil.constructUpsertStatement("MYTAB", ImmutableList.<ColumnInfo>of());
    }

    @Test
    public void testConstructGenericUpsertStatement() {
        assertEquals(
                "UPSERT INTO MYTAB VALUES (?, ?)",
                QueryUtil.constructGenericUpsertStatement("MYTAB", 2));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructGenericUpsertStatement_NoColumns() {
        QueryUtil.constructGenericUpsertStatement("MYTAB", 0);
    }
    
    @Test
    public void testConstructSelectStatement() {
        assertEquals(
                "SELECT \"ID\",\"NAME\" FROM MYTAB",
                QueryUtil.constructSelectStatement("MYTAB", ImmutableList.of(ID_COLUMN,NAME_COLUMN),null));
    }

    @Test
    public void testConstructSelectStatementWithSchema() {
        assertEquals(
                "SELECT \"ID\",\"NAME\" FROM A.MYTAB",
                QueryUtil.constructSelectStatement("A.MYTAB", ImmutableList.of(ID_COLUMN,NAME_COLUMN),null));
    }
    
    @Test
    public void testConstructSelectStatementWithCaseSensitiveSchema() {
        final String tableName = "MYTAB";
        final String schemaName = SchemaUtil.getEscapedArgument("a");
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        assertEquals(
                "SELECT \"ID\",\"NAME\" FROM \"a\".MYTAB",
                QueryUtil.constructSelectStatement(fullTableName, ImmutableList.of(ID_COLUMN,NAME_COLUMN),null));
    }
    
    @Test
    public void testConstructSelectStatementWithCaseSensitiveTable() {
        final String tableName = SchemaUtil.getEscapedArgument("mytab");
        final String schemaName = SchemaUtil.getEscapedArgument("a");
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        assertEquals(
                "SELECT \"ID\",\"NAME\" FROM \"a\".\"mytab\"",
                QueryUtil.constructSelectStatement(fullTableName, ImmutableList.of(ID_COLUMN,NAME_COLUMN),null));
    }
    
    /**
     * Test that we create connection strings from the HBase Configuration that match the
     * expected syntax. Expected to log exceptions as it uses ZK host names that don't exist
     * @throws Exception on failure
     */
    @Test
    public void testCreateConnectionFromConfiguration() throws Exception {
        Properties props = new Properties();
        // standard lookup. this already checks if we set hbase.zookeeper.clientPort
        Configuration conf = new Configuration(false);
        conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        String conn = QueryUtil.getConnectionUrl(props, conf);
        validateUrl(conn);

        // set the zks to a few hosts, some of which are no online
        conf.set(HConstants.ZOOKEEPER_QUORUM, "host.at.some.domain.1,localhost," +
                "host.at.other.domain.3");
        conn = QueryUtil.getConnectionUrl(props, conf);
        validateUrl(conn);

        // and try with different leader/peer ports
        conf.set("hbase.zookeeper.peerport", "3338");
        conf.set("hbase.zookeeper.leaderport", "3339");
        conn = QueryUtil.getConnectionUrl(props, conf);
        validateUrl(conn);
    }

    private void validateUrl(String url) {
        String prefix = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
        assertTrue("JDBC URL missing jdbc protocol prefix", url.startsWith(prefix));
        assertTrue("JDBC URL missing jdbc terminator suffix", url.endsWith(";"));
        // remove the prefix, should only be left with server[,server...]:port:/znode
        url = url.substring(prefix.length());
        String[] splits = url.split(":");
        assertTrue("zk details should contain at least server component", splits.length >= 1);
        // make sure that each server is comma separated
        String[] servers = splits[0].split(",");
        for(String server: servers){
            assertFalse("Found whitespace in server names for url: " + url, server.contains(" "));
        }
        if (splits.length >= 2) {
            // second bit is a port number, should not through
            try {
                Integer.parseInt(splits[1]);
            } catch (NumberFormatException e) {
                fail(e.getMessage());
            }
        }
        if (splits.length >= 3) {
            assertTrue("znode parent is not an absolute path", splits[2].startsWith("/"));
        }
    }
}