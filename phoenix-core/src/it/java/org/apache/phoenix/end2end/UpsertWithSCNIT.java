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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@Category(ParallelStatsDisabledTest.class)
public class UpsertWithSCNIT extends ParallelStatsDisabledIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    Properties props = null;
    private PreparedStatement prep = null;
    String tableName =null;

    private void helpTestUpsertWithSCNIT(boolean rowColumn, boolean txTable,
                                        boolean mutable, boolean local, boolean global)
            throws SQLException {

        tableName = generateUniqueName();
        String indx;
        String createTable = "CREATE TABLE "+tableName+" ("
                + (rowColumn ? "CREATED_DATE DATE NOT NULL, ":"")
                + "METRIC_ID CHAR(15) NOT NULL,METRIC_VALUE VARCHAR(50) CONSTRAINT PK PRIMARY KEY("
                + (rowColumn? "CREATED_DATE ROW_TIMESTAMP, ":"") + "METRIC_ID)) "
                + "IMMUTABLE_ROWS=" + (mutable? "false" : "true" )
                + (txTable ? ", TRANSACTION_PROVIDER='OMID',TRANSACTIONAL=true":"");
        props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(createTable);

        if(local || global ){
            indx = "CREATE "+ (local? "LOCAL " : "") + "INDEX "+tableName+"_idx ON " +
                    ""+tableName+" (METRIC_VALUE)";
            conn.createStatement().execute(indx);
        }

        props.setProperty("CurrentSCN", Long.toString(System.currentTimeMillis()));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String upsert = "UPSERT INTO "+tableName+" (METRIC_ID, METRIC_VALUE) VALUES (?,?)";
        prep = conn.prepareStatement(upsert);
        prep.setString(1,"abc");
        prep.setString(2,"This is the first comment!");
    }

    @Test // See https://issues.apache.org/jira/browse/PHOENIX-4983
    public void testUpsertOnSCNSetTxnTable() throws SQLException {

        helpTestUpsertWithSCNIT(false, true, false, false, false);
        exception.expect(SQLException.class);
        exception.expectMessage(String.valueOf(
                SQLExceptionCode
                .CANNOT_SPECIFY_SCN_FOR_TXN_TABLE
                .getErrorCode()));
        prep.executeUpdate();
    }

    @Test
    public void testUpsertOnSCNSetMutTableWithoutIdx() throws Exception {

        helpTestUpsertWithSCNIT(false, false, true, false, false);
        prep.executeUpdate();
        props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(),props);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM "+tableName);
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertEquals("This is the first comment!", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testUpsertOnSCNSetTable() throws Exception {

        helpTestUpsertWithSCNIT(false, false, false, false, false);
        prep.executeUpdate();
        props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(),props);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM "+tableName);
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertEquals("This is the first comment!", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testUpsertOnSCNSetMutTableWithLocalIdx() throws Exception {

        helpTestUpsertWithSCNIT(false, false, true, true, false);
        exception.expect(SQLException.class);
        exception.expectMessage(String.valueOf(
                SQLExceptionCode
                .CANNOT_UPSERT_WITH_SCN_FOR_TABLE_WITH_INDEXES
                .getErrorCode()));
        prep.executeUpdate();
    }

    @Test
    public void testUpsertOnSCNSetImmutableTableWithLocalIdx() throws Exception {

        helpTestUpsertWithSCNIT(false, false, false, true, false);
        exception.expect(SQLException.class);
        exception.expectMessage(String.valueOf(
                SQLExceptionCode
                        .CANNOT_UPSERT_WITH_SCN_FOR_TABLE_WITH_INDEXES
                        .getErrorCode()));
        prep.executeUpdate();
    }

    @Test
    public void testUpsertOnSCNSetMutTableWithGlobalIdx() throws Exception {

        helpTestUpsertWithSCNIT(false, false, true, false, true);
        exception.expect(SQLException.class);
        exception.expectMessage(String.valueOf(
                SQLExceptionCode
                        .CANNOT_UPSERT_WITH_SCN_FOR_TABLE_WITH_INDEXES
                        .getErrorCode()));
        prep.executeUpdate();
    }

    @Test
    public void testUpsertOnSCNSetImmutableTableWithGlobalIdx() throws Exception {

        helpTestUpsertWithSCNIT(false, false, false, false, true);
        exception.expect(SQLException.class);
        exception.expectMessage(String.valueOf(
                SQLExceptionCode
                        .CANNOT_UPSERT_WITH_SCN_FOR_TABLE_WITH_INDEXES
                        .getErrorCode()));
        prep.executeUpdate();
    }

    @Test
    public void testUpsertOnSCNSetWithRowTSColumn() throws Exception {

        helpTestUpsertWithSCNIT(true, false, false, false, false);
        exception.expect(SQLException.class);
        exception.expectMessage(String.valueOf(
                SQLExceptionCode
                        .CANNOT_UPSERT_WITH_SCN_FOR_ROW_TIMESTAMP_COLUMN
                        .getErrorCode()));
        prep.executeUpdate();
    }
}

