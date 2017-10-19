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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UngroupedAggregateRegionObserverIT extends ParallelStatsDisabledIT {

    private String dataTableName;
    private String indexTableName;
    private String schemaName;
    private String dataTableFullName;
    private static String indexTableFullName;

    @Mock
    private Appender mockAppender;

    @Captor
    private ArgumentCaptor<LoggingEvent> captorLoggingEvent;
    private UngroupedAggregateRegionObserver ungroupedObserver;

    @Before
    public void setup() {
        ungroupedObserver = new UngroupedAggregateRegionObserver();
        ungroupedObserver.setCompactionConfig(PropertiesUtil.cloneConfig(config));
    }

    /**
     * Tests the that post compact hook doesn't log any NPE for a System table
     */
    @Test
    public void testPostCompactSystemSequence() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            startCapturingIndexLog();
            // run the post-compact hook
            ungroupedObserver.clearTsOnDisabledIndexes("SYSTEM.SEQUENCE");
            stopCapturingIndexLog();
            // uneventful - nothing should be logged
            Mockito.verify(mockAppender, never())
                    .doAppend((LoggingEvent) captorLoggingEvent.capture());
        }
    }

    /**
     * Tests that calling the post compact hook on the data table permanently disables an index that
     * is being rebuilt (i.e. already disabled or inactive)
     */
    @Test
    public void testPostCompactDataTableDuringRebuild() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            generateUniqueTableNames();
            testRebuildPostCompact(conn, dataTableFullName);
        }
    }

    /**
     * Tests that calling the post compact hook on the index table permanently disables an index
     * that is being rebuilt (i.e. already disabled or inactive)
     */
    @Test
    public void testPostCompactIndexTableDuringRebuild() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            generateUniqueTableNames();
            testRebuildPostCompact(conn, indexTableFullName);
        }
    }

    private void testRebuildPostCompact(Connection conn, String tableToCompact)
            throws SQLException {
        conn.createStatement().execute(
            String.format(PartialScannerResultsDisabledIT.TEST_TABLE_DDL, dataTableFullName));
        conn.createStatement().execute(String.format(PartialScannerResultsDisabledIT.INDEX_1_DDL,
            indexTableName, dataTableFullName));
        // disable the index, simulating an index write failure
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        IndexUtil.updateIndexState(pConn, indexTableFullName, PIndexState.DISABLE,
            EnvironmentEdgeManager.currentTimeMillis());

        // run the post-compact hook on the data table
        startCapturingIndexLog();
        ungroupedObserver.clearTsOnDisabledIndexes(tableToCompact);
        stopCapturingIndexLog();
        // an event should've been logged
        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        assertThat(loggingEvent.getLevel(), is(Level.INFO));
        // index should be permanently disabled (disabletime of 0)
        assertTrue(TestUtil.checkIndexState(pConn, indexTableFullName, PIndexState.DISABLE, 0L));
    }

    /**
     * Tests that a non-Phoenix table (created purely through HBase) doesn't log a warning in
     * postCompact
     */
    @Test
    public void testPostCompactTableNotFound() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            HBaseTestingUtility utility = getUtility();
            String nonPhoenixTable = "NOT_A_PHOENIX_TABLE";
            utility.getHBaseAdmin().createTable(utility.createTableDescriptor(nonPhoenixTable));
            startCapturingIndexLog();
            ungroupedObserver.clearTsOnDisabledIndexes(nonPhoenixTable);
            stopCapturingIndexLog();
            // a debug level event should've been logged
            Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
            LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
            assertThat(loggingEvent.getLevel(), is(Level.DEBUG));
        }
    }

    private void stopCapturingIndexLog() {
        LogManager.getLogger(UngroupedAggregateRegionObserver.class).removeAppender(mockAppender);
    }

    private void startCapturingIndexLog() {
        LogManager.getLogger(UngroupedAggregateRegionObserver.class).addAppender(mockAppender);
    }

    private void generateUniqueTableNames() {
        schemaName = generateUniqueName();
        dataTableName = generateUniqueName() + "_DATA";
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        indexTableName = generateUniqueName() + "_IDX";
        indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
    }
}
