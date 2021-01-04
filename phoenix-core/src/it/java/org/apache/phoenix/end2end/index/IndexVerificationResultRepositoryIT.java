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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class IndexVerificationResultRepositoryIT extends ParallelStatsDisabledIT {

    @BeforeClass
    public static synchronized void setupClass() throws Exception {
        Map<String, String> props = Collections.emptyMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testReadResultRow() throws Exception {
        String tableName = "T" + generateUniqueName();
        String indexName = "I" + generateUniqueName();
        byte[] indexNameBytes = Bytes.toBytes(indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTableAndIndex(conn, tableName, indexName);
            IndexVerificationResultRepository resultRepository =
                new IndexVerificationResultRepository(conn, indexNameBytes);
            resultRepository.createResultTable(conn);
            byte[] regionOne = Bytes.toBytes("a.1.00000000000000000000");
            byte[] regionTwo = Bytes.toBytes("a.2.00000000000000000000");
            long scanMaxTs = EnvironmentEdgeManager.currentTimeMillis();
            IndexToolVerificationResult expectedResult = getExpectedResult(scanMaxTs);
            resultRepository.logToIndexToolResultTable(expectedResult, IndexTool.IndexVerifyType.BOTH,
                regionOne);
            resultRepository.logToIndexToolResultTable(expectedResult, IndexTool.IndexVerifyType.BOTH,
                regionTwo);
            IndexToolVerificationResult actualResult =
                resultRepository.getVerificationResult(conn, scanMaxTs, indexNameBytes);
            assertVerificationResult(expectedResult, actualResult);

        }
    }

    private void assertVerificationResult(IndexToolVerificationResult expectedResult, IndexToolVerificationResult actualResult) {
        assertEquals(expectedResult.getScanMaxTs(), actualResult.getScanMaxTs());
        assertArrayEquals(expectedResult.getStartRow(), actualResult.getStartRow());
        assertArrayEquals(actualResult.getStopRow(), actualResult.getStopRow());

        //because we're combining two near-identical rows (same values, different region)
        //we assert on 2x the expected value
        assertEquals(2 * expectedResult.getBeforeRebuildExpiredIndexRowCount(), 
            actualResult.getBeforeRebuildExpiredIndexRowCount());
        assertEquals(2 * expectedResult.getBeforeRebuildInvalidIndexRowCount(),
            actualResult.getBeforeRebuildInvalidIndexRowCount());
        assertEquals(2 * expectedResult.getBeforeRebuildMissingIndexRowCount(),
            actualResult.getBeforeRebuildMissingIndexRowCount());
        assertEquals(2 * expectedResult.getBeforeRebuildValidIndexRowCount(),
            actualResult.getBeforeRebuildValidIndexRowCount());

        assertEquals(2 * expectedResult.getAfterRebuildExpiredIndexRowCount(),
            actualResult.getAfterRebuildExpiredIndexRowCount());
        assertEquals(2 * expectedResult.getAfterRebuildInvalidIndexRowCount(),
            actualResult.getAfterRebuildInvalidIndexRowCount());
        assertEquals(2 * expectedResult.getAfterRebuildMissingIndexRowCount(),
            actualResult.getAfterRebuildMissingIndexRowCount());
        assertEquals(2 * expectedResult.getAfterRebuildValidIndexRowCount(),
            actualResult.getAfterRebuildValidIndexRowCount());

        assertEquals(2 * expectedResult.getScannedDataRowCount(),
            actualResult.getScannedDataRowCount());
        assertEquals(2 * expectedResult.getRebuiltIndexRowCount(),
            actualResult.getRebuiltIndexRowCount());
        
    }

    private IndexToolVerificationResult getExpectedResult(long scanMaxTs) {
        byte[] startRow = Bytes.toBytes("a");
        byte[] stopRow = Bytes.toBytes("b");
        IndexToolVerificationResult result = new IndexToolVerificationResult(startRow, stopRow,
            scanMaxTs);
        IndexToolVerificationResult.PhaseResult before =
            new IndexToolVerificationResult.PhaseResult();

        IndexToolVerificationResult.PhaseResult after =
            new IndexToolVerificationResult.PhaseResult();
        result.setBefore(before);
        result.setAfter(after);
        return result;
    }

    private void createTable(Connection conn, String tableName) throws Exception {
        conn.createStatement().execute("create table " + tableName +
            " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), " +
            "val3 varchar(10))");
    }

    private void createTableAndIndex(Connection conn, String dataTableName,
                                       String indexTableName) throws Exception {
        createTable(conn, dataTableName);
        conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
            dataTableName + " (val1) include (val2, val3)");
        conn.commit();
    }
}
