/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.mapreduce.index;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Arrays;

import org.apache.phoenix.mapreduce.util.IndexColumnNames;
import org.junit.Before;
import org.junit.Test;

public class IndexScrutinyTableOutputTest extends BaseIndexTest {

    private static final long SCRUTINY_TIME_MILLIS = 1502908914193L;

    @Before
    public void setup() throws Exception {
        super.setup();
        conn.createStatement().execute(IndexScrutinyTableOutput.OUTPUT_TABLE_DDL);
        conn.createStatement().execute(IndexScrutinyTableOutput.OUTPUT_METADATA_DDL);
    }

    @Test
    public void testConstructMetadataParamQuery() {
        String metadataParamQuery =
            IndexScrutinyTableOutput
                .constructMetadataParamQuery(Arrays.asList("INVALID_ROWS_QUERY_ALL"));
        assertEquals(
            "SELECT \"INVALID_ROWS_QUERY_ALL\" FROM PHOENIX_INDEX_SCRUTINY_METADATA WHERE (\"SOURCE_TABLE\",\"TARGET_TABLE\",\"SCRUTINY_EXECUTE_TIME\") IN ((?,?,?))",
            metadataParamQuery);
    }

    @Test
    public void testGetSqlQueryAllInvalidRows() throws SQLException {
        SourceTargetColumnNames columnNames =
            new SourceTargetColumnNames.DataSourceColNames(pDataTable, pIndexTable);
        String sqlStr =
            IndexScrutinyTableOutput.getSqlQueryAllInvalidRows(conn, columnNames,
                SCRUTINY_TIME_MILLIS);
        assertEquals("SELECT \"SOURCE_TABLE\" , \"TARGET_TABLE\" , \"SCRUTINY_EXECUTE_TIME\" , \"SOURCE_ROW_PK_HASH\" , \"SOURCE_TS\" , \"TARGET_TS\" , \"HAS_TARGET_ROW\" , \"BEYOND_MAX_LOOKBACK\" , \"ID\" , \"PK_PART2\" , \"NAME\" , \"ZIP\" , \":ID\" , \":PK_PART2\" , \"0:NAME\" , \"0:ZIP\" FROM PHOENIX_INDEX_SCRUTINY(\"ID\" INTEGER,\"PK_PART2\" TINYINT,\"NAME\" VARCHAR,\"ZIP\" BIGINT,\":ID\" INTEGER,\":PK_PART2\" TINYINT,\"0:NAME\" VARCHAR,\"0:ZIP\" BIGINT) WHERE (\"SOURCE_TABLE\",\"TARGET_TABLE\",\"SCRUTINY_EXECUTE_TIME\") IN (('TEST_SCHEMA.TEST_INDEX_COLUMN_NAMES_UTIL','TEST_SCHEMA.TEST_ICN_INDEX',1502908914193))",
            sqlStr);
    }

    @Test
    public void testGetSqlQueryMissingTargetRows() throws SQLException {
        SourceTargetColumnNames columnNames =
            new SourceTargetColumnNames.DataSourceColNames(pDataTable, pIndexTable);
        String query =
            IndexScrutinyTableOutput.getSqlQueryMissingTargetRows(conn, columnNames,
                SCRUTINY_TIME_MILLIS);
        assertEquals("SELECT \"SOURCE_TABLE\" , \"TARGET_TABLE\" , \"SCRUTINY_EXECUTE_TIME\" , \"SOURCE_ROW_PK_HASH\" , \"SOURCE_TS\" , \"TARGET_TS\" , \"HAS_TARGET_ROW\" , \"BEYOND_MAX_LOOKBACK\" , \"ID\" , \"PK_PART2\" , \"NAME\" , \"ZIP\" , \":ID\" , \":PK_PART2\" , \"0:NAME\" , \"0:ZIP\" FROM PHOENIX_INDEX_SCRUTINY(\"ID\" INTEGER,\"PK_PART2\" TINYINT,\"NAME\" VARCHAR,\"ZIP\" BIGINT,\":ID\" INTEGER,\":PK_PART2\" TINYINT,\"0:NAME\" VARCHAR,\"0:ZIP\" BIGINT) WHERE (\"SOURCE_TABLE\",\"TARGET_TABLE\",\"SCRUTINY_EXECUTE_TIME\", \"HAS_TARGET_ROW\") IN (('TEST_SCHEMA.TEST_INDEX_COLUMN_NAMES_UTIL','TEST_SCHEMA.TEST_ICN_INDEX',1502908914193,false))",
            query);
    }

    @Test
    public void testGetSqlQueryBadCoveredColVal() throws SQLException {
        SourceTargetColumnNames columnNames =
            new SourceTargetColumnNames.DataSourceColNames(pDataTable, pIndexTable);
        String query =
            IndexScrutinyTableOutput.getSqlQueryBadCoveredColVal(conn, columnNames,
                SCRUTINY_TIME_MILLIS);
        assertEquals("SELECT \"SOURCE_TABLE\" , \"TARGET_TABLE\" , \"SCRUTINY_EXECUTE_TIME\" , \"SOURCE_ROW_PK_HASH\" , \"SOURCE_TS\" , \"TARGET_TS\" , \"HAS_TARGET_ROW\" , \"BEYOND_MAX_LOOKBACK\" , \"ID\" , \"PK_PART2\" , \"NAME\" , \"ZIP\" , \":ID\" , \":PK_PART2\" , \"0:NAME\" , \"0:ZIP\" FROM PHOENIX_INDEX_SCRUTINY(\"ID\" INTEGER,\"PK_PART2\" TINYINT,\"NAME\" VARCHAR,\"ZIP\" BIGINT,\":ID\" INTEGER,\":PK_PART2\" TINYINT,\"0:NAME\" VARCHAR,\"0:ZIP\" BIGINT) WHERE (\"SOURCE_TABLE\",\"TARGET_TABLE\",\"SCRUTINY_EXECUTE_TIME\", \"HAS_TARGET_ROW\") IN (('TEST_SCHEMA.TEST_INDEX_COLUMN_NAMES_UTIL','TEST_SCHEMA.TEST_ICN_INDEX',1502908914193,true))",
            query);
    }

    @Test
    public void testGetSqlQueryBeyondMaxLookback() throws SQLException {
        SourceTargetColumnNames columnNames =
            new SourceTargetColumnNames.DataSourceColNames(pDataTable, pIndexTable);
        String query =
            IndexScrutinyTableOutput.getSqlQueryBeyondMaxLookback(conn, columnNames,
                SCRUTINY_TIME_MILLIS);
        assertEquals("SELECT \"SOURCE_TABLE\" , \"TARGET_TABLE\" , \"SCRUTINY_EXECUTE_TIME\" , \"SOURCE_ROW_PK_HASH\" , \"SOURCE_TS\" , \"TARGET_TS\" , \"HAS_TARGET_ROW\" , \"BEYOND_MAX_LOOKBACK\" , \"ID\" , \"PK_PART2\" , \"NAME\" , \"ZIP\" , \":ID\" , \":PK_PART2\" , \"0:NAME\" , \"0:ZIP\" FROM PHOENIX_INDEX_SCRUTINY(\"ID\" INTEGER,\"PK_PART2\" TINYINT,\"NAME\" VARCHAR,\"ZIP\" BIGINT,\":ID\" INTEGER,\":PK_PART2\" TINYINT,\"0:NAME\" VARCHAR,\"0:ZIP\" BIGINT) WHERE (\"SOURCE_TABLE\",\"TARGET_TABLE\",\"SCRUTINY_EXECUTE_TIME\", \"HAS_TARGET_ROW\", \"BEYOND_MAX_LOOKBACK\") IN (('TEST_SCHEMA.TEST_INDEX_COLUMN_NAMES_UTIL','TEST_SCHEMA.TEST_ICN_INDEX',1502908914193,false,true))",
            query);
    }

    @Test
    public void testGetOutputTableUpsert() throws Exception {
        IndexColumnNames columnNames = new IndexColumnNames(pDataTable, pIndexTable);
        String outputTableUpsert =
            IndexScrutinyTableOutput.constructOutputTableUpsert(
                columnNames.getDynamicDataCols(), columnNames.getDynamicIndexCols(), conn);
        conn.prepareStatement(outputTableUpsert); // shouldn't throw
        assertEquals("UPSERT  INTO PHOENIX_INDEX_SCRUTINY (\"SOURCE_TABLE\", \"TARGET_TABLE\", \"SCRUTINY_EXECUTE_TIME\", \"SOURCE_ROW_PK_HASH\", \"SOURCE_TS\", \"TARGET_TS\", \"HAS_TARGET_ROW\", \"BEYOND_MAX_LOOKBACK\", \"ID\" INTEGER, \"PK_PART2\" TINYINT, \"NAME\" VARCHAR, \"ZIP\" BIGINT, \":ID\" INTEGER, \":PK_PART2\" TINYINT, \"0:NAME\" VARCHAR, \"0:ZIP\" BIGINT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            outputTableUpsert);
    }

}
