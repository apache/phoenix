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
package org.apache.phoenix.mapreduce.util;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.BaseIndexTest;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class IndexColumnNamesTest extends BaseIndexTest {

    private static final String DYNAMIC_COL_DDL =
            "CREATE TABLE IF NOT EXISTS PRECISION_NAME_TEST\n" + "(\n"
                    + "    CHAR_TEST CHAR(15) NOT NULL primary key,\n"
                    + "    VARCHAR_TEST VARCHAR(1),\n" + "    DECIMAL_TEST DECIMAL(10,2),\n"
                    + "    BINARY_TEST BINARY(11),\n"
                    + "    VARCHAR_UNSPEC VARCHAR,\n"
                    + "    DEC_UNSPEC DECIMAL\n" + ")";

    private static final String DYNAMIC_COL_IDX_DDL =
            "CREATE INDEX PRECISION_NAME_IDX_TEST ON PRECISION_NAME_TEST(VARCHAR_TEST) INCLUDE (CHAR_TEST,DECIMAL_TEST,BINARY_TEST,VARCHAR_UNSPEC,DEC_UNSPEC)";

    @Test
    public void testGetColumnNames() {
        IndexColumnNames indexColumnNames = new IndexColumnNames(pDataTable, pIndexTable);
        assertEquals("[ID, PK_PART2, 0.NAME, 0.ZIP]", indexColumnNames.getDataColNames().toString());
        assertEquals("[:ID, :PK_PART2, 0:NAME, 0:ZIP]", indexColumnNames.getIndexColNames().toString()); //index column names, leading with the data table pk
        assertEquals("[:ID, :PK_PART2, 0:NAME]", indexColumnNames.getIndexPkColNames().toString());
        assertEquals("[ID, PK_PART2]", indexColumnNames.getDataPkColNames().toString());
        assertEquals("[0.NAME, 0.ZIP]", indexColumnNames.getDataNonPkColNames().toString());

        assertEquals("[\"ID\" INTEGER, \"PK_PART2\" TINYINT, \"NAME\" VARCHAR, \"ZIP\" BIGINT]", indexColumnNames.getDynamicDataCols().toString());
        assertEquals("[\":ID\" INTEGER, \":PK_PART2\" TINYINT, \"0:NAME\" VARCHAR, \"0:ZIP\" BIGINT]", indexColumnNames.getDynamicIndexCols().toString());
        assertEquals("UPSERT /*+ NO_INDEX */  INTO TEST_SCHEMA.TEST_INDEX_COLUMN_NAMES_UTIL (\"ID\" INTEGER, \"PK_PART2\" TINYINT, \"NAME\" VARCHAR, \"ZIP\" BIGINT) VALUES (?, ?, ?, ?)", QueryUtil.constructUpsertStatement(DATA_TABLE_FULL_NAME, indexColumnNames.getDynamicDataCols(), Hint.NO_INDEX));
    }

    /**
     * Tests that col types with a precision are outputted correctly in the dynamic columns
     * @throws SQLException
     */
    @Test
    public void testGetDynamicColPrecision() throws SQLException {
        conn.createStatement().execute(DYNAMIC_COL_DDL);
        conn.createStatement().execute(DYNAMIC_COL_IDX_DDL);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        pDataTable = pconn.getTable(new PTableKey(pconn.getTenantId(), "PRECISION_NAME_TEST"));
        pIndexTable = pconn.getTable(new PTableKey(pconn.getTenantId(), "PRECISION_NAME_IDX_TEST"));
        IndexColumnNames indexColumnNames = new IndexColumnNames(pDataTable, pIndexTable);
        assertEquals("[\"CHAR_TEST\" CHAR(15), \"VARCHAR_TEST\" VARCHAR(1), \"DECIMAL_TEST\" DECIMAL(10,2), \"BINARY_TEST\" BINARY(11), \"VARCHAR_UNSPEC\" VARCHAR, \"DEC_UNSPEC\" DECIMAL]", indexColumnNames.getDynamicDataCols().toString());
        assertEquals("[\":CHAR_TEST\" CHAR(15), \"0:VARCHAR_TEST\" VARCHAR(1), \"0:DECIMAL_TEST\" DECIMAL(10,2), \"0:BINARY_TEST\" BINARY(11), \"0:VARCHAR_UNSPEC\" VARCHAR, \"0:DEC_UNSPEC\" DECIMAL]",
                indexColumnNames.getDynamicIndexCols().toString());
    }
}
