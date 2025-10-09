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

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.fail;
public class MixedCaseInnerJoinAliasesIT extends ParallelStatsDisabledIT {
    @BeforeClass
    public static void setUp() throws Exception {

        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        String createContractTable = "CREATE TABLE T1 (ID CHAR(256) PRIMARY KEY, \"F\".DUMMY_COLUMN VARCHAR)";
        conn.createStatement().execute(createContractTable);

        String createContractViewSql = "CREATE VIEW V1(ROWKEY VARCHAR PRIMARY KEY,F.COL1 VARCHAR,F.COL2 VARCHAR," +
                "F.COL3 VARCHAR) AS SELECT * FROM T1";
        conn.createStatement().execute(createContractViewSql);


        String createCounterPartyTable = "CREATE TABLE T2 (ID CHAR(256) PRIMARY KEY, \"F2\".DUMMY_COLUMN VARCHAR)";
        conn.createStatement().execute(createCounterPartyTable);
        String createCounterPartyViewSql = "CREATE VIEW V2(ROWKEY VARCHAR PRIMARY KEY,F2.COL1 VARCHAR," +
                "F2.COL2 VARCHAR,F2.COL3 VARCHAR) AS SELECT * FROM T2";
        conn.createStatement().execute(createCounterPartyViewSql);
    }

    @Test
    public void testInInnerJoinAliasesWithoutQuotes() {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try {
            Connection conn = DriverManager.getConnection(getUrl(), props);

            String selectQuery = "SELECT MixedCaseAlias1.\"COL1\" AS \"MixedCaseAlias1_COL1\", " +
                    "\"MixedCaseAlias2\".\"COL2\" AS \"MixedCaseAlias2_COL2\", " +
                    "\"MixedCaseAlias2\".\"COL3\" AS \"MixedCaseAlias2_COL3\" " +
                    "FROM (SELECT \"COL1\", \"COL2\" FROM V1) AS MixedCaseAlias1 " +
                    "INNER JOIN V2 AS \"MixedCaseAlias2\" " +
                    "ON (MixedCaseAlias1.\"COL1\" = \"MixedCaseAlias2\".\"COL1\")";
            conn.createStatement().executeQuery(selectQuery);
        } catch (Exception e) {
            fail(e.getClass().getSimpleName() + " was not expectedly thrown: " + e.getMessage());
        }
    }

    @Test
    public void testInInnerJoinWithAliasesWithQuotes() {
        try {
            Connection conn = DriverManager.getConnection(getUrl(),
                    PropertiesUtil.deepCopy(TEST_PROPERTIES));

            String selectQuery = "SELECT \"MixedCaseAlias1\".\"COL1\" AS \"MixedCaseAlias1_COL1\", " +
                    "\"MixedCaseAlias2\".\"COL2\" AS \"MixedCaseAlias2_COL2\", " +
                    "\"MixedCaseAlias2\".\"COL3\" as \"MixedCaseAlias2_COL3\" " +
                    "FROM (SELECT \"COL1\", \"COL2\" FROM V1) AS \"MixedCaseAlias1\" " +
                    "INNER JOIN V2 AS \"MixedCaseAlias2\" ON " +
                    "(\"MixedCaseAlias1\".\"COL1\" = \"MixedCaseAlias2\".\"COL1\")";
            conn.createStatement().executeQuery(selectQuery);
        } catch (Exception e) {
            fail(e.getClass().getSimpleName() + " was not expectedly thrown: " + e.getMessage());
        }
    }
}