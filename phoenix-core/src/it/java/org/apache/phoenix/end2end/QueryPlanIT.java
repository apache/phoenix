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

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.PTSDB3_NAME;
import static org.apache.phoenix.util.TestUtil.PTSDB_NAME;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class QueryPlanIT extends BaseConnectedQueryIT {
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Override date format so we don't have a bunch of zeros
        props.put(QueryServices.DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testExplainPlan() throws Exception {
        ensureTableCreated(getUrl(), ATABLE_NAME, getDefaultSplits(getOrganizationId()));
        ensureTableCreated(getUrl(), PTSDB_NAME, getDefaultSplits(getOrganizationId()));
        ensureTableCreated(getUrl(), PTSDB3_NAME, getDefaultSplits(getOrganizationId()));
        String[] queryPlans = new String[] {

                "SELECT host FROM PTSDB3 WHERE host IN ('na1', 'na2','na3')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 3 KEYS OVER PTSDB3 [~'na3'] - [~'na1']\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY",

                "SELECT host FROM PTSDB WHERE inst IS NULL AND host IS NOT NULL AND date >= to_date('2013-01-01')",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER PTSDB [null,not null]\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY AND DATE >= '2013-01-01 00:00:00.000'",

                // Since inst IS NOT NULL is unbounded, we won't continue optimizing
                "SELECT host FROM PTSDB WHERE inst IS NOT NULL AND host IS NULL AND date >= to_date('2013-01-01')",
                "CLIENT PARALLEL 4-WAY RANGE SCAN OVER PTSDB [not null]\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY AND (HOST IS NULL AND DATE >= '2013-01-01 00:00:00.000')",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id = '000000000000002' AND x_integer = 2 AND a_integer < 5 ",
                "CLIENT PARALLEL 1-WAY POINT LOOKUP ON 1 KEY OVER ATABLE\n" + 
                "    SERVER FILTER BY (X_INTEGER = 2 AND A_INTEGER < 5)",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id > '000000000000002' AND entity_id < '000000000000008' AND (organization_id,entity_id) >= ('000000000000001','000000000000005') ",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000005'] - ['000000000000001','000000000000008']",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id > '000000000000002' AND entity_id < '000000000000008' AND (organization_id,entity_id) <= ('000000000000001','000000000000005') ",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000003'] - ['000000000000001','000000000000006']",

                "SELECT a_string,b_string FROM atable WHERE organization_id > '000000000000001' AND entity_id > '000000000000002' AND entity_id < '000000000000008' AND (organization_id,entity_id) >= ('000000000000003','000000000000005') ",
                "CLIENT PARALLEL 4-WAY RANGE SCAN OVER ATABLE ['000000000000003','000000000000005'] - [*]\n" + 
                "    SERVER FILTER BY (ENTITY_ID > '000000000000002' AND ENTITY_ID < '000000000000008')",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id >= '000000000000002' AND entity_id < '000000000000008' AND (organization_id,entity_id) >= ('000000000000000','000000000000005') ",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','000000000000002'] - ['000000000000001','000000000000008']",

                "SELECT * FROM atable",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE",

                "SELECT inst,host FROM PTSDB WHERE REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1', 'na2','na3')", // REVIEW: should this use skip scan given the regexpr_substr
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 3 RANGES OVER PTSDB ['na1'] - ['na4']\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY AND REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1','na2','na3')",

                "SELECT inst,host FROM PTSDB WHERE inst IN ('na1', 'na2','na3') AND host IN ('a','b') AND date >= to_date('2013-01-01') AND date < to_date('2013-01-02')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 6 RANGES OVER PTSDB ['na1','a','2013-01-01'] - ['na3','b','2013-01-02']\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY",

                "SELECT inst,host FROM PTSDB WHERE inst LIKE 'na%' AND host IN ('a','b') AND date >= to_date('2013-01-01') AND date < to_date('2013-01-02')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 RANGES OVER PTSDB ['na','a','2013-01-01'] - ['nb','b','2013-01-02']\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY",

                "SELECT count(*) FROM atable",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO SINGLE ROW",

                // TODO: review: why does this change with parallelized non aggregate queries?
                "SELECT count(*) FROM atable WHERE organization_id='000000000000001' AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','003'] - ['000000000000001','004']\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO SINGLE ROW",

                "SELECT a_string FROM atable WHERE organization_id='000000000000001' AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001','003'] - ['000000000000001','004']",

                "SELECT count(1) FROM atable GROUP BY a_string",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]\n" +
                "CLIENT MERGE SORT",

                "SELECT count(1) FROM atable GROUP BY a_string LIMIT 5",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT 5 ROW LIMIT",

                "SELECT a_string FROM atable ORDER BY a_string DESC LIMIT 3",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER TOP 3 ROWS SORTED BY [A_STRING DESC]\n" + 
                "CLIENT MERGE SORT",

                "SELECT count(1) FROM atable GROUP BY a_string,b_string HAVING max(a_string) = 'a'",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT FILTER BY MAX(A_STRING) = 'a'",

                "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY ROUND(a_time,'HOUR',2),entity_id HAVING max(a_string) = 'a'",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER FILTER BY A_INTEGER = 1\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ENTITY_ID, ROUND(A_TIME)]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT FILTER BY MAX(A_STRING) = 'a'",

                "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY a_string,b_string HAVING max(a_string) = 'a' ORDER BY b_string",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER FILTER BY A_INTEGER = 1\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT FILTER BY MAX(A_STRING) = 'a'\n" +
                "CLIENT SORTED BY [B_STRING]",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id != '000000000000002' AND x_integer = 2 AND a_integer < 5 LIMIT 10",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001']\n" + 
                "    SERVER FILTER BY (ENTITY_ID != '000000000000002' AND X_INTEGER = 2 AND A_INTEGER < 5)\n" + 
                "    SERVER 10 ROW LIMIT\n" + 
                "CLIENT 10 ROW LIMIT",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' ORDER BY a_string ASC NULLS FIRST LIMIT 10",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001']\n" + 
                "    SERVER TOP 10 ROWS SORTED BY [A_STRING]\n" + 
                "CLIENT MERGE SORT",

                "SELECT max(a_integer) FROM atable WHERE organization_id = '000000000000001' GROUP BY organization_id,entity_id,ROUND(a_date,'HOUR') ORDER BY entity_id NULLS LAST LIMIT 10",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001']\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ORGANIZATION_ID, ENTITY_ID, ROUND(A_DATE)]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT TOP 10 ROWS SORTED BY [ENTITY_ID NULLS LAST]",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' ORDER BY a_string DESC NULLS LAST LIMIT 10",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE ['000000000000001']\n" + 
                "    SERVER TOP 10 ROWS SORTED BY [A_STRING DESC NULLS LAST]\n" + 
                "CLIENT MERGE SORT",

                "SELECT a_string,b_string FROM atable WHERE organization_id IN ('000000000000001', '000000000000005')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER ATABLE ['000000000000001'] - ['000000000000005']",

                "SELECT a_string,b_string FROM atable WHERE organization_id IN ('00D000000000001', '00D000000000005') AND entity_id IN('00E00000000000X','00E00000000000Z')",
                "CLIENT PARALLEL 1-WAY POINT LOOKUP ON 4 KEYS OVER ATABLE",
        };
        for (int i = 0; i < queryPlans.length; i+=2) {
            String query = queryPlans[i];
            String plan = queryPlans[i+1];
            Properties props = new Properties();
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                Statement statement = conn.createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + query);
                // TODO: figure out a way of verifying that query isn't run during explain execution
                assertEquals(query, plan, QueryUtil.getExplainPlan(rs));
            } catch (Exception e) {
                throw new Exception(query + ": "+ e.getMessage(), e);
            } finally {
                conn.close();
            }
        }
    }

}
