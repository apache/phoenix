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
package org.apache.phoenix.calcite;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CalciteIndexIT extends BaseCalciteIT {
    
    private final boolean localIndex;
    
    public CalciteIndexIT(boolean localIndex) {
        this.localIndex = localIndex;
    }
    
    @Parameters(name="localIndex = {0}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {     
                 { false }, { true }
           });
    }
    
    @Before
    public void initTable() throws Exception {
        final String url = getUrl();
        final String index = localIndex ? "LOCAL INDEX" : "INDEX";
        initATableValues(getOrganizationId(), null, url);
        initSaltedTables(index);
        initMultiTenantTables(index);
        final Connection connection = DriverManager.getConnection(url);
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX1 ON aTable (a_string) INCLUDE (b_string, x_integer)");
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX2 ON aTable (b_string) INCLUDE (a_string, y_integer)");
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX_FULL ON aTable (b_string) INCLUDE (a_string, a_integer, a_date, a_time, a_timestamp, x_decimal, x_long, x_integer, y_integer, a_byte, a_short, a_float, a_double, a_unsigned_float, a_unsigned_double)");
        connection.createStatement().execute("UPDATE STATISTICS ATABLE");
        connection.createStatement().execute("UPDATE STATISTICS " + SALTED_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS IDX_" + SALTED_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS IDX1");
        connection.createStatement().execute("UPDATE STATISTICS IDX2");
        connection.createStatement().execute("UPDATE STATISTICS IDX_FULL");
        connection.close();
    }
    
    @Test public void testIndex() throws Exception {
        start(true).sql("select * from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(ORGANIZATION_ID=[$1], ENTITY_ID=[$2], A_STRING=[$3], B_STRING=[$0], A_INTEGER=[$4], A_DATE=[$5], A_TIME=[$6], A_TIMESTAMP=[$7], X_DECIMAL=[$8], X_LONG=[$9], X_INTEGER=[$10], Y_INTEGER=[$11], A_BYTE=[$12], A_SHORT=[$13], A_FLOAT=[$14], A_DOUBLE=[$15], A_UNSIGNED_FLOAT=[$16], A_UNSIGNED_DOUBLE=[$17])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX_FULL]], filter=[=($0, 'b')])\n")
            .close();
        start(true).sql("select x_integer from aTable")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(X_INTEGER=[$4])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]])\n")
            .close();
        start(true).sql("select a_string from aTable order by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], scanOrder=[FORWARD])\n")
            .close();
        start(true).sql("select a_string from aTable order by organization_id")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$2], ORGANIZATION_ID=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
            .close();
        start(true).sql("select a_integer from aTable order by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerSort(sort0=[$1], dir0=[ASC])\n" +
                       "    PhoenixServerProject(A_INTEGER=[$4], A_STRING=[$2])\n" +
                       "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
            .close();
        start(true).sql("select a_string, b_string from aTable where a_string = 'a'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$0], B_STRING=[$3])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], filter=[=($0, 'a')])\n")
            .close();
        start(true).sql("select a_string, b_string from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$3], B_STRING=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX2]], filter=[=($0, 'b')])\n")
            .close();
        start(true).sql("select a_string, b_string, x_integer, y_integer from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$3], B_STRING=[$0], X_INTEGER=[$10], Y_INTEGER=[$11])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX_FULL]], filter=[=($0, 'b')])\n")
            .close();
        start(true).sql("select a_string, count(*) from aTable group by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()], isOrdered=[true])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], scanOrder=[FORWARD])\n")
            .close();
    }
    
    @Test public void testSaltedIndex() throws Exception {
        start(true).sql("select count(*) from " + NOSALT_TABLE_NAME + " where col0 > 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_NOSALT_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 3)])\n")
                .resultIs(new Object[][]{{2L}})
                .close();
        start(true).sql("select mypk0, mypk1, col0 from " + NOSALT_TABLE_NAME + " where col0 <= 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_NOSALT_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 4)])\n")
                .resultIs(new Object[][] {
                        {2, 3, 4},
                        {1, 2, 3}})
                .close();
        start(true).sql("select * from " + SALTED_TABLE_NAME + " where mypk0 < 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], filter=[<($0, 3)])\n")
                .resultIs(new Object[][] {
                        {1, 2, 3, 4},
                        {2, 3, 4, 5}})
                .close();
        start(true).sql("select count(*) from " + SALTED_TABLE_NAME + " where col0 > 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_SALTED_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 3)])\n")
                .resultIs(new Object[][]{{2L}})
                .close();
        start(true).sql("select mypk0, mypk1, col0 from " + SALTED_TABLE_NAME + " where col0 <= 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_SALTED_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 4)])\n")
                .resultIs(new Object[][] {
                        {2, 3, 4},
                        {1, 2, 3}})
                .close();
        start(true).sql("select count(*) from " + SALTED_TABLE_NAME + " where col1 > 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 4)])\n")
                .resultIs(new Object[][]{{2L}})
                .close();
        start(true).sql("select * from " + SALTED_TABLE_NAME + " where col1 <= 5 order by col1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[$3], COL1=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 5)], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                        {1, 2, 3, 4},
                        {2, 3, 4, 5}})
                .close();
        start(true).sql("select * from " + SALTED_TABLE_NAME + " s1, " + SALTED_TABLE_NAME + " s2 where s1.mypk0 = s2.mypk0 and s1.mypk1 = s2.mypk1 and s1.mypk0 > 1 and s2.col1 < 6")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerJoin(condition=[AND(=($0, $4), =($1, $5))], joinType=[inner])\n" +
                           "    PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], filter=[>($0, 1)])\n" +
                           "    PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[$3], COL1=[CAST($0):INTEGER])\n" +
                           "      PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[<(CAST($0):INTEGER, 6)])\n")
                .resultIs(new Object[][] {
                        {2, 3, 4, 5, 2, 3, 4, 5}})
                .close();
    }
    
    @Test public void testMultiTenant() throws Exception {
        Properties props = getConnectionProps(true);
        start(props).sql("select * from " + MULTI_TENANT_TABLE)
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]])\n")
                .resultIs(new Object[][] {
                        {"10", "2", 3, 4, 5},
                        {"15", "3", 4, 5, 6},
                        {"20", "4", 5, 6, 7},
                        {"20", "5", 6, 7, 8}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where tenant_id = '20' and col1 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(TENANT_ID=[$0], ID=[$2], COL0=[$3], COL1=[CAST($1):INTEGER], COL2=[$4])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[AND(=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, '20'), >(CAST($1):INTEGER, 1))])\n")
                .resultIs(new Object[][] {
                        {"20", "4", 5, 6, 7},
                        {"20", "5", 6, 7, 8}})
                .close();
        
        try {
            start(props).sql("select * from " + MULTI_TENANT_VIEW1)
                .explainIs("")
                .close();
            fail("Should have got SQLException.");
        } catch (SQLException e) {
        }
        
        props.setProperty("TenantId", "15");
        start(props).sql("select * from " + MULTI_TENANT_TABLE)
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]])\n")
                .resultIs(new Object[][] {
                        {"3", 4, 5, 6}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where col1 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[$2], COL1=[CAST($0):INTEGER], COL2=[$3])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"3", 4, 5, 6}})
                .close();
        
        try {
            start(props).sql("select * from " + MULTI_TENANT_VIEW1)
                .explainIs("")
                .close();
            fail("Should have got SQLException.");
        } catch (SQLException e) {
        }

        props.setProperty("TenantId", "10");
        start(props).sql("select * from " + MULTI_TENANT_VIEW1)
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]])\n")
                .resultIs(new Object[][] {
                        {"2", 3, 4, 5}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where col1 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[$2], COL1=[CAST($0):INTEGER], COL2=[$3])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"2", 3, 4, 5}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW1 + " where col0 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, S1, IDX_MULTITENANT_TEST_VIEW1]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"2", 3}})
                .close();

        props.setProperty("TenantId", "20");
        start(props).sql("select * from " + MULTI_TENANT_VIEW2)
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]], filter=[>($3, 7)])\n")
                .resultIs(new Object[][] {
                        {"5", 6, 7, 8}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW2 + " where col0 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, S2, IDX_MULTITENANT_TEST_VIEW2]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"5", 6}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW2 + " order by col0")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, S2, IDX_MULTITENANT_TEST_VIEW2]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                        {"5", 6}})
                .close();
    }

}
