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
        Connection connection = DriverManager.getConnection(url);
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX1 ON aTable (a_string) INCLUDE (b_string, x_integer)");
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX2 ON aTable (b_string) INCLUDE (a_string, y_integer)");
        connection.createStatement().execute("CREATE " + index + " IF NOT EXISTS IDX_FULL ON aTable (b_string) INCLUDE (a_string, a_integer, a_date, a_time, a_timestamp, x_decimal, x_long, x_integer, y_integer, a_byte, a_short, a_float, a_double, a_unsigned_float, a_unsigned_double)");
        connection.createStatement().execute("UPDATE STATISTICS ATABLE");
        connection.createStatement().execute("UPDATE STATISTICS " + NOSALT_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + SALTED_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + MULTI_TENANT_TABLE);
        connection.close();
        
        Properties props = new Properties();
        props.setProperty("TenantId", "10");
        connection = DriverManager.getConnection(url, props);
        connection.createStatement().execute("UPDATE STATISTICS " + MULTI_TENANT_VIEW1);
        connection.close();
        
        props.setProperty("TenantId", "20");
        connection = DriverManager.getConnection(url, props);
        connection.createStatement().execute("UPDATE STATISTICS " + MULTI_TENANT_VIEW2);
        connection.close();
    }
    
    @Test public void testIndex() throws Exception {
        start(true, 1000f).sql("select * from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(ORGANIZATION_ID=[$1], ENTITY_ID=[$2], A_STRING=[$3], B_STRING=[$0], A_INTEGER=[$4], A_DATE=[$5], A_TIME=[$6], A_TIMESTAMP=[$7], X_DECIMAL=[$8], X_LONG=[$9], X_INTEGER=[$10], Y_INTEGER=[$11], A_BYTE=[$12], A_SHORT=[$13], A_FLOAT=[$14], A_DOUBLE=[$15], A_UNSIGNED_FLOAT=[$16], A_UNSIGNED_DOUBLE=[$17])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX_FULL]], filter=[=($0, 'b')])\n")
            .close();
        start(true, 1000f).sql("select x_integer from aTable")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(X_INTEGER=[$4])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]])\n")
            .close();
        start(true, 1000f).sql("select a_string from aTable order by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], scanOrder=[FORWARD])\n")
            .close();
        start(true, 1000000f).sql("select a_string from aTable order by organization_id")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$2], ORGANIZATION_ID=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
            .close();
        start(true, 1000f).sql("select a_integer from aTable order by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerSort(sort0=[$1], dir0=[ASC])\n" +
                       "    PhoenixServerProject(A_INTEGER=[$4], A_STRING=[$3])\n" +
                       "      PhoenixTableScan(table=[[phoenix, IDX_FULL]])\n")
            .close();
        start(true, 1000f).sql("select a_string, b_string from aTable where a_string = 'a'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$0], B_STRING=[$3])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], filter=[=($0, 'a')])\n")
            .close();
        start(true, 1000f).sql("select a_string, b_string from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$3], B_STRING=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX2]], filter=[=($0, 'b')])\n")
            .close();
        start(true, 1000f).sql("select a_string, b_string, x_integer, y_integer from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$3], B_STRING=[$0], X_INTEGER=[$10], Y_INTEGER=[$11])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX_FULL]], filter=[=($0, 'b')])\n")
            .close();
        start(true, 1000f).sql("select a_string, count(*) from aTable group by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()], isOrdered=[true])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], scanOrder=[FORWARD])\n")
            .close();
    }
    
    @Test public void testSaltedIndex() throws Exception {
        start(true, 1f).sql("select count(*) from " + NOSALT_TABLE_NAME + " where col0 > 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_NOSALT_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 3)])\n")
                .resultIs(false, new Object[][]{{999L}})
                .close();
        start(true, 1f).sql("select mypk0, mypk1, col0 from " + NOSALT_TABLE_NAME + " where col0 <= 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_NOSALT_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 4)])\n")
                .resultIs(false, new Object[][] {
                        {2, 3, 4},
                        {1, 2, 3}})
                .close();
        start(true, 1f).sql("select * from " + SALTED_TABLE_NAME + " where mypk0 < 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], filter=[<($0, 3)])\n")
                .resultIs(false, new Object[][] {
                        {1, 2, 3, 4},
                        {2, 3, 4, 5}})
                .close();
        start(true, 1f).sql("select count(*) from " + SALTED_TABLE_NAME + " where col0 > 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_SALTED_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 3)])\n")
                .resultIs(false, new Object[][]{{999L}})
                .close();
        start(true, 1f).sql("select mypk0, mypk1, col0 from " + SALTED_TABLE_NAME + " where col0 <= 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_SALTED_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 4)])\n")
                .resultIs(false, new Object[][] {
                        {2, 3, 4},
                        {1, 2, 3}})
                .close();
        start(true, 1f).sql("select count(*) from " + SALTED_TABLE_NAME + " where col1 > 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 4)])\n")
                .resultIs(false, new Object[][]{{999L}})
                .close();
        start(true, 1f).sql("select * from " + SALTED_TABLE_NAME + " where col1 <= 5 order by col1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[$3], COL1=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 5)], scanOrder=[FORWARD])\n")
                .resultIs(true, new Object[][] {
                        {1, 2, 3, 4},
                        {2, 3, 4, 5}})
                .close();
        start(true, 1f).sql("select * from " + SALTED_TABLE_NAME + " s1, " + SALTED_TABLE_NAME + " s2 where s1.mypk0 = s2.mypk0 and s1.mypk1 = s2.mypk1 and s1.mypk0 > 500 and s2.col1 < 505")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerJoin(condition=[AND(=($0, $4), =($1, $5))], joinType=[inner])\n" +
                           "    PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], filter=[>($0, 500)])\n" +
                           "    PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[$3], COL1=[CAST($0):INTEGER])\n" +
                           "      PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[<(CAST($0):INTEGER, 505)])\n")
                .resultIs(false, new Object[][] {
                        {501, 502, 503, 504, 501, 502, 503, 504}})
                .close();
    }
    
    @Test public void testMultiTenant() throws Exception {
        Properties props = getConnectionProps(true, 1f);
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where tenant_id = '10' and id <= '0004'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]], filter=[AND(=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, '10'), <=($1, '0004'))])\n")
                .resultIs(false, new Object[][] {
                        {"10", "0002", 3, 4, 5},
                        {"10", "0003", 4, 5, 6},
                        {"10", "0004", 5, 6, 7}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where tenant_id = '20' and col1 < 8")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(TENANT_ID=[$0], ID=[$2], COL0=[$3], COL1=[CAST($1):INTEGER], COL2=[$4])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[AND(=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, '20'), <(CAST($1):INTEGER, 8))])\n")
                .resultIs(false, new Object[][] {
                        {"20", "0004", 5, 6, 7},
                        {"20", "0005", 6, 7, 8}})
                .close();
        
        try {
            start(props).sql("select * from " + MULTI_TENANT_VIEW1)
                .explainIs("")
                .close();
            fail("Should have got SQLException.");
        } catch (SQLException e) {
        }
        
        props.setProperty("TenantId", "15");
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where id = '0284'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]], filter=[=(CAST($0):VARCHAR(4) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, '0284')])\n")
                .resultIs(false, new Object[][] {
                        {"0284", 285, 286, 287}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where col1 > 1000")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[$2], COL1=[CAST($0):INTEGER], COL2=[$3])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 1000)])\n")
                .resultIs(false, new Object[][] {
                        {"0999", 1000, 1001, 1002},
                        {"1000", 1001, 1002, 1003},
                        {"1001", 1002, 1003, 1004},
                        {"1002", 1003, 1004, 1005}})
                .close();
        
        try {
            start(props).sql("select * from " + MULTI_TENANT_VIEW1)
                .explainIs("")
                .close();
            fail("Should have got SQLException.");
        } catch (SQLException e) {
        }

        props.setProperty("TenantId", "10");
        start(props).sql("select * from " + MULTI_TENANT_VIEW1 + " where id = '0512'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]], filter=[=(CAST($0):VARCHAR(4) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, '0512')])\n")
                .resultIs(false, new Object[][] {
                        {"0512", 513, 514, 515}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where col1 <= 6")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[$2], COL1=[CAST($0):INTEGER], COL2=[$3])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 6)])\n")
                .resultIs(false, new Object[][] {
                        {"0002", 3, 4, 5},
                        {"0003", 4, 5, 6},
                        {"0004", 5, 6, 7}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW1 + " where col0 >= 1000")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, S1, IDX_MULTITENANT_TEST_VIEW1]], filter=[>=(CAST($0):INTEGER, 1000)])\n")
                .resultIs(false, new Object[][] {
                        {"0999", 1000},
                        {"1000", 1001},
                        {"1001", 1002}})
                .close();

        props.setProperty("TenantId", "20");
        start(props).sql("select * from " + MULTI_TENANT_VIEW2 + " where id = '0765'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]], filter=[AND(>($3, 7), =(CAST($0):VARCHAR(4) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, '0765'))])\n")
                .resultIs(false, new Object[][] {
                        {"0765", 766, 767, 768}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW2 + " where col0 between 272 and 275")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, S2, IDX_MULTITENANT_TEST_VIEW2]], filter=[AND(>=(CAST($0):INTEGER, 272), <=(CAST($0):INTEGER, 275))])\n")
                .resultIs(false, new Object[][] {
                        {"0271", 272},
                        {"0272", 273},
                        {"0273", 274},
                        {"0274", 275}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW2 + " order by col0 limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "      PhoenixTableScan(table=[[phoenix, S2, IDX_MULTITENANT_TEST_VIEW2]], scanOrder=[FORWARD])\n")
                .resultIs(true, new Object[][] {
                        {"0005", 6},
                        {"0006", 7},
                        {"0007", 8},
                        {"0008", 9},
                        {"0009", 10}})
                .close();
    }

}
