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
package org.apache.phoenix.parse;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import static org.junit.Assert.*;


public class CursorParserTest {

    private void parseCursor(String sql) throws IOException, SQLException {
        SQLParser parser = new SQLParser(new StringReader(sql));
        BindableStatement stmt = null;
        try{
            stmt = parser.parseDeclareCursor();
        } catch (SQLException e){
            fail("Unable to parse:\n" + sql);
        }
    }

    private void parseFetch(String sql) throws IOException, SQLException {
        SQLParser parser = new SQLParser(new StringReader(sql));
        BindableStatement stmt = null;
        try{
            stmt = parser.parseFetch();
        } catch (SQLException e){
            fail("Unable to parse:\n" + sql);
        }
    }

    private void parseOpen(String sql) throws IOException, SQLException {
        SQLParser parser = new SQLParser(new StringReader(sql));
        BindableStatement stmt = null;
        try{
            stmt = parser.parseOpen();
        } catch (SQLException e){
            fail("Unable to parse:\n" + sql);
        }
    }

    @Test
    public void testParseCursor0() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select a from b\n" +
                "where ((ind.name = 'X')" +
                "and rownum <= (1000 + 1000))\n";

        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
    }

    @Test
    public void testParseCursor1() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select /*gatherSlowStats*/ count(1) from core.search_name_lookup ind\n" +
                "where( (ind.name = 'X'\n" +
                "and rownum <= 1 + 2)\n" +
                "and (ind.organization_id = '000000000000000')\n" +
                "and (ind.key_prefix = '00T')\n" +
                "and (ind.name_type = 't'))";
        

        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
    }

    @Test
    public void testParseCursor2() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select /*gatherSlowStats*/ count(1) from core.custom_index_value ind\n" +
                "where (ind.string_value in ('a', 'b', 'c', 'd'))\n" +
                "and rownum <= ( 3 + 1 )\n" +
                "and (ind.organization_id = '000000000000000')\n" +
                "and (ind.key_prefix = '00T')\n" +
                "and (ind.deleted = '0')\n" +
                "and (ind.index_num = 1)";
        

        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testParseCursor3() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select /*gatherSlowStats*/ count(1) from core.custom_index_value ind\n" +
                "where (ind.number_value > 3)\n" +
                "and rownum <= 1000\n" +
                "and (ind.organization_id = '000000000000000')\n" +
                "and (ind.key_prefix = '001'\n" +
                "and (ind.deleted = '0'))\n" +
                "and (ind.index_num = 2)";
        

        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testParseCursor4() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select /*+ index(t iecustom_entity_data_created) */ /*gatherSlowStats*/ count(1) from core.custom_entity_data t\n" +
                "where (t.created_date > to_date('01/01/2001'))\n" +
                "and rownum <= 4500\n" +
                "and (t.organization_id = '000000000000000')\n" +
                "and (t.key_prefix = '001')";
        

        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testCountDistinctCursor() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select count(distinct foo) from core.custom_entity_data t\n"
                + "where (t.created_date > to_date('01/01/2001'))\n"
                + "and (t.organization_id = '000000000000000')\n"
                + "and (t.key_prefix = '001')\n" + "limit 4500";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testIsNullCursor() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select count(foo) from core.custom_entity_data t\n" +
                "where (t.created_date is null)\n" +
                "and (t.organization_id is not null)\n";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testAsInColumnAlias() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select count(foo) AS c from core.custom_entity_data t\n" +
                "where (t.created_date is null)\n" +
                "and (t.organization_id is not null)\n";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testParseJoin1() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select /*SOQL*/ \"Id\"\n" +
                "from (select /*+ ordered index(cft) */\n" +
                "cft.val188 \"Marketing_Offer_Code__c\",\n" +
                "t.account_id \"Id\"\n" +
                "from sales.account_cfdata cft,\n" +
                "sales.account t\n" +
                "where (cft.account_cfdata_id = t.account_id)\n" +
                "and (cft.organization_id = '00D300000000XHP')\n" +
                "and (t.organization_id = '00D300000000XHP')\n" +
                "and (t.deleted = '0')\n" +
                "and (t.account_id != '000000000000000'))\n" +
                "where (\"Marketing_Offer_Code__c\" = 'FSCR')";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testParseJoin2() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select /*rptacctlist 00O40000002C3of*/ \"00N40000001M8VK\",\n" +
                "\"00N40000001M8VK.ID\",\n" +
                "\"00N30000000r0K2\",\n" +
                "\"00N30000000jgjo\"\n" +
                "from (select /*+ ordered use_hash(aval368) index(cfa) */\n" +
                "a.record_type_id \"RECORDTYPE\",\n" +
                "aval368.last_name,aval368.first_name || ' ' || aval368.last_name,aval368.name \"00N40000001M8VK\",\n" +
                "a.last_update \"LAST_UPDATE\",\n" +
                "cfa.val368 \"00N40000001M8VK.ID\",\n" +
                "TO_DATE(cfa.val282) \"00N30000000r0K2\",\n" +
                "cfa.val252 \"00N30000000jgjo\"\n" +
                "from sales.account a,\n" +
                "sales.account_cfdata cfa,\n" +
                "core.name_denorm aval368\n" +
                "where (cfa.account_cfdata_id = a.account_id)\n" +
                "and (aval368.entity_id = cfa.val368)\n" +
                "and (a.deleted = '0')\n" +
                "and (a.organization_id = '00D300000000EaE')\n" +
                "and (a.account_id <> '000000000000000')\n" +
                "and (cfa.organization_id = '00D300000000EaE')\n" +
                "and (aval368.organization_id = '00D300000000EaE')\n" +
                "and (aval368.entity_id like '005%'))\n" +
                "where (\"RECORDTYPE\" = '0123000000002Gv')\n" +
                "AND (\"00N40000001M8VK\" is null or \"00N40000001M8VK\" in ('BRIAN IRWIN', 'BRIAN MILLER', 'COLLEEN HORNYAK', 'ERNIE ZAVORAL JR', 'JAMIE TRIMBUR', 'JOE ANTESBERGER', 'MICHAEL HYTLA', 'NATHAN DELSIGNORE', 'SANJAY GANDHI', 'TOM BASHIOUM'))\n" +
                "AND (\"LAST_UPDATE\" >= to_date('2009-08-01 07:00:00'))";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testCommentCursor() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select a from b -- here we come\n" +
                "where ((ind.name = 'X') // to save the day\n" +
                "and rownum /* won't run */ <= (1000 + 1000))\n";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testQuoteEscapeCursor() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select a from b\n" +
                "where ind.name = 'X''Y'\n";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testSubtractionInSelect() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select a, 3-1-2, -4- -1-1 from b\n" +
                "where d = c - 1\n";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testNextValueForSelect() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select next value for foo.bar \n" +
                "from core.custom_entity_data\n";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testPercentileQuery1() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY salary DESC) from core.custom_index_value ind";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testPercentileQuery2() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mark ASC) from core.custom_index_value ind";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testRowValueConstructorQuery() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select a_integer FROM aTable where (x_integer, y_integer) > (3, 4)";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testSingleTopLevelNot() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select * from t where not c = 5";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
    }

    @Test
    public void testHavingWithNot() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "select\n" +
                "\"WEB_STAT_ALIAS\".\"DOMAIN\" as \"c0\"\n" +
                "from \"WEB_STAT\" \"WEB_STAT_ALIAS\"\n" +
                "group by \"WEB_STAT_ALIAS\".\"DOMAIN\" having\n" +
                "(\n" +
                "(\n" +
                "NOT\n" +
                "(\n" +
                "(sum(\"WEB_STAT_ALIAS\".\"ACTIVE_VISITOR\") is null)\n" +
                ")\n" +
                "OR NOT((sum(\"WEB_STAT_ALIAS\".\"ACTIVE_VISITOR\") is null))\n" +
                ")\n" +
                "OR NOT((sum(\"WEB_STAT_ALIAS\".\"ACTIVE_VISITOR\") is null))\n" +
                ")\n" +
                "order by CASE WHEN \"WEB_STAT_ALIAS\".\"DOMAIN\" IS NULL THEN 1 ELSE 0 END,\n" +
                "\"WEB_STAT_ALIAS\".\"DOMAIN\" ASC";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testDoubleBackslash() throws Exception {
        String expectedNameToken = "testCursor";
        String expectedSelectStatement = "SELECT * FROM T WHERE A LIKE 'a\\(d'";
        
        String sql = "DECLARE " + expectedNameToken + " CURSOR FOR " + expectedSelectStatement;
        parseCursor(sql);
        
    }

    @Test
    public void testOpenCursor() throws Exception {
        String expectedNameToken = "testCursor";
        String sql = "OPEN " + expectedNameToken;
        parseOpen(sql);
    }

    @Test
    public void testFetchNext() throws Exception {
        String expectedNameToken = "testCursor";
        String sql = "FETCH NEXT FROM " + expectedNameToken;
        parseFetch(sql);
    }

}
