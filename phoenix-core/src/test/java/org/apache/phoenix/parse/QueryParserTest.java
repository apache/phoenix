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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;


public class QueryParserTest {
    private void parseQuery(String sql) throws IOException, SQLException {
        SQLParser parser = new SQLParser(new StringReader(sql));
        BindableStatement stmt = null;
        stmt = parser.parseStatement();
        if (stmt.getOperation() != Operation.QUERY) {
            return;
        }
        String newSQL = stmt.toString();
        SQLParser newParser = new SQLParser(new StringReader(newSQL));
        BindableStatement newStmt = null;
        try {
            newStmt = newParser.parseStatement();
        } catch (SQLException e) {
            fail("Unable to parse new:\n" + newSQL);
        }
        assertEquals("Expected equality:\n" + sql + "\n" + newSQL, stmt, newStmt);
    }
    
    @Test
    public void testParsePreQuery0() throws Exception {
        String sql = ((
            "select a from b\n" +
            "where ((ind.name = 'X')" +
            "and rownum <= (1000 + 1000))\n"
            ));
        parseQuery(sql);
    }

    @Test
    public void testParsePreQuery1() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ count(1) from core.search_name_lookup ind\n" +
            "where( (ind.name = 'X'\n" +
            "and rownum <= 1 + 2)\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't'))"
            ));
        parseQuery(sql);
    }

    @Test
    public void testParsePreQuery2() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ count(1) from core.custom_index_value ind\n" + 
            "where (ind.string_value in ('a', 'b', 'c', 'd'))\n" + 
            "and rownum <= ( 3 + 1 )\n" + 
            "and (ind.organization_id = '000000000000000')\n" + 
            "and (ind.key_prefix = '00T')\n" + 
            "and (ind.deleted = '0')\n" + 
            "and (ind.index_num = 1)"
            ));
        parseQuery(sql);
    }

    @Test
    public void testParsePreQuery3() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ count(1) from core.custom_index_value ind\n" + 
            "where (ind.number_value > 3)\n" + 
            "and rownum <= 1000\n" + 
            "and (ind.organization_id = '000000000000000')\n" + 
            "and (ind.key_prefix = '001'\n" + 
            "and (ind.deleted = '0'))\n" + 
            "and (ind.index_num = 2)"
            ));
        parseQuery(sql);
    }

    @Test
    public void testParsePreQuery4() throws Exception {
        String sql = ((
            "select /*+ index(t iecustom_entity_data_created) */ /*gatherSlowStats*/ count(1) from core.custom_entity_data t\n" + 
            "where (t.created_date > to_date('01/01/2001'))\n" + 
            "and rownum <= 4500\n" + 
            "and (t.organization_id = '000000000000000')\n" + 
            "and (t.key_prefix = '001')"
            ));
        parseQuery(sql);
    }

    @Test
    public void testCountDistinctQuery() throws Exception {
        String sql = ((
                "select count(distinct foo) from core.custom_entity_data t\n"
                        + "where (t.created_date > to_date('01/01/2001'))\n"
                        + "and (t.organization_id = '000000000000000')\n"
                        + "and (t.key_prefix = '001')\n" + "limit 4500"));
        parseQuery(sql);
    }

    @Test
    public void testIsNullQuery() throws Exception {
        String sql = ((
            "select count(foo) from core.custom_entity_data t\n" + 
            "where (t.created_date is null)\n" + 
            "and (t.organization_id is not null)\n"
            ));
        parseQuery(sql);
    }

    @Test
    public void testAsInColumnAlias() throws Exception {
        String sql = ((
            "select count(foo) AS c from core.custom_entity_data t\n" + 
            "where (t.created_date is null)\n" + 
            "and (t.organization_id is not null)\n"
            ));
        parseQuery(sql);
    }

    @Test
    public void testParseJoin1() throws Exception {
        String sql = ((
            "select /*SOQL*/ \"Id\"\n" + 
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
            "where (\"Marketing_Offer_Code__c\" = 'FSCR')"
            ));
        parseQuery(sql);
    }

    @Test
    public void testParseJoin2() throws Exception {
        String sql = ((
            "select /*rptacctlist 00O40000002C3of*/ \"00N40000001M8VK\",\n" + 
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
            "AND (\"LAST_UPDATE\" >= to_date('2009-08-01 07:00:00'))"
            ));
        parseQuery(sql);
    }
    
    @Test
    public void testNegative1() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ count(1) core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MISMATCHED_TOKEN.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testNegative2() throws Exception {
        String sql = ((
            "seelect /*gatherSlowStats*/ count(1) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"seelect\" at line 1, column 1."));
        }
    }

    @Test
    public void testNegative3() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ count(1) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't'))"
            ));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 603 (42P00): Syntax error. Unexpected input. Expecting \"EOF\", got \")\" at line 6, column 26."));
        }
    }

    @Test
    public void testNegativeCountDistinct() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ max( distinct 1) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLFeatureNotSupportedException e) {
            // expected
        }
    }

    @Test
    public void testNegativeCountStar() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ max(*) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"*\" at line 1, column 32."));
        }
    }

    @Test
    public void testUnknownFunction() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ bogus_function(ind.key_prefix) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UNKNOWN_FUNCTION.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testNegativeNonBooleanWhere() throws Exception {
        String sql = ((
            "select /*gatherSlowStats*/ max( distinct 1) from core.search_name_lookup ind\n" +
            "where 1"
            ));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLFeatureNotSupportedException e) {
            // expected
        }
    }
    
    @Test
    public void testCommentQuery() throws Exception {
        String sql = ((
            "select a from b -- here we come\n" +
            "where ((ind.name = 'X') // to save the day\n" +
            "and rownum /* won't run */ <= (1000 + 1000))\n"
            ));
        parseQuery(sql);
    }

    @Test
    public void testQuoteEscapeQuery() throws Exception {
        String sql = ((
            "select a from b\n" +
            "where ind.name = 'X''Y'\n"
            ));
        parseQuery(sql);
    }

    @Test
    public void testSubtractionInSelect() throws Exception {
        String sql = ((
            "select a, 3-1-2, -4- -1-1 from b\n" +
            "where d = c - 1\n"
            ));
        parseQuery(sql);
    }

    @Test
    public void testParsingStatementWithMispellToken() throws Exception {
        try {
            String sql = ((
                    "selects a from b\n" +
                    "where e = d\n"));
            parseQuery(sql);
            fail("Should have caught exception.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"selects\" at line 1, column 1."));
        }
        try {
            String sql = ((
                    "select a froms b\n" +
                    "where e = d\n"));
            parseQuery(sql);
            fail("Should have caught exception.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 602 (42P00): Syntax error. Missing \"FROM\" at line 1, column 16."));
        }
    }

    @Test
    public void testParsingStatementWithExtraToken() throws Exception {
        try {
            String sql = ((
                    "select a,, from b\n" +
                    "where e = d\n"));
            parseQuery(sql);
            fail("Should have caught exception.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \",\" at line 1, column 10."));
        }
        try {
            String sql = ((
                    "select a from from b\n" +
                    "where e = d\n"));
            parseQuery(sql);
            fail("Should have caught exception.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"from\" at line 1, column 15."));
        }
    }

    @Test
    public void testParsingStatementWithMissingToken() throws Exception {
        try {
            String sql = ((
                    "select a b\n" +
                    "where e = d\n"));
            parseQuery(sql);
            fail("Should have caught exception.");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MISMATCHED_TOKEN.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testParseCreateTableInlinePrimaryKeyWithOrder() throws Exception {
    	for (String order : new String[]{"asc", "desc"}) {
            String s = "create table core.entity_history_archive (id char(15) primary key ${o})".replace("${o}", order);
    		CreateTableStatement stmt = (CreateTableStatement)new SQLParser((s)).parseStatement();
    		List<ColumnDef> columnDefs = stmt.getColumnDefs();
    		assertEquals(1, columnDefs.size());
    		assertEquals(SortOrder.fromDDLValue(order), columnDefs.iterator().next().getSortOrder()); 
    	}
    }
    
    @Test
    public void testParseCreateTableOrderWithoutPrimaryKeyFails() throws Exception {
    	for (String order : new String[]{"asc", "desc"}) {
    		String stmt = "create table core.entity_history_archive (id varchar(20) ${o})".replace("${o}", order);
    		try {
    			new SQLParser((stmt)).parseStatement();
    			fail("Expected parse exception to be thrown");
    		} catch (SQLException e) {
    			String errorMsg = "ERROR 603 (42P00): Syntax error. Unexpected input. Expecting \"RPAREN\", got \"${o}\"".replace("${o}", order);
    			assertTrue("Expected message to contain \"" + errorMsg + "\" but got \"" + e.getMessage() + "\"", e.getMessage().contains(errorMsg));
    		}
    	}
    }
    
    @Test
    public void testParseCreateTablePrimaryKeyConstraintWithOrder() throws Exception {
    	for (String order : new String[]{"asc", "desc"}) {
    		String s = "create table core.entity_history_archive (id CHAR(15), name VARCHAR(150), constraint pk primary key (id ${o}, name ${o}))".replace("${o}", order);
    		CreateTableStatement stmt = (CreateTableStatement)new SQLParser((s)).parseStatement();
    		PrimaryKeyConstraint pkConstraint = stmt.getPrimaryKeyConstraint();
    		List<Pair<ColumnName,SortOrder>> columns = pkConstraint.getColumnNames();
    		assertEquals(2, columns.size());
    		for (Pair<ColumnName,SortOrder> pair : columns) {
    			assertEquals(SortOrder.fromDDLValue(order), pkConstraint.getColumn(pair.getFirst()).getSecond());
    		}    		
    	}
    }

    @Test
    public void testParseCreateTableCommaBeforePrimaryKeyConstraint() throws Exception {
        for (String leadingComma : new String[]{",", ""}) {
            String s = "create table core.entity_history_archive (id CHAR(15), name VARCHAR(150)${o} constraint pk primary key (id))".replace("${o}", leadingComma);

            CreateTableStatement stmt = (CreateTableStatement)new SQLParser((s)).parseStatement();

            assertEquals(2, stmt.getColumnDefs().size());
            assertNotNull(stmt.getPrimaryKeyConstraint());
        }
    }

    @Test
    public void testInvalidTrailingCommaOnCreateTable() throws Exception {
        String sql = (
                (
                        "create table foo (c1 varchar primary key, c2 varchar,)"));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MISMATCHED_TOKEN.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
	public void testCreateSequence() throws Exception {
		String sql = ((
				"create sequence foo.bar\n" + 
						"start with 0\n"	+ 
						"increment by 1\n"));
		parseQuery(sql);
	}
	
	@Test
	public void testNextValueForSelect() throws Exception {
		String sql = ((
				"select next value for foo.bar \n" + 
						"from core.custom_entity_data\n"));						
		parseQuery(sql);
	}
	
	@Test
    public void testNextValueForWhere() throws Exception {
        String sql = ((
                "upsert into core.custom_entity_data\n" + 
                        "select next value for foo.bar from core.custom_entity_data\n"));                    
        parseQuery(sql);
    }
	
    public void testBadCharDef() throws Exception {
        try {
            String sql = ("CREATE TABLE IF NOT EXISTS testBadVarcharDef" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col CHAR(0))");
            parseQuery(sql);
            fail("Should have caught bad char definition.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 208 (22003): CHAR or VARCHAR must have a positive length. columnName=COL"));
        }
        try {
            String sql = ("CREATE TABLE IF NOT EXISTS testBadVarcharDef" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col CHAR)");
            parseQuery(sql);
            fail("Should have caught bad char definition.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 207 (22003): Missing length for CHAR. columnName=COL"));
        }
    }

    @Test
    public void testBadVarcharDef() throws Exception {
        try {
            String sql = ("CREATE TABLE IF NOT EXISTS testBadVarcharDef" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col VARCHAR(0))");
            parseQuery(sql);
            fail("Should have caught bad varchar definition.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 208 (22003): CHAR or VARCHAR must have a positive length. columnName=COL"));
        }
    }

    @Test
    public void testBadDecimalDef() throws Exception {
        try {
            String sql = ("CREATE TABLE IF NOT EXISTS testBadDecimalDef" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col DECIMAL(0, 5))");
            parseQuery(sql);
            fail("Should have caught bad decimal definition.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 209 (22003): Decimal precision outside of range. Should be within 1 and 38. columnName=COL"));
        }
        try {
            String sql = ("CREATE TABLE IF NOT EXISTS testBadDecimalDef" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col DECIMAL(40, 5))");
            parseQuery(sql);
            fail("Should have caught bad decimal definition.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 209 (22003): Decimal precision outside of range. Should be within 1 and 38. columnName=COL"));
        }
    }

    @Test
    public void testBadBinaryDef() throws Exception {
        try {
            String sql = ("CREATE TABLE IF NOT EXISTS testBadBinaryDef" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col BINARY(0))");
            parseQuery(sql);
            fail("Should have caught bad binary definition.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 211 (22003): BINARY must have a positive length. columnName=COL"));
        }
        try {
            String sql = ("CREATE TABLE IF NOT EXISTS testBadVarcharDef" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col BINARY)");
            parseQuery(sql);
            fail("Should have caught bad char definition.");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 210 (22003): Missing length for BINARY. columnName=COL"));
        }
    }

    @Test
    public void testPercentileQuery1() throws Exception {
        String sql = (
                (
                        "select PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY salary DESC) from core.custom_index_value ind"));
        parseQuery(sql);
    }

    @Test
    public void testPercentileQuery2() throws Exception {
        String sql = (
                (
                        "select PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mark ASC) from core.custom_index_value ind"));
        parseQuery(sql);
    }
    
    @Test
    public void testRowValueConstructorQuery() throws Exception {
        String sql = (
                (
                        "select a_integer FROM aTable where (x_integer, y_integer) > (3, 4)"));
        parseQuery(sql);
    }

    @Test
    public void testSingleTopLevelNot() throws Exception {
        String sql = (
                (
                        "select * from t where not c = 5"));
        parseQuery(sql);
    }

    @Test
    public void testTopLevelNot() throws Exception {
        String sql = (
                (
                        "select * from t where not c"));
        parseQuery(sql);
    }

    @Test
    public void testRVCInList() throws Exception {
        String sql = (
                (
                        "select * from t where k in ( (1,2), (3,4) )"));
        parseQuery(sql);
    }

    @Test
    public void testInList() throws Exception {
        String sql = (
                (
                        "select * from t where k in ( 1,2 )"));
        parseQuery(sql);
    }

    @Test
    public void testInvalidSelectStar() throws Exception {
        String sql = (
                (
                        "select *,k from t where k in ( 1,2 )"));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MISMATCHED_TOKEN.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testTableNameStartsWithUnderscore() throws Exception {
        String sql = (
                (
                        "select* from _t where k in ( 1,2 )"));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.PARSER_ERROR.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testValidUpsertSelectHint() throws Exception {
        String sql = (
                (
                        "upsert /*+ NO_INDEX */ into t select k from t where k in ( 1,2 )"));
            parseQuery(sql);
    }

    @Test
    public void testHavingWithNot() throws Exception {
        String sql = (
                (
                        "select\n" + 
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
                        "\"WEB_STAT_ALIAS\".\"DOMAIN\" ASC"));
        parseQuery(sql);
    }

    @Test
    public void testToDateInList() throws Exception {
        String sql = (
                ("select * from date_test where d in (to_date('2013-11-04 09:12:00'))"));
        parseQuery(sql);
    }
    
    @Test
    public void testDateLiteral() throws Exception {
        String sql = (
                (
                        "select * from t where d = DATE '2013-11-04 09:12:00'"));
        parseQuery(sql);
    }

    @Test
    public void testTimeLiteral() throws Exception {
        String sql = (
                (
                        "select * from t where d = TIME '2013-11-04 09:12:00'"));
        parseQuery(sql);
    }


    @Test
    public void testTimestampLiteral() throws Exception {
        String sql = (
                (
                        "select * from t where d = TIMESTAMP '2013-11-04 09:12:00'"));
        parseQuery(sql);
    }
    
    @Test
    public void testUnsignedDateLiteral() throws Exception {
        String sql = (
                (
                        "select * from t where d = UNSIGNED_DATE '2013-11-04 09:12:00'"));
        parseQuery(sql);
    }

    @Test
    public void testUnsignedTimeLiteral() throws Exception {
        String sql = (
                (
                        "select * from t where d = UNSIGNED_TIME '2013-11-04 09:12:00'"));
        parseQuery(sql);
    }


    @Test
    public void testUnsignedTimestampLiteral() throws Exception {
        String sql = (
                (
                        "select * from t where d = UNSIGNED_TIMESTAMP '2013-11-04 09:12:00'"));
        parseQuery(sql);
    }
    
    @Test
    public void testParseDateEquality() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select a from b\n" +
            "where date '2014-01-04' = date '2014-01-04'"
            ));
        parser.parseStatement();
    }

    @Test
    public void testParseDateIn() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select a from b\n" +
            "where date '2014-01-04' in (date '2014-01-04')"
            ));
        parser.parseStatement();
    }
    
    @Test
    public void testUnknownLiteral() throws Exception {
        String sql = (
                (
                        "select * from t where d = FOO '2013-11-04 09:12:00'"));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testUnsupportedLiteral() throws Exception {
        String sql = (
                (
                        "select * from t where d = DECIMAL '2013-11-04 09:12:00'"));
        try {
            parseQuery(sql);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testAnyElementExpression1() throws Exception {
        String sql = "select * from t where 'a' = ANY(a)";
        parseQuery(sql);
    }

    @Test
    public void testAnyElementExpression2() throws Exception {
        String sql = "select * from t where 'a' <= ANY(a-b+1)";
        parseQuery(sql);
    }

    @Test
    public void testAllElementExpression() throws Exception {
        String sql = "select * from t where 'a' <= ALL(a-b+1)";
        parseQuery(sql);
    }
}
