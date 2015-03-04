package org.apache.phoenix.calcite;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.phoenix.calcite.PhoenixRel.Implementor;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.junit.Test;


public class ToExpressionTest extends BaseConnectionlessQueryTest {
	
	@Test
	public void toExpressionTest() throws Exception {
		final String expectedColName = "K2";
		final Object expectedValue = "foo";
		Connection conn = DriverManager.getConnection(getUrl());
		conn.createStatement().execute("CREATE TABLE t(k1 VARCHAR PRIMARY KEY, k2 VARCHAR, v1 VARCHAR)");
		final PTable table = conn.unwrap(PhoenixConnection.class).getMetaDataCache().getTable(new PTableKey(null,"T"));
		PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
		String query = "SELECT * FROM T WHERE K2 = 'foo'";
        SelectStatement select = new SQLParser(query).parseQuery();
        ColumnResolver resolver = FromCompiler.getResolverForQuery(select, conn.unwrap(PhoenixConnection.class));
        Expression where = WhereCompiler.compile(new StatementContext(stmt, resolver), select, null, Collections.<SubqueryParseNode>emptySet());
		
		JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
		RexBuilder builder = new RexBuilder(typeFactory);
		RelDataType dataType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
		RexInputRef ref = builder.makeInputRef(dataType,table.getColumn(expectedColName).getPosition());
		RexNode lit = builder.makeLiteral(expectedValue, dataType, true);
		RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, ref, lit);
		
		Implementor implementor = new PhoenixRelImplementorImpl();
		implementor.setContext(conn.unwrap(PhoenixConnection.class), table, null);
		Expression e = CalciteUtils.toExpression(call, implementor);
		assertEquals(where,e);
	}
}
