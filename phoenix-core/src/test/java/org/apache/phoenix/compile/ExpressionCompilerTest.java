package org.apache.phoenix.compile;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.LikeParseNode;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ExpressionCompilerTest extends BaseConnectionlessQueryTest {
	private ExpressionCompiler getExpressionCompiler() throws SQLException {
		PhoenixConnection pconn = DriverManager.getConnection(getUrl(),
				PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(
				PhoenixConnection.class);
		PhoenixStatement phoenixStatement = new PhoenixStatement(pconn);
		StatementContext statementContext = new StatementContext(
				phoenixStatement);
		return new ExpressionCompiler(statementContext);
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testVisitLeaveForLikeParseNode() throws SQLException {
		ExpressionCompiler expressionCompiler = getExpressionCompiler();
		for (PDataType<?> dataType : PDataType.values()) {
			if (dataType.isArrayType() || PVarbinary.INSTANCE == dataType
					|| PBinary.INSTANCE == dataType) {
				continue;
			}
			Object value1 = dataType.getSampleValue();
			LiteralParseNode parseNode1 = new LiteralParseNode(value1);
			Object value2 = dataType.getSampleValue();
			LiteralParseNode parseNode2 = new LiteralParseNode(value2);

			ParseNodeFactory factory = new ParseNodeFactory();
			LikeParseNode likeParseNode = factory.like(parseNode1, parseNode2,
					false, LikeType.CASE_INSENSITIVE);
			Expression expression1 = LiteralExpression.newConstant(value1,
					dataType);
			Expression expression2 = LiteralExpression.newConstant(value2,
					dataType);

			if (PVarchar.INSTANCE != dataType && PChar.INSTANCE != dataType) {
				try {
					expressionCompiler.visitLeave(likeParseNode,
							Lists.newArrayList(expression1, expression2));
					fail("LikeParseNode should throw exception for data type which is not coercible to VARCHAR");
				} catch (SQLException sqe) {
					assertEquals(
							SQLExceptionCode.TYPE_NOT_SUPPORTED_FOR_OPERATOR
									.getErrorCode(),
							sqe.getErrorCode());
				}
			} else {
				expressionCompiler.visitLeave(likeParseNode,
						Lists.newArrayList(expression1, expression2));
			}
		}
	}

}
