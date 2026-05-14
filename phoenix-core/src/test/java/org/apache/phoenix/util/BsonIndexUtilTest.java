package org.apache.phoenix.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.junit.Test;

public class BsonIndexUtilTest {

  private static ParseNode parseExpr(String s) throws Exception {
    return new SQLParser(s).parseExpression();
  }

  @Test
  public void detectsBsonValueAtTopLevel() throws Exception {
    assertTrue(BsonIndexUtil.containsBsonExpression(
        parseExpr("BSON_VALUE(doc, '$.a', 'VARCHAR')")));
  }

  @Test
  public void detectsBsonValueNested() throws Exception {
    assertTrue(BsonIndexUtil.containsBsonExpression(
        parseExpr("UPPER(BSON_VALUE(doc, '$.a', 'VARCHAR'))")));
  }

  @Test
  public void detectsJsonValue() throws Exception {
    assertTrue(BsonIndexUtil.containsBsonExpression(
        parseExpr("JSON_VALUE(doc, '$.a')")));
  }

  @Test
  public void plainExpressionIsNotBson() throws Exception {
    assertFalse(BsonIndexUtil.containsBsonExpression(parseExpr("a + 1")));
  }

  @Test
  public void wholeColumnIsNotBson() throws Exception {
    assertFalse(BsonIndexUtil.containsBsonExpression(parseExpr("doc")));
  }
}
