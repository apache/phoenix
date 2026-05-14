package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.junit.Test;

public class BsonPathCanonicalizerTest {

  private static ParseNode parseExpr(String s) throws Exception {
    return new SQLParser(s).parseExpression();
  }

  @Test
  public void nonBsonNodePassesThrough() throws Exception {
    ParseNode in = parseExpr("a + 1");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertNotNull(out);
    assertEquals(in.toString(), out.toString());
  }

  @Test
  public void canonicalizesBareDotPath() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a.b', 'VARCHAR')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(" BSON_VALUE(DOC,'$.a.b','VARCHAR')", out.toString());
  }

  @Test
  public void canonicalIsAlreadyCanonical() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, '$.a.b', 'VARCHAR')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(" BSON_VALUE(DOC,'$.a.b','VARCHAR')", out.toString());
  }

  @Test
  public void canonicalizesTypeCase() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, '$.a', 'varchar')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(" BSON_VALUE(DOC,'$.a','VARCHAR')", out.toString());
  }

  @Test
  public void canonicalizesArrayIndex() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a[0]', 'BIGINT')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(" BSON_VALUE(DOC,'$.a[0]','BIGINT')", out.toString());
  }

  @Test
  public void canonicalizesQuotedKey() throws Exception {
    // Phoenix treats double-quoted strings as identifiers, not string literals,
    // so the path arg here is a ColumnParseNode and the canonicalizer leaves it unchanged.
    ParseNode in = parseExpr("BSON_VALUE(doc, \"['weird key']\", 'VARCHAR')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(in.toString(), out.toString());
  }

  @Test
  public void invalidPathIsLeftAlone() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, '$..bad', 'VARCHAR')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    // unsupported path → no rewrite, returns input unchanged.
    assertEquals(in.toString(), out.toString());
  }

  @Test
  public void argCountMismatchLeftAlone() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a.b')");  // missing type arg
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(in.toString(), out.toString());
  }

  @Test
  public void jsonValueRewritesToBsonValueVarchar() throws Exception {
    ParseNode in = parseExpr("JSON_VALUE(doc, '$.a.b')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(" BSON_VALUE(DOC,'$.a.b','VARCHAR')", out.toString());
  }

  @Test
  public void jsonValueWithBarePath() throws Exception {
    ParseNode in = parseExpr("JSON_VALUE(doc, 'a.b')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(" BSON_VALUE(DOC,'$.a.b','VARCHAR')", out.toString());
  }

  @Test
  public void jsonValueWithUnsupportedPathLeftAlone() throws Exception {
    ParseNode in = parseExpr("JSON_VALUE(doc, '$.*')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(in.toString(), out.toString());
  }

  @Test
  public void canonicalizesInsideEquality() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a.b', 'varchar') = 'x'");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(" BSON_VALUE(DOC,'$.a.b','VARCHAR') = 'x'", out.toString());
  }

  @Test
  public void canonicalizesInsideAnd() throws Exception {
    ParseNode in = parseExpr(
        "BSON_VALUE(doc, 'a', 'varchar') = 'x' AND BSON_VALUE(doc, 'b', 'bigint') > 5");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(
        "( BSON_VALUE(DOC,'$.a','VARCHAR') = 'x' AND  BSON_VALUE(DOC,'$.b','BIGINT') > 5)",
        out.toString());
  }

  @Test
  public void canonicalizesInsideIn() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a', 'varchar') IN ('x', 'y')");
    ParseNode out = BsonPathCanonicalizer.rewrite(in);
    assertEquals(" BSON_VALUE(DOC,'$.a','VARCHAR') IN('x','y')", out.toString());
  }

  @Test
  public void extractPathReturnsBsonPathForCanonicalizable() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, 'a.b', 'VARCHAR')");
    ParseNode canon = BsonPathCanonicalizer.rewrite(in);
    org.apache.phoenix.parse.bson.BsonPath p = BsonPathCanonicalizer.extractPath(canon);
    assertEquals("$.a.b", p.toString());
  }

  @Test
  public void extractPathReturnsNullForOther() throws Exception {
    ParseNode in = parseExpr("a + 1");
    org.junit.Assert.assertNull(BsonPathCanonicalizer.extractPath(in));
  }

  @Test
  public void extractPathReturnsNullForBadPath() throws Exception {
    ParseNode in = parseExpr("BSON_VALUE(doc, '$..bad', 'VARCHAR')");
    org.junit.Assert.assertNull(BsonPathCanonicalizer.extractPath(in));
  }
}
