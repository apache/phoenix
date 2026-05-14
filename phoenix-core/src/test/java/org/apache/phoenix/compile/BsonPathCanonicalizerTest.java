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
}
