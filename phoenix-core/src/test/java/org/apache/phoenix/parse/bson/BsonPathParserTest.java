package org.apache.phoenix.parse.bson;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class BsonPathParserTest {

  @Test
  public void exceptionTypeIsCheckedAndCarriesOffset() {
    BsonPathSyntaxException e = new BsonPathSyntaxException("bad", 3);
    assertNotNull(e.getMessage());
    org.junit.Assert.assertEquals(3, e.getErrorOffset());
  }

  // ----- positive cases -----

  @Test
  public void parsesSingleFieldDot() throws Exception {
    org.junit.Assert.assertEquals("$.a", BsonPathParser.parse("$.a").toString());
  }

  @Test
  public void parsesNestedDot() throws Exception {
    org.junit.Assert.assertEquals("$.a.b.c", BsonPathParser.parse("$.a.b.c").toString());
  }

  @Test
  public void parsesArrayIndex() throws Exception {
    org.junit.Assert.assertEquals("$.a[0]", BsonPathParser.parse("$.a[0]").toString());
    org.junit.Assert.assertEquals("$.a[10][3]", BsonPathParser.parse("$.a[10][3]").toString());
  }

  @Test
  public void parsesBracketedQuoted() throws Exception {
    org.junit.Assert.assertEquals("$['weird key']",
        BsonPathParser.parse("$['weird key']").toString());
    org.junit.Assert.assertEquals("$['weird key']",
        BsonPathParser.parse("$[\"weird key\"]").toString());
  }

  @Test
  public void parsesBareDotPath() throws Exception {
    org.junit.Assert.assertEquals("$.a.b", BsonPathParser.parse("a.b").toString());
  }

  @Test
  public void parsesBareSingleField() throws Exception {
    org.junit.Assert.assertEquals("$.a", BsonPathParser.parse("a").toString());
  }

  @Test
  public void parsesBareWithIndex() throws Exception {
    org.junit.Assert.assertEquals("$.a[0]", BsonPathParser.parse("a[0]").toString());
  }

  @Test
  public void parsesQuotedWithEscapes() throws Exception {
    BsonPath p = BsonPathParser.parse("$['it\\'s \\\\ tricky']");
    org.junit.Assert.assertEquals("$['it\\'s \\\\ tricky']", p.toString());
  }

  @Test
  public void parsesMixedSegmentTypes() throws Exception {
    org.junit.Assert.assertEquals("$.a[3].b['x y']",
        BsonPathParser.parse("$.a[3].b['x y']").toString());
  }
}
