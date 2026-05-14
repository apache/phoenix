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

  // ----- negative cases -----

  private static void expectFail(String s) {
    try {
      BsonPathParser.parse(s);
      org.junit.Assert.fail("expected BsonPathSyntaxException for input: " + s);
    } catch (BsonPathSyntaxException ok) {
      // expected
    }
  }

  @Test public void rejectsEmpty() { expectFail(""); }
  @Test public void rejectsNullThrows() {
    try {
      BsonPathParser.parse(null);
      org.junit.Assert.fail("expected exception for null");
    } catch (BsonPathSyntaxException ok) {
      // expected
    }
  }
  @Test public void rejectsLeadingDot() { expectFail("."); }
  @Test public void rejectsTrailingDot() { expectFail("$.a."); }
  @Test public void rejectsBareLeadingDot() { expectFail(".a"); }
  @Test public void rejectsDoubleDot() { expectFail("$..a"); }
  @Test public void rejectsRecursiveDescent() { expectFail("$..b"); }
  @Test public void rejectsWildcardField() { expectFail("$.*"); }
  @Test public void rejectsWildcardBracket() { expectFail("$[*]"); }
  @Test public void rejectsFilter() { expectFail("$[?(@.x>1)]"); }
  @Test public void rejectsSlice() { expectFail("$[0:2]"); }
  @Test public void rejectsUnterminatedBracket() { expectFail("$.a["); }
  @Test public void rejectsUnterminatedQuoted() { expectFail("$['oops"); }
  @Test public void rejectsBadIdentifier() { expectFail("$.1bad"); }
  @Test public void rejectsLoneDollar() { expectFail("$"); }
  @Test public void rejectsTrailingChars() { expectFail("$.a junk"); }
  @Test public void rejectsNegativeIndexLooksLikeWildcard() { expectFail("$.a[-1]"); }
}
