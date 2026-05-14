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
}
