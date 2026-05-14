package org.apache.phoenix.parse.bson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.phoenix.parse.bson.BsonPath.FieldSegment;
import org.apache.phoenix.parse.bson.BsonPath.IndexSegment;
import org.apache.phoenix.parse.bson.BsonPath.Segment;
import org.junit.Test;

public class BsonPathTest {

  @Test
  public void equalsIsStructural() {
    BsonPath a = new BsonPath(Arrays.<Segment>asList(new FieldSegment("a"), new FieldSegment("b")));
    BsonPath b = new BsonPath(Arrays.<Segment>asList(new FieldSegment("a"), new FieldSegment("b")));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void differentSegmentTypesAreNotEqual() {
    BsonPath f = new BsonPath(Arrays.<Segment>asList(new FieldSegment("0")));
    BsonPath i = new BsonPath(Arrays.<Segment>asList(new IndexSegment(0)));
    assertNotEquals(f, i);
  }

  @Test
  public void canonicalToStringForSimpleDotPath() {
    BsonPath p = new BsonPath(Arrays.<Segment>asList(new FieldSegment("a"), new FieldSegment("b")));
    assertEquals("$.a.b", p.toString());
  }

  @Test
  public void canonicalToStringEscapesQuotedSegment() {
    BsonPath p = new BsonPath(Arrays.<Segment>asList(new FieldSegment("weird key")));
    assertEquals("$['weird key']", p.toString());
  }

  @Test
  public void canonicalToStringMixesArrayIndex() {
    BsonPath p = new BsonPath(Arrays.<Segment>asList(
        new FieldSegment("a"), new IndexSegment(3), new FieldSegment("b")));
    assertEquals("$.a[3].b", p.toString());
  }

  @Test
  public void quotedSegmentEscapesSingleQuoteAndBackslash() {
    BsonPath p = new BsonPath(Arrays.<Segment>asList(new FieldSegment("it's \\ tricky")));
    assertTrue(p.toString().contains("['it\\'s \\\\ tricky']"));
  }
}
