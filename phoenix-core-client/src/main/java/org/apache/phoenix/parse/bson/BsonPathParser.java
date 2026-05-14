package org.apache.phoenix.parse.bson;

import java.util.ArrayList;
import java.util.List;
import org.apache.phoenix.parse.bson.BsonPath.FieldSegment;
import org.apache.phoenix.parse.bson.BsonPath.IndexSegment;
import org.apache.phoenix.parse.bson.BsonPath.Segment;

/**
 * Recursive-descent parser for the JSONPath subset used by Phoenix BSON path indexes.
 * Accepted forms: {@code $.a.b}, {@code $.a[0]}, {@code $['key']}, {@code $["key"]},
 * and the bare equivalents {@code a.b}, {@code a}, {@code a[0]}.
 * Rejects wildcards, filters, recursive descent, slices.
 */
public final class BsonPathParser {

  private final String input;
  private int pos;

  private BsonPathParser(String input) {
    this.input = input;
    this.pos = 0;
  }

  public static BsonPath parse(String input) throws BsonPathSyntaxException {
    if (input == null || input.isEmpty()) {
      throw new BsonPathSyntaxException("path must be non-empty", 0);
    }
    BsonPathParser p = new BsonPathParser(input);
    return p.parsePath();
  }

  private BsonPath parsePath() throws BsonPathSyntaxException {
    List<Segment> segments = new ArrayList<>();
    if (peek() == '$') {
      pos++;
      // After '$', either end (illegal — empty path), '.', or '['.
      if (pos == input.length()) {
        throw new BsonPathSyntaxException("path must have at least one segment after '$'", pos);
      }
    }
    boolean first = true;
    while (pos < input.length()) {
      char c = input.charAt(pos);
      if (c == '.') {
        pos++;
        if (pos < input.length() && input.charAt(pos) == '.') {
          throw new BsonPathSyntaxException("recursive descent ($..) is not supported", pos);
        }
        segments.add(parseDotField());
      } else if (c == '[') {
        segments.add(parseBracketSegment());
      } else if (first) {
        // Bare leading field, e.g. "a.b" or "a[0]".
        segments.add(parseDotField());
      } else {
        throw new BsonPathSyntaxException("unexpected char '" + c + "'", pos);
      }
      first = false;
    }
    if (segments.isEmpty()) {
      throw new BsonPathSyntaxException("path is empty", 0);
    }
    return new BsonPath(segments);
  }

  private FieldSegment parseDotField() throws BsonPathSyntaxException {
    int start = pos;
    if (pos == input.length()) {
      throw new BsonPathSyntaxException("expected field name", pos);
    }
    char c0 = input.charAt(pos);
    if (c0 == '*') {
      throw new BsonPathSyntaxException("wildcards are not supported", pos);
    }
    if (!isIdStart(c0)) {
      throw new BsonPathSyntaxException("invalid field name start '" + c0 + "'", pos);
    }
    pos++;
    while (pos < input.length() && isIdPart(input.charAt(pos))) {
      pos++;
    }
    return new FieldSegment(input.substring(start, pos));
  }

  private Segment parseBracketSegment() throws BsonPathSyntaxException {
    if (input.charAt(pos) != '[') {
      throw new BsonPathSyntaxException("expected '['", pos);
    }
    int openPos = pos;
    pos++;
    if (pos == input.length()) {
      throw new BsonPathSyntaxException("unterminated '['", openPos);
    }
    char first = input.charAt(pos);
    if (first == '*') {
      throw new BsonPathSyntaxException("wildcards are not supported", pos);
    }
    if (first == '?') {
      throw new BsonPathSyntaxException("filter expressions are not supported", pos);
    }
    Segment seg;
    if (first == '\'' || first == '"') {
      seg = parseQuotedSegment(first);
    } else if (first >= '0' && first <= '9') {
      seg = parseIndexSegment(openPos);
    } else {
      throw new BsonPathSyntaxException("expected quoted key or array index", pos);
    }
    if (pos >= input.length() || input.charAt(pos) != ']') {
      throw new BsonPathSyntaxException("expected ']'", pos);
    }
    pos++;
    return seg;
  }

  private FieldSegment parseQuotedSegment(char quote) throws BsonPathSyntaxException {
    pos++;
    StringBuilder sb = new StringBuilder();
    while (pos < input.length()) {
      char c = input.charAt(pos);
      if (c == '\\') {
        if (pos + 1 >= input.length()) {
          throw new BsonPathSyntaxException("dangling backslash in quoted segment", pos);
        }
        char esc = input.charAt(pos + 1);
        if (esc == '\\' || esc == quote) {
          sb.append(esc);
          pos += 2;
        } else {
          throw new BsonPathSyntaxException("invalid escape '\\" + esc + "'", pos);
        }
      } else if (c == quote) {
        pos++;
        return new FieldSegment(sb.toString());
      } else {
        sb.append(c);
        pos++;
      }
    }
    throw new BsonPathSyntaxException("unterminated quoted segment", pos);
  }

  private IndexSegment parseIndexSegment(int openPos) throws BsonPathSyntaxException {
    int start = pos;
    while (pos < input.length() && Character.isDigit(input.charAt(pos))) {
      pos++;
    }
    if (pos < input.length() && input.charAt(pos) == ':') {
      throw new BsonPathSyntaxException("array slice is not supported", pos);
    }
    int idx;
    try {
      idx = Integer.parseInt(input.substring(start, pos));
    } catch (NumberFormatException nfe) {
      throw new BsonPathSyntaxException("invalid array index", openPos);
    }
    return new IndexSegment(idx);
  }

  private char peek() {
    return pos < input.length() ? input.charAt(pos) : '\0';
  }

  private static boolean isIdStart(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
  }

  private static boolean isIdPart(char c) {
    return isIdStart(c) || (c >= '0' && c <= '9');
  }
}
