/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.util.proto;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

/**
 * Recursive-descent parser for path literals.
 */
public final class ProtoPathParser {

  /** One parsed segment: an identifier plus an optional integer subscript. */
  public static final class ParsedSegment {
    public final String identifier;
    /** {@code null} when no {@code [N]} subscript is present. */
    public final Long intSubscript;

    ParsedSegment(String identifier, Long intSubscript) {
      this.identifier = identifier;
      this.intSubscript = intSubscript;
    }
  }

  private ProtoPathParser() {
  }

  public static List<ParsedSegment> parse(String path) throws SQLException {
    if (path == null || path.isEmpty()) {
      throw syntaxError("path literal must not be empty", path);
    }
    List<ParsedSegment> segments = new ArrayList<>();
    int i = 0;
    int len = path.length();
    while (i < len) {
      int identStart = i;
      char c = path.charAt(i);
      if (!isIdentifierStart(c)) {
        throw syntaxError("expected identifier at position " + i, path);
      }
      i++;
      while (i < len && isIdentifierPart(path.charAt(i))) {
        i++;
      }
      String identifier = path.substring(identStart, i);
      Long subscript = null;
      if (i < len && path.charAt(i) == '[') {
        int bracketStart = i;
        i++;
        int numStart = i;
        if (i < len && path.charAt(i) == '-') {
          i++;
        }
        int digitsStart = i;
        while (i < len && isDigit(path.charAt(i))) {
          i++;
        }
        if (i == digitsStart) {
          throw syntaxError("expected integer inside '[...]' at position " + bracketStart, path);
        }
        if (i >= len || path.charAt(i) != ']') {
          throw syntaxError("expected ']' at position " + i, path);
        }
        String numText = path.substring(numStart, i);
        try {
          subscript = Long.parseLong(numText);
        } catch (NumberFormatException e) {
          throw syntaxError("integer subscript out of range: " + numText, path);
        }
        i++;
      }
      segments.add(new ParsedSegment(identifier, subscript));
      if (i < len) {
        if (path.charAt(i) == '[') {
          throw syntaxError("chained subscripts like 'name[i][j]' are not supported "
            + "at position " + i + "; protobuf has no multi-dimensional arrays. "
            + "Use an explicit wrapper-message field between the two subscripts.", path);
        }
        if (path.charAt(i) != '.') {
          throw syntaxError("expected '.' between path segments at position " + i, path);
        }
        i++;
        if (i == len) {
          throw syntaxError("trailing '.' at end of path", path);
        }
      }
    }
    return segments;
  }

  private static boolean isIdentifierStart(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
  }

  private static boolean isIdentifierPart(char c) {
    return isIdentifierStart(c) || (c >= '0' && c <= '9') || c == '-';
  }

  private static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  private static SQLException syntaxError(String detail, String path) {
    return new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_PATH_SYNTAX_ERROR)
      .setMessage("Invalid PROTO_VALUE path '" + path + "': " + detail).build().buildException();
  }
}
