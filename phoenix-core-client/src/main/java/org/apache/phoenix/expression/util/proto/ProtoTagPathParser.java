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
 * Parser for the schema-less tag-number path grammar used by {@code PROTO_VALUE_AT_TAG}. Produces a
 * list of {@link TagSegment} from a string like {@code "1.2[0].5"}.
 * <p>
 * Grammar:
 *
 * <pre>
 *   tag_path    ::= tag_segment ( '.' tag_segment )*
 *   tag_segment ::= unsigned_integer [ '[' unsigned_integer ']' ]
 * </pre>
 */
public final class ProtoTagPathParser {

  /** One parsed segment: a tag number plus an optional repeated-element index. */
  public static final class TagSegment {
    public final int tagNumber;
    /** -1 when no {@code [N]} subscript is present. */
    public final int index;

    TagSegment(int tagNumber, int index) {
      this.tagNumber = tagNumber;
      this.index = index;
    }

    public boolean hasIndex() {
      return index >= 0;
    }
  }

  private ProtoTagPathParser() {
  }

  public static List<TagSegment> parse(String path) throws SQLException {
    if (path == null || path.isEmpty()) {
      throw syntaxError("tag-path must not be empty", path);
    }
    List<TagSegment> segments = new ArrayList<>();
    int i = 0;
    int len = path.length();
    while (i < len) {
      int numStart = i;
      if (!isDigit(path.charAt(i))) {
        throw syntaxError("expected unsigned integer at position " + i, path);
      }
      while (i < len && isDigit(path.charAt(i))) {
        i++;
      }
      int tagNumber;
      try {
        tagNumber = Integer.parseInt(path.substring(numStart, i));
      } catch (NumberFormatException e) {
        throw syntaxError("tag number out of range at position " + numStart, path);
      }
      if (tagNumber <= 0) {
        throw syntaxError("tag number must be positive at position " + numStart, path);
      }
      int subscript = -1;
      if (i < len && path.charAt(i) == '[') {
        i++;
        int subStart = i;
        if (i >= len || !isDigit(path.charAt(i))) {
          throw syntaxError("expected unsigned integer inside '[...]' at position " + subStart,
            path);
        }
        while (i < len && isDigit(path.charAt(i))) {
          i++;
        }
        if (i >= len || path.charAt(i) != ']') {
          throw syntaxError("expected ']' at position " + i, path);
        }
        try {
          subscript = Integer.parseInt(path.substring(subStart, i));
        } catch (NumberFormatException e) {
          throw syntaxError("subscript out of range at position " + subStart, path);
        }
        i++;
      }
      segments.add(new TagSegment(tagNumber, subscript));
      if (i < len) {
        if (path.charAt(i) != '.') {
          throw syntaxError("expected '.' between segments at position " + i, path);
        }
        i++;
        if (i == len) {
          throw syntaxError("trailing '.' at end of path", path);
        }
      }
    }
    return segments;
  }

  private static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  private static SQLException syntaxError(String detail, String path) {
    return new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_TAG_PATH_INVALID_SYNTAX)
      .setMessage("Invalid tag-path '" + path + "': " + detail).build().buildException();
  }
}
