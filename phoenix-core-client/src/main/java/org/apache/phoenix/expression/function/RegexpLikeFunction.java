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
package org.apache.phoenix.expression.function;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.util.regex.AbstractBasePattern;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.RegexpLikeParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * Function similar to Oracle's REGEXP_LIKE, which tests whether a string matches a regular
 * expression pattern. Usage: {@code REGEXP_LIKE(<source_char>, <pattern> [, <match_parameter>]) }
 * <p>
 * source_char is the string to search. pattern is a Java compatible regular expression string.
 * match_parameter is an optional string of flags that modify matching behavior:
 * <ul>
 * <li>'i' - case-insensitive matching</li>
 * <li>'c' - case-sensitive matching (default)</li>
 * <li>'m' - multiline mode (^ and $ match line boundaries). Note: with full-match semantics, 'm'
 * has limited use on its own; it is most useful combined with 's'. May become independently useful
 * if partial-match semantics are adopted in the future.</li>
 * <li>'s' - dotall mode (. matches any character including newline)</li>
 * </ul>
 * The function returns a {@link org.apache.phoenix.schema.types.PBoolean}.
 * @since 5.3
 */
@BuiltInFunction(name = RegexpLikeFunction.NAME, nodeClass = RegexpLikeParseNode.class,
    args = { @Argument(allowedTypes = { PVarchar.class }),
      @Argument(allowedTypes = { PVarchar.class }),
      @Argument(allowedTypes = { PVarchar.class }, defaultValue = "null") },
    classType = FunctionParseNode.FunctionClassType.ABSTRACT,
    derivedFunctions = { ByteBasedRegexpLikeFunction.class, StringBasedRegexpLikeFunction.class })
public abstract class RegexpLikeFunction extends ScalarFunction {
  public static final String NAME = "REGEXP_LIKE";

  private static final PVarchar TYPE = PVarchar.INSTANCE;
  private AbstractBasePattern pattern;

  public RegexpLikeFunction() {
  }

  public RegexpLikeFunction(List<Expression> children) {
    super(children);
    init();
  }

  protected abstract AbstractBasePattern compilePatternSpec(String value, int flags);

  /**
   * Parse the match_parameter string into regex flags. Subclasses translate these into
   * implementation-specific flag values.
   * @param matchParameter the match parameter string (e.g. "im", "cs")
   * @return a bitmask of standard Java regex flags
   */
  static int parseMatchParameter(String matchParameter) {
    int flags = 0;
    if (matchParameter == null || matchParameter.isEmpty()) {
      return flags;
    }
    for (int i = 0; i < matchParameter.length(); i++) {
      char c = matchParameter.charAt(i);
      switch (c) {
        case 'i':
          // Enable case-insensitive matching. If 'c' appears later, it will override this.
          flags |= java.util.regex.Pattern.CASE_INSENSITIVE;
          break;
        case 'c':
          // Enable case-sensitive matching (default). Overrides a preceding 'i'.
          // Per Oracle semantics, if both 'i' and 'c' are specified, the last one wins.
          flags &= ~java.util.regex.Pattern.CASE_INSENSITIVE;
          break;
        case 'm':
          // Enable multiline mode: ^ and $ match at line boundaries, not just
          // the start and end of the entire string.
          // NOTE: With the current full-match semantics, 'm' has limited practical use
          // on its own because the entire string must match the pattern regardless.
          // It becomes more meaningful when combined with 's' (dotall) and .* wrappers,
          // e.g., REGEXP_LIKE(val, '.*^ERROR.*$.*', 'ms').
          // If REGEXP_LIKE is changed to partial-match semantics in the future, 'm' will
          // become independently useful (e.g., REGEXP_LIKE(val, '^ERROR', 'm') would
          // find 'ERROR' at the start of any line).
          // Users can also use (?m) inline in the pattern as an alternative.
          flags |= java.util.regex.Pattern.MULTILINE;
          break;
        case 's':
          // Enable dotall mode: the '.' metacharacter matches any character
          // including newline characters.
          flags |= java.util.regex.Pattern.DOTALL;
          break;
        default:
          throw new IllegalArgumentException("Invalid match_parameter character '" + c
            + "' in REGEXP_LIKE. Valid values are 'i', 'c', 'm', 's'.");
      }
    }
    return flags;
  }

  private void init() {
    ImmutableBytesWritable tmpPtr = new ImmutableBytesWritable();
    Expression patternExpr = getPatternExpression();
    if (
      patternExpr.isStateless() && patternExpr.getDeterminism() == Determinism.ALWAYS
        && patternExpr.evaluate(null, tmpPtr)
    ) {
      String patternStr =
        (String) TYPE.toObject(tmpPtr, patternExpr.getDataType(), patternExpr.getSortOrder());
      if (patternStr != null) {
        int flags = resolveFlags(tmpPtr);
        pattern = compilePatternSpec(patternStr, flags);
      }
    }
  }

  /**
   * Resolve the regex flags from the optional match_parameter argument.
   */
  private int resolveFlags(ImmutableBytesWritable tmpPtr) {
    if (children.size() <= 2) {
      return 0;
    }
    Expression matchParamExpr = getMatchParameterExpression();
    if (
      matchParamExpr.isStateless() && matchParamExpr.getDeterminism() == Determinism.ALWAYS
        && matchParamExpr.evaluate(null, tmpPtr)
    ) {
      String matchParam =
        (String) TYPE.toObject(tmpPtr, matchParamExpr.getDataType(), matchParamExpr.getSortOrder());
      return parseMatchParameter(matchParam);
    }
    return 0;
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    AbstractBasePattern pattern = this.pattern;
    if (pattern == null) {
      Expression patternExpr = getPatternExpression();
      if (!patternExpr.evaluate(tuple, ptr)) {
        return false;
      }
      if (ptr.getLength() == 0) {
        return true;
      }
      String patternStr =
        (String) TYPE.toObject(ptr, patternExpr.getDataType(), patternExpr.getSortOrder());
      if (patternStr == null) {
        return false;
      }
      // Resolve flags at evaluation time if not pre-compiled
      int flags = 0;
      if (children.size() > 2) {
        Expression matchParamExpr = getMatchParameterExpression();
        if (!matchParamExpr.evaluate(tuple, ptr)) {
          return false;
        }
        String matchParam =
          (String) TYPE.toObject(ptr, matchParamExpr.getDataType(), matchParamExpr.getSortOrder());
        flags = parseMatchParameter(matchParam);
      }
      pattern = compilePatternSpec(patternStr, flags);
    }

    Expression sourceExpr = getSourceExpression();
    if (!sourceExpr.evaluate(tuple, ptr)) {
      return false;
    }
    if (ptr.getLength() == 0) {
      return true;
    }
    TYPE.coerceBytes(ptr, TYPE, sourceExpr.getSortOrder(), SortOrder.ASC);

    pattern.matches(ptr);
    return true;
  }

  private Expression getSourceExpression() {
    return children.get(0);
  }

  private Expression getPatternExpression() {
    return children.get(1);
  }

  private Expression getMatchParameterExpression() {
    return children.get(2);
  }

  @Override
  public PDataType getDataType() {
    return PBoolean.INSTANCE;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    init();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
