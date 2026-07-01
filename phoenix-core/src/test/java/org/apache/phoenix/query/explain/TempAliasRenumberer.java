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
package org.apache.phoenix.query.explain;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Renumbers Phoenix temp-alias tokens of the form {@code $<digits>}.
 * <p>
 * Temp aliases are produced by {@code ParseNodeFactory.createTempAlias()} from a JVM-global
 * {@code AtomicInteger}, so the literal numbers in an EXPLAIN line depend on what was compiled
 * earlier in the same JVM.
 * <p>
 * The first distinct alias encountered becomes {@code $1}, the second {@code $2}, and so on, so
 * that structural relationships are preserved within one plan.
 */
final class TempAliasRenumberer {

  private static final Pattern TEMP_ALIAS = Pattern.compile("\\$\\d+");

  private final Map<String, String> mapping = new HashMap<>();
  private int next = 0;

  /**
   * @param s arbitrary text
   * @return {@code s} with each {@code $<digits>} token replaced by its renumbered alias, or
   *         {@code s} unchanged when no token appears. {@code null} input returns {@code null}.
   */
  String rewrite(String s) {
    if (s == null) {
      return null;
    }
    Matcher m = TEMP_ALIAS.matcher(s);
    if (!m.find()) {
      return s;
    }
    m.reset();
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String tok = m.group();
      String repl = mapping.get(tok);
      if (repl == null) {
        next++;
        repl = "$" + next;
        mapping.put(tok, repl);
      }
      m.appendReplacement(sb, Matcher.quoteReplacement(repl));
    }
    m.appendTail(sb);
    return sb.toString();
  }
}
