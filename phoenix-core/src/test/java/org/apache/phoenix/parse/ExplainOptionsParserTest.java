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
package org.apache.phoenix.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import org.apache.phoenix.parse.ExplainOptions.Format;
import org.junit.Test;

/**
 * Parser-level tests for the {@code EXPLAIN [(<opt> [, <opt>]*)] <stmt>} option-list grammar and
 * the legacy {@code EXPLAIN WITH REGIONS} alias.
 */
public class ExplainOptionsParserTest {

  private static final String STMT = " SELECT * FROM atable";

  private ExplainOptions parseOptions(String sql) throws SQLException, IOException {
    SQLParser parser = new SQLParser(new StringReader(sql));
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected an ExplainStatement, got " + stmt.getClass(),
      stmt instanceof ExplainStatement);
    return ((ExplainStatement) stmt).getOptions();
  }

  private void assertParseError(String sql) {
    try {
      new SQLParser(new StringReader(sql)).parseStatement();
      fail("Expected a parse error for: " + sql);
    } catch (SQLException | IOException e) {
      // expected
    }
  }

  @Test
  public void testPlainExplain() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN" + STMT);
    assertFalse(opts.isRegions());
    assertFalse(opts.isVerbose());
    assertEquals(Format.TEXT, opts.getFormat());
  }

  @Test
  public void testRegionsOption() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN (REGIONS)" + STMT);
    assertTrue(opts.isRegions());
    assertFalse(opts.isVerbose());
    assertEquals(Format.TEXT, opts.getFormat());
  }

  @Test
  public void testVerboseOption() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN (VERBOSE)" + STMT);
    assertFalse(opts.isRegions());
    assertTrue(opts.isVerbose());
    assertEquals(Format.TEXT, opts.getFormat());
  }

  @Test
  public void testFormatText() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN (FORMAT TEXT)" + STMT);
    assertFalse(opts.isRegions());
    assertEquals(Format.TEXT, opts.getFormat());
  }

  @Test
  public void testFormatJson() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN (FORMAT JSON)" + STMT);
    assertEquals(Format.JSON, opts.getFormat());
  }

  @Test
  public void testMultipleOptions() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN (REGIONS, FORMAT JSON)" + STMT);
    assertTrue(opts.isRegions());
    assertEquals(Format.JSON, opts.getFormat());
  }

  @Test
  public void testMultipleOptionsOrderIndependent() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN (FORMAT JSON, REGIONS)" + STMT);
    assertTrue(opts.isRegions());
    assertEquals(Format.JSON, opts.getFormat());
  }

  @Test
  public void testAllThreeOptions() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN (REGIONS, VERBOSE, FORMAT JSON)" + STMT);
    assertTrue(opts.isRegions());
    assertTrue(opts.isVerbose());
    assertEquals(Format.JSON, opts.getFormat());
  }

  @Test
  public void testLegacyWithRegionsAlias() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN WITH REGIONS" + STMT);
    assertTrue(opts.isRegions());
    assertFalse(opts.isVerbose());
    assertEquals(Format.TEXT, opts.getFormat());
  }

  @Test
  public void testCaseInsensitiveOptions() throws Exception {
    ExplainOptions opts = parseOptions("EXPLAIN (regions, format json)" + STMT);
    assertTrue(opts.isRegions());
    assertEquals(Format.JSON, opts.getFormat());
  }

  @Test
  public void testUnknownOption() {
    assertParseError("EXPLAIN (BOGUS)" + STMT);
  }

  @Test
  public void testDuplicateOption() {
    assertParseError("EXPLAIN (REGIONS, REGIONS)" + STMT);
  }

  @Test
  public void testFormatWithoutValue() {
    assertParseError("EXPLAIN (FORMAT)" + STMT);
  }

  @Test
  public void testUnknownFormatKind() {
    assertParseError("EXPLAIN (FORMAT XML)" + STMT);
  }

  @Test
  public void testRegionsWithValue() {
    assertParseError("EXPLAIN (REGIONS VALUE)" + STMT);
  }

  @Test
  public void testMixingLegacyAndOptionList() {
    assertParseError("EXPLAIN WITH REGIONS (REGIONS)" + STMT);
  }
}
