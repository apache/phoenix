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
 */
package org.apache.phoenix.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;

import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

public class DynamicIndexGrammarTest {

  private SQLParser parser(String sql) throws Exception {
    return new SQLParser(new StringReader(sql), new ParseNodeFactory());
  }

  @Test
  public void parsesDynamicTokenInIndexColumn() throws Exception {
    CreateIndexStatement stmt = (CreateIndexStatement) parser(
        "CREATE INDEX idx ON t(extra VARCHAR DYNAMIC)").parseStatement();
    IndexKeyConstraint ik = stmt.getIndexConstraint();
    assertEquals(1, ik.getParseNodeAndSortOrderList().size());
    assertTrue(ik.isDynamic(0));
    assertEquals("VARCHAR", ik.getColumnDef(0).getDataType().getSqlTypeName());
  }

  @Test
  public void backwardCompatRegularIndexParses() throws Exception {
    CreateIndexStatement stmt = (CreateIndexStatement) parser(
        "CREATE INDEX idx ON t(regular_col)").parseStatement();
    IndexKeyConstraint ik = stmt.getIndexConstraint();
    assertEquals(1, ik.getParseNodeAndSortOrderList().size());
    assertTrue("non-DYNAMIC entries must report dynamic=false", !ik.isDynamic(0));
  }

  @Test
  public void parsesMixedIndexColumns() throws Exception {
    CreateIndexStatement stmt = (CreateIndexStatement) parser(
        "CREATE INDEX idx ON t(regular_col, extra VARCHAR DYNAMIC)").parseStatement();
    IndexKeyConstraint ik = stmt.getIndexConstraint();
    assertEquals(2, ik.getParseNodeAndSortOrderList().size());
    assertTrue(!ik.isDynamic(0));
    assertTrue(ik.isDynamic(1));
  }

  @Test
  public void parsesDynamicWithSortOrder() throws Exception {
    CreateIndexStatement stmt = (CreateIndexStatement) parser(
        "CREATE INDEX idx ON t(extra BIGINT DYNAMIC DESC)").parseStatement();
    IndexKeyConstraint ik = stmt.getIndexConstraint();
    assertTrue(ik.isDynamic(0));
    assertEquals(SortOrder.DESC, ik.getParseNodeAndSortOrderList().get(0).getSecond());
  }

  @Test(expected = Exception.class)
  public void rejectsDynamicWithoutType() throws Exception {
    // "extra DYNAMIC" without an explicit type — ambiguous, must fail at parse time.
    parser("CREATE INDEX idx ON t(extra DYNAMIC)").parseStatement();
  }

  @Test
  public void dynamicAsRegularIdentifierStillWorks() throws Exception {
    // Soft keyword: DYNAMIC must still be usable as a regular column name elsewhere.
    parser("SELECT dynamic FROM t").parseStatement();
    parser("CREATE TABLE t (dynamic VARCHAR PRIMARY KEY)").parseStatement();
  }
}
