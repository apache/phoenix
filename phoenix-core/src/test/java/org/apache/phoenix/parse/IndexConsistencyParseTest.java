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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.phoenix.schema.types.IndexConsistency;
import org.junit.Test;

/**
 * Test class for parsing CREATE INDEX statements with CONSISTENCY clause.
 */
public class IndexConsistencyParseTest {

  @Test
  public void testCreateIndexWithStrongConsistency() throws Exception {
    String sql = "CREATE INDEX idx ON mytable (col1) CONSISTENCY=STRONG";
    SQLParser parser = new SQLParser(sql);
    CreateIndexStatement stmt = (CreateIndexStatement) parser.parseStatement();

    assertNotNull("Statement should not be null", stmt);
    assertEquals("Should have STRONG consistency", IndexConsistency.STRONG,
      stmt.getIndexConsistency());
  }

  @Test
  public void testCreateIndexWithEventualConsistency() throws Exception {
    String sql = "CREATE INDEX idx ON mytable (col1) CONSISTENCY=EVENTUAL";
    SQLParser parser = new SQLParser(sql);
    CreateIndexStatement stmt = (CreateIndexStatement) parser.parseStatement();

    assertNotNull("Statement should not be null", stmt);
    assertEquals("Should have EVENTUAL consistency", IndexConsistency.EVENTUAL,
      stmt.getIndexConsistency());
  }

  @Test
  public void testCreateIndexWithoutConsistency() throws Exception {
    String sql = "CREATE INDEX idx ON mytable (col1)";
    SQLParser parser = new SQLParser(sql);
    CreateIndexStatement stmt = (CreateIndexStatement) parser.parseStatement();

    assertNotNull("Statement should not be null", stmt);
    assertNull("Should default to STRONG consistency", stmt.getIndexConsistency());
  }

  @Test
  public void testCreateIndexWithConsistencyAndOtherClauses() throws Exception {
    String sql = "CREATE INDEX idx ON mytable (col1) INCLUDE (col2) ASYNC CONSISTENCY=EVENTUAL";
    SQLParser parser = new SQLParser(sql);
    CreateIndexStatement stmt = (CreateIndexStatement) parser.parseStatement();

    assertNotNull("Statement should not be null", stmt);
    assertEquals("Should have EVENTUAL consistency", IndexConsistency.EVENTUAL,
      stmt.getIndexConsistency());
    assertEquals("Should have include columns", 1, stmt.getIncludeColumns().size());
    assertEquals("Should be async", true, stmt.isAsync());
  }

  @Test
  public void testLocalIndexWithConsistency() throws Exception {
    String sql = "CREATE LOCAL INDEX idx ON mytable (col1) CONSISTENCY=EVENTUAL";
    SQLParser parser = new SQLParser(sql);
    CreateIndexStatement stmt = (CreateIndexStatement) parser.parseStatement();

    assertNotNull("Statement should not be null", stmt);
    assertEquals("Should have EVENTUAL consistency", IndexConsistency.EVENTUAL,
      stmt.getIndexConsistency());
  }
}
