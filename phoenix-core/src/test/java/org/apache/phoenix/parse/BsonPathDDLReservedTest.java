/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Unit tests asserting that the {@code USING PATH} clause on {@code CREATE INDEX}
 * is reserved for a future release (Phase 4 of BSON-path functional indexes).
 */
public class BsonPathDDLReservedTest {

  @Test
  public void usingPathIsReserved() {
    String sql = "CREATE INDEX idx ON mytable USING PATH (BSON_VALUE(doc, '$.a', 'VARCHAR'))";
    try {
      new SQLParser(sql).parseStatement();
      fail("expected reserved-keyword error for USING PATH");
    } catch (Exception e) {
      // Either the parser surfaces the wrapped SQLException directly, or the runtime exception
      // contains the marker message — accept both.
      String msg = String.valueOf(e.getMessage()) + " "
          + (e.getCause() == null ? "" : String.valueOf(e.getCause().getMessage()));
      assertTrue("error must mention reserved/USING PATH; got: " + msg,
          msg.toLowerCase().contains("path") || msg.toLowerCase().contains("reserved"));
    }
  }

  @Test
  public void plainCreateIndexStillWorks() throws Exception {
    String sql = "CREATE INDEX idx ON mytable (col1)";
    new SQLParser(sql).parseStatement();
  }

  @Test
  public void pathRemainsUsableAsIdentifier() throws Exception {
    // 'path' must remain a soft keyword: still legal as a column identifier.
    new SQLParser("CREATE TABLE t (k VARCHAR PRIMARY KEY, path VARCHAR)").parseStatement();
    new SQLParser("CREATE INDEX idx ON t (path)").parseStatement();
  }
}
