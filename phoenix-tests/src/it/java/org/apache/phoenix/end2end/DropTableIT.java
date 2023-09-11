/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DropTableIT extends ParallelStatsDisabledIT {

    @Test
    public void testRepeatedDropTable() throws Exception {
      final String tableName = generateUniqueName();
      final String url = getUrl();
      try (final Connection conn = DriverManager.getConnection(url);
          final Statement stmt = conn.createStatement()) {
        assertFalse(stmt.execute(String.format("CREATE TABLE %s(pk varchar not null primary key)", tableName)));
        String dropTable = String.format("DROP TABLE IF EXISTS %s", tableName);
        for (int i = 0; i < 5; i++) {
          assertFalse(stmt.execute(dropTable));
        }
      }
    }
}
