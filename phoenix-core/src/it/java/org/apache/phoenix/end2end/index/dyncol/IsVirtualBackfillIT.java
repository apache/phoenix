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
package org.apache.phoenix.end2end.index.dyncol;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class IsVirtualBackfillIT extends ParallelStatsDisabledIT {

  @Test
  public void systemCatalogHasIsVirtualColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      DatabaseMetaData md = conn.getMetaData();
      ResultSet rs = md.getColumns(null, "SYSTEM", "CATALOG", "IS_VIRTUAL");
      assertTrue("IS_VIRTUAL column should exist on SYSTEM.CATALOG", rs.next());
      assertEquals("IS_VIRTUAL column should be BOOLEAN", Types.BOOLEAN, rs.getInt("DATA_TYPE"));
    }
  }
}
