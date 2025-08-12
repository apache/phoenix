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
package org.apache.phoenix.end2end;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import org.junit.experimental.categories.Category;

/**
 * Tests for TOTAL_SEGMENTS() with VARBINARY_ENCODED primary key
 */
@Category(ParallelStatsDisabledTest.class)
public class TotalSegmentsFunction2IT extends BaseTotalSegmentsFunctionIT {

  @Override
  protected String getPrimaryKeyColumnType() {
    return "VARBINARY_ENCODED";
  }

  @Override
  protected String getCompositeKeyColumnType() {
    return "VARBINARY_ENCODED";
  }

  @Override
  protected String extractPrimaryKeyValue(ResultSet rs) throws SQLException {
    return Base64.getEncoder().encodeToString(rs.getBytes(1));
  }
}
