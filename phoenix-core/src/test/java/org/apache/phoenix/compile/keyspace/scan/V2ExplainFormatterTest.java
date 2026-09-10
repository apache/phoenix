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
package org.apache.phoenix.compile.keyspace.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

/**
 * Focused unit test for {@link V2ExplainFormatter}. Validates that key-range-display-
 * relevant queries render inclusive upper bounds as {@code [*, N]} (matching V1) rather
 * than the byte-bumped {@code [*, N+1)} form that V2's compound emission leaves on the
 * {@code KeyRange}.
 * <p>
 * Covers the shape behind {@code AggregateIT.testGroupByOrderPreserving}:
 * several pinned leading PK columns plus a trailing inclusive-upper range on a
 * fixed-width PK column. Runs connectionless under the default V2 configuration.
 */
public class V2ExplainFormatterTest extends BaseConnectionlessQueryTest {

  /**
   * Regression for AggregateIT.testGroupByOrderPreserving: pinned prefix + trailing
   * inclusive-upper range on a fixed-width PK column. Without {@link V2ExplainFormatter}
   * V2's compound byte emission renders the upper as {@code [..., 0, 2]} because the
   * compound byte sequence is {@code nextKey(1) = 2}; the test expects V1's
   * {@code [..., 0, 1]}. The formatter reads the KeySpaceList's pre-encoding upper
   * ({@code 1 inclusive}) so the display matches V1 regardless of compound emission.
   */
  @Test
  public void inclusiveUpperRendersUnbumped() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String tableName = "T_EXPL";
      conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + tableName
        + "(ORGANIZATION_ID char(15) NOT NULL, "
        + "JOURNEY_ID char(15) NOT NULL, "
        + "DATASOURCE SMALLINT NOT NULL, "
        + "MATCH_STATUS TINYINT NOT NULL, "
        + "EXTERNAL_DATASOURCE_KEY VARCHAR(30), "
        + "ENTITY_ID CHAR(15) NOT NULL, "
        + "CONSTRAINT pk PRIMARY KEY ("
        + "  ORGANIZATION_ID, JOURNEY_ID, DATASOURCE, MATCH_STATUS, "
        + "  EXTERNAL_DATASOURCE_KEY, ENTITY_ID))");

      String sql = "SELECT EXTERNAL_DATASOURCE_KEY FROM " + tableName
        + " WHERE ORGANIZATION_ID = '000001111122222'"
        + " AND JOURNEY_ID = '333334444455555'"
        + " AND DATASOURCE = 0"
        + " AND MATCH_STATUS <= 1";

      ExplainPlan plan = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class)
        .optimizeQuery().getExplainPlan();
      ExplainPlanAttributes attrs = plan.getPlanStepsAsAttributes();
      String keyRanges = attrs.getKeyRanges();
      assertNotNull(keyRanges);
      // V1 (and now V2 via V2ExplainFormatter) renders the inclusive upper as `1`, not
      // the post-nextKey-bump `2`.
      assertEquals(
        " ['000001111122222','333334444455555',0,*] - ['000001111122222','333334444455555',0,1]",
        keyRanges);
    }
  }
}
