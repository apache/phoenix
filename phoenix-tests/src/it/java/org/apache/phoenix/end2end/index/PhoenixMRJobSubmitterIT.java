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
package org.apache.phoenix.end2end.index;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.mapreduce.index.automation.PhoenixAsyncIndex;
import org.apache.phoenix.mapreduce.index.automation.PhoenixMRJobSubmitter;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.RunUntilFailure;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixMRJobSubmitterIT extends BaseTest {

  private static String REQUEST_INDEX_REBUILD_SQL = "ALTER INDEX %s ON %s REBUILD ASYNC";

  @BeforeClass
  public static synchronized void setUp() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);

  }

  @Test
  public void testGetCandidateJobs() throws Exception {
    String tableName = "TBL_" + generateUniqueName();
    String asyncIndexName = "IDX_" + generateUniqueName();
    String needsRebuildIndexName = "IDX_" + generateUniqueName();
    String tableDDL = "CREATE TABLE " + tableName + TestUtil.TEST_TABLE_SCHEMA;
    String asyncIndexDDL = "CREATE INDEX " + asyncIndexName + " ON " + tableName + " (a.varchar_col1) ASYNC";
    String needsRebuildIndexDDL = "CREATE INDEX " + needsRebuildIndexName + " ON " + tableName + " (a.char_col1)";
    long rebuildTimestamp = 100L;

    createTestTable(getUrl(), tableDDL);

    createTestTable(getUrl(), needsRebuildIndexDDL);
    Connection conn = null;
    PreparedStatement stmt = null;
    try {
      conn = DriverManager.getConnection(getUrl());
      TestUtil.assertIndexState(conn, needsRebuildIndexName, PIndexState.ACTIVE, 0L);

      //first make sure that we don't return an active index
      PhoenixMRJobSubmitter submitter = new PhoenixMRJobSubmitter(getUtility().getConfiguration());
      Map<String, PhoenixAsyncIndex> candidateMap = submitter.getCandidateJobs(conn);
      Assert.assertNotNull(candidateMap);
      Assert.assertEquals(0, candidateMap.size());

      //create an index with ASYNC that will need building via MapReduce
      createTestTable(getUrl(), asyncIndexDDL);
      TestUtil.assertIndexState(conn, asyncIndexName, PIndexState.BUILDING, 0L);

      //now force a rebuild on the needsRebuildIndex
      stmt = conn.prepareStatement(String.format(REQUEST_INDEX_REBUILD_SQL, needsRebuildIndexName, tableName));
      stmt.execute();
      conn.commit();
      TestUtil.assertIndexState(conn, asyncIndexName, PIndexState.BUILDING, 0L);

      //regenerate the candidateMap. We should get both indexes back this time.
      candidateMap = submitter.getCandidateJobs(conn);
      Assert.assertNotNull(candidateMap);
      Assert.assertEquals(2, candidateMap.size());
      boolean foundAsyncIndex = false;
      boolean foundNeedsRebuildIndex = false;
      for (PhoenixAsyncIndex indexInfo : candidateMap.values()){
        if (indexInfo.getTableName().equals(asyncIndexName)){
          foundAsyncIndex = true;
        } else if (indexInfo.getTableName().equals(needsRebuildIndexName)){
          foundNeedsRebuildIndex = true;
        }
      }
      Assert.assertTrue("Did not return index in BUILDING created with ASYNC!", foundAsyncIndex);
      Assert.assertTrue("Did not return index in REBUILD with an ASYNC_REBUILD_TIMESTAMP!", foundNeedsRebuildIndex);
    } catch(Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    }

  }
}
