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

package org.apache.phoenix.end2end.chaos.actions;

import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.client.Admin;

public class SplitRandomRegionOfTableAction extends BaseAction {

  private final long sleepTime;

  public SplitRandomRegionOfTableAction() {
      this(-1);
  }

  public SplitRandomRegionOfTableAction(int sleepTime) {
      this.sleepTime = sleepTime;
  }

  @Override
  public void perform() throws Exception {
      // Don't try the split if we're stopping
      if (context.isStopping()) {
          return;
      }

      HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
      Admin admin = util.getHBaseAdmin();
      TableName tableName = getRandomTable(admin);

      LOG.info("Performing action: Split random region of table " + tableName);
      List<HRegionInfo> regions = admin.getTableRegions(tableName);
      if (regions == null || regions.isEmpty()) {
          LOG.info("Table " + tableName + " doesn't have regions to split");
          return;
      }

      HRegionInfo region = PolicyBasedChaosMonkey.selectRandomItem(
          regions.toArray(new HRegionInfo[regions.size()]));
      LOG.debug("Splitting region " + region.getRegionNameAsString());
      try {
          admin.splitRegion(region.getRegionName());
      } catch (Exception ex) {
          LOG.warn("Split failed, might be caused by other chaos: " + ex.getMessage());
      }
      if (sleepTime > 0) {
          Thread.sleep(sleepTime);
      }
  }

}
