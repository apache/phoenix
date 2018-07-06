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

package org.apache.phoenix.end2end.chaos.factories;

import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.factories.MonkeyConstants;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.CompositeSequentialPolicy;
import org.apache.hadoop.hbase.chaos.policies.DoActionsOncePolicy;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;
import org.apache.phoenix.end2end.chaos.actions.CompactRandomRegionOfTableAction;
import org.apache.phoenix.end2end.chaos.actions.CompactTableAction;
import org.apache.phoenix.end2end.chaos.actions.FlushRandomRegionOfTableAction;
import org.apache.phoenix.end2end.chaos.actions.FlushTableAction;
import org.apache.phoenix.end2end.chaos.actions.MergeRandomAdjacentRegionsOfTableAction;
import org.apache.phoenix.end2end.chaos.actions.MoveRandomRegionOfTableAction;
import org.apache.phoenix.end2end.chaos.actions.MoveRegionsOfTableAction;
import org.apache.phoenix.end2end.chaos.actions.SnapshotTableAction;
import org.apache.phoenix.end2end.chaos.actions.SplitAllRegionOfTableAction;
import org.apache.phoenix.end2end.chaos.actions.SplitRandomRegionOfTableAction;

public class NoKillMonkeyFactory extends MonkeyFactory {

  private long action1Period;
  private long action2Period;
  private long action3Period;
  private long action4Period;
  private long moveRegionsMaxTime;
  private long moveRegionsSleepTime;
  private long moveRandomRegionSleepTime;
  private float compactTableRatio;
  private float compactRandomRegionRatio;

  @Override
  public ChaosMonkey build() {

    loadProperties();

    // Actions such as compact/flush a table/region,
    // move one region around. They are not so destructive,
    // can be executed more frequently.
    Action[] actions1 = new Action[] {
        new CompactTableAction(compactTableRatio),
        new CompactRandomRegionOfTableAction(compactRandomRegionRatio),
        new FlushTableAction(),
        new FlushRandomRegionOfTableAction(),
        new MoveRandomRegionOfTableAction()
    };

    // Actions such as split/merge/snapshot.
    Action[] actions2 = new Action[] {
        new SplitRandomRegionOfTableAction(),
        new MergeRandomAdjacentRegionsOfTableAction(),
        new SnapshotTableAction(),
    };

    // Destructive actions to mess things around.
    Action[] actions3 = new Action[] {
        new MoveRegionsOfTableAction(moveRegionsSleepTime, moveRegionsMaxTime),
        new MoveRandomRegionOfTableAction(moveRandomRegionSleepTime),
        new SplitAllRegionOfTableAction(),
    };

    // Action to log more info for debugging
    Action[] actions4 = new Action[] {
        new DumpClusterStatusAction()
    };

    return new PolicyBasedChaosMonkey(util,
        new PeriodicRandomActionPolicy(action1Period, actions1),
        new PeriodicRandomActionPolicy(action2Period, actions2),
        new CompositeSequentialPolicy(
            new DoActionsOncePolicy(action3Period, actions3),
            new PeriodicRandomActionPolicy(action3Period, actions3)),
        new PeriodicRandomActionPolicy(action4Period, actions4));
  }

  private void loadProperties() {
      action1Period = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.PERIODIC_ACTION1_PERIOD,
          MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD + ""));
      action2Period = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.PERIODIC_ACTION2_PERIOD,
          MonkeyConstants.DEFAULT_PERIODIC_ACTION2_PERIOD + ""));
      action3Period = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.COMPOSITE_ACTION3_PERIOD,
          MonkeyConstants.DEFAULT_COMPOSITE_ACTION3_PERIOD + ""));
      action4Period = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.PERIODIC_ACTION4_PERIOD,
          MonkeyConstants.DEFAULT_PERIODIC_ACTION4_PERIOD + ""));
      moveRegionsMaxTime = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.MOVE_REGIONS_MAX_TIME,
          MonkeyConstants.DEFAULT_MOVE_REGIONS_MAX_TIME + ""));
      moveRegionsSleepTime = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.MOVE_REGIONS_SLEEP_TIME,
          MonkeyConstants.DEFAULT_MOVE_REGIONS_SLEEP_TIME + ""));
      moveRandomRegionSleepTime = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.MOVE_RANDOM_REGION_SLEEP_TIME,
          MonkeyConstants.DEFAULT_MOVE_RANDOM_REGION_SLEEP_TIME + ""));
      compactTableRatio = Float.parseFloat(this.properties.getProperty(
          MonkeyConstants.COMPACT_TABLE_ACTION_RATIO,
          MonkeyConstants.DEFAULT_COMPACT_TABLE_ACTION_RATIO + ""));
      compactRandomRegionRatio = Float.parseFloat(this.properties.getProperty(
          MonkeyConstants.COMPACT_RANDOM_REGION_RATIO,
          MonkeyConstants.DEFAULT_COMPACT_RANDOM_REGION_RATIO + ""));
  }

}
