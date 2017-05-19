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
import org.apache.hadoop.hbase.chaos.actions.ForceBalancerAction;
import org.apache.hadoop.hbase.chaos.actions.RestartActiveMasterAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomRsAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRsHoldingMetaAction;
import org.apache.hadoop.hbase.chaos.factories.MonkeyConstants;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.CompositeSequentialPolicy;
import org.apache.hadoop.hbase.chaos.policies.DoActionsOncePolicy;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

public class ServerKillingMonkeyFactory extends MonkeyFactory {

  private long restartRandomRSSleepTime;
  private long restartActiveMasterSleepTime;
  private long restartRsHoldingMetaSleepTime;

  @Override
  public ChaosMonkey build() {

      loadProperties();

      // Destructive actions to mess things around. Cannot run batch restart
      Action[] actions1 = new Action[] {
          new RestartRandomRsAction(restartRandomRSSleepTime),
          new RestartActiveMasterAction(restartActiveMasterSleepTime),
          new RestartRsHoldingMetaAction(restartRsHoldingMetaSleepTime),
          new ForceBalancerAction()
      };

      // Action to log more info for debugging
      Action[] actions2 = new Action[] {
          new DumpClusterStatusAction()
      };

      return new PolicyBasedChaosMonkey(util,
          new CompositeSequentialPolicy(
              new DoActionsOncePolicy(60 * 1000, actions1),
              new PeriodicRandomActionPolicy(60 * 1000, actions1)),
          new PeriodicRandomActionPolicy(60 * 1000, actions2));
  }

  private void loadProperties() {
      restartRandomRSSleepTime = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.RESTART_RANDOM_RS_SLEEP_TIME,
          MonkeyConstants.DEFAULT_RESTART_RANDOM_RS_SLEEP_TIME + ""));
      restartActiveMasterSleepTime = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.RESTART_ACTIVE_MASTER_SLEEP_TIME,
          MonkeyConstants.DEFAULT_RESTART_ACTIVE_MASTER_SLEEP_TIME + ""));
      restartRsHoldingMetaSleepTime = Long.parseLong(this.properties.getProperty(
          MonkeyConstants.RESTART_RS_HOLDING_META_SLEEP_TIME,
          MonkeyConstants.DEFAULT_RESTART_RS_HOLDING_META_SLEEP_TIME + ""));
  }

}
