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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.query.QueryServices.SYNCHRONOUS_REPLICATION_ENABLED;
import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.PHOENIX_REPLICATION_REPLAY_ENABLED;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.junit.BeforeClass;

public class HABaseIT {
  protected static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();

  protected static Configuration conf1;
  protected static Configuration conf2;

  @BeforeClass
  public static synchronized void doBaseSetup() {
    conf1 = CLUSTERS.getHBaseCluster1().getConfiguration();
    conf2 = CLUSTERS.getHBaseCluster2().getConfiguration();
    conf1.setBoolean(SYNCHRONOUS_REPLICATION_ENABLED, true);
    conf2.setBoolean(SYNCHRONOUS_REPLICATION_ENABLED, true);
    // Enable replication replay service
    conf1.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, true);
    conf2.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, true);
  }
}
