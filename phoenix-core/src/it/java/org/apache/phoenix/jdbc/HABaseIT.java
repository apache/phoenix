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

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.replication.ReplicationLogGroup;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class HABaseIT {
  @ClassRule
  public static TemporaryFolder standbyFolder = new TemporaryFolder();
  @ClassRule
  public static TemporaryFolder localFolder = new TemporaryFolder();

  protected static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();

  protected static Configuration conf1;
  protected static Configuration conf2;
  protected static URI standbyUri;
  protected static URI fallbackUri;

  @BeforeClass
  public static synchronized void doBaseSetup() {
    conf1 = CLUSTERS.getHBaseCluster1().getConfiguration();
    conf2 = CLUSTERS.getHBaseCluster2().getConfiguration();
    standbyUri = new Path(standbyFolder.getRoot().toString()).toUri();
    fallbackUri = new Path(localFolder.getRoot().toString()).toUri();
    conf1.setBoolean(SYNCHRONOUS_REPLICATION_ENABLED, true);
    conf2.setBoolean(SYNCHRONOUS_REPLICATION_ENABLED, true);
    conf1.set(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());
    conf1.set(ReplicationLogGroup.REPLICATION_FALLBACK_HDFS_URL_KEY, fallbackUri.toString());
    conf2.set(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());
    conf2.set(ReplicationLogGroup.REPLICATION_FALLBACK_HDFS_URL_KEY, fallbackUri.toString());
  }
}
