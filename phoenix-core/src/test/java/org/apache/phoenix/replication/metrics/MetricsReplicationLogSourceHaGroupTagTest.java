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
package org.apache.phoenix.replication.metrics;

import static org.apache.phoenix.metrics.MetricConstants.HA_GROUP_TAG_NAME;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.junit.Test;

/**
 * Verifies that every per-HA-group replication metrics source registers the {@code haGroup}
 * Metrics2 tag carrying the raw (unquoted, case-preserved) HA group name, matching the existing
 * HAGroupStore metrics source. This is the tag the JMX-to-Argus collector promotes so replication
 * metrics can be sliced per HA group.
 */
public class MetricsReplicationLogSourceHaGroupTagTest {

  private static void assertHaGroupTagged(BaseSourceImpl source, String haGroupName) {
    assertEquals(haGroupName, source.getMetricsRegistry().getTag(HA_GROUP_TAG_NAME).value());
  }

  @Test
  public void testLogGroupSourceTagsHaGroup() {
    String haGroupName = "testHaGroup-" + System.nanoTime();
    MetricsReplicationLogGroupSourceImpl source =
      new MetricsReplicationLogGroupSourceImpl(haGroupName);
    try {
      assertHaGroupTagged(source, haGroupName);
    } finally {
      source.close();
    }
  }

  @Test
  public void testLogProcessorSourceTagsHaGroup() {
    String haGroupName = "testHaGroup-" + System.nanoTime();
    MetricsReplicationLogProcessorImpl source = new MetricsReplicationLogProcessorImpl(haGroupName);
    try {
      assertHaGroupTagged(source, haGroupName);
    } finally {
      source.close();
    }
  }

  @Test
  public void testDiscoveryReplaySourceTagsHaGroup() {
    String haGroupName = "testHaGroup-" + System.nanoTime();
    MetricsReplicationLogDiscoveryReplayImpl source =
      new MetricsReplicationLogDiscoveryReplayImpl(haGroupName);
    try {
      assertHaGroupTagged(source, haGroupName);
    } finally {
      source.close();
    }
  }

  @Test
  public void testDiscoveryForwarderSourceTagsHaGroup() {
    String haGroupName = "testHaGroup-" + System.nanoTime();
    MetricsReplicationLogDiscoveryForwarderImpl source =
      new MetricsReplicationLogDiscoveryForwarderImpl(haGroupName);
    try {
      assertHaGroupTagged(source, haGroupName);
    } finally {
      source.close();
    }
  }

  @Test
  public void testTrackerReplaySourceTagsHaGroup() {
    String haGroupName = "testHaGroup-" + System.nanoTime();
    MetricsReplicationLogTrackerReplayImpl source =
      new MetricsReplicationLogTrackerReplayImpl(haGroupName);
    try {
      assertHaGroupTagged(source, haGroupName);
    } finally {
      source.close();
    }
  }

  @Test
  public void testTrackerForwarderSourceTagsHaGroup() {
    String haGroupName = "testHaGroup-" + System.nanoTime();
    MetricsReplicationLogTrackerForwarderImpl source =
      new MetricsReplicationLogTrackerForwarderImpl(haGroupName);
    try {
      assertHaGroupTagged(source, haGroupName);
    } finally {
      source.close();
    }
  }
}
