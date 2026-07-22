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
package org.apache.phoenix.replication;

import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_MUTATION_SERIALIZE;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Runs the eventual-index CDC scenario with {@code serializeCDCMutations=true}: the standby
 * regenerates the CDC index row including the serialized downstream-index {@code _IDX_PRE_}/
 * {@code _IDX_POST_} payload (see {@code prepareEventuallyConsistentIndexMutations}). Inherits the
 * single test from {@link ReplicationLogGroupEventualIndexIT} and only flips the cluster-level
 * {@code serializeCDCMutations} config; {@code assertCDCIndexPayloadMatchesConfig} then asserts the
 * payload is present.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogGroupEventualIndexWithSerializeCDCIT
  extends ReplicationLogGroupEventualIndexIT {

  @BeforeClass
  public static void doSetup() throws Exception {
    // serializeCDCMutations is read once at IRO start(), so set it before the clusters come up;
    // setupEventualIndexClusters() handles the consumer-disable and cluster start.
    conf1.setBoolean(PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, true);
    conf2.setBoolean(PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, true);
    setupEventualIndexClusters();
  }
}
