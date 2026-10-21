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
package org.apache.hadoop.hbase.ipc.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

public class ServerRpcControllerFactoryTest {

  /**
   * Verifies that server-to-server calls on SYSTEM.CATALOG and SYSTEM.VECTOR_CENTROID get the
   * server-side priority, with and without namespace mapping. Index maintenance reads
   * SYSTEM.VECTOR_CENTROID from RPC handlers on a centroid cache miss. Calls on a user table must
   * not get this priority.
   */
  @Test
  public void testServerSidePriorityForSystemTables() {
    Configuration conf = HBaseConfiguration.create();
    int serverSidePriority = IndexUtil.getServerSidePriority(conf);
    ServerRpcControllerFactory factory = new ServerRpcControllerFactory(conf);
    for (byte[] name : new byte[][] { PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
      PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME_BYTES }) {
      for (boolean namespaceMapped : new boolean[] { false, true }) {
        TableName table = SchemaUtil.getPhysicalTableName(name, namespaceMapped);
        HBaseRpcController controller = factory.newController();
        controller.setPriority(table);
        assertEquals(table.getNameAsString(), serverSidePriority, controller.getPriority());
      }
    }
    HBaseRpcController controller = factory.newController();
    controller.setPriority(TableName.valueOf("T"));
    assertNotEquals(serverSidePriority, controller.getPriority());
  }
}
