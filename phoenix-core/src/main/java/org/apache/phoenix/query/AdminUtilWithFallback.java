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

package org.apache.phoenix.query;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;

/**
 * Admin utility class specifically useful for running Admin APIs in the middle of the
 * rolling upgrade from HBase 1.x to 2.x versions. Some Admin APIs fail to retrieve
 * table state details from meta table if master is running on 1.x version and coprocs are
 * running on 2.x version. Hence, as a fallback, server side coproc can directly perform
 * zookeeper look-up to retrieve table state data. The fallback use-cases would not be
 * encountered for fully upgraded 2.x cluster.
 */
public class AdminUtilWithFallback {

  private static final Logger LOG = LoggerFactory.getLogger(AdminUtilWithFallback.class);

  public static boolean tableExists(Admin admin, TableName tableName)
      throws IOException, InterruptedException {
    try {
      return admin.tableExists(tableName);
    } catch (IOException e) {
      if (e instanceof NoSuchColumnFamilyException || (e.getCause() != null
          && e.getCause() instanceof NoSuchColumnFamilyException)) {
        LOG.warn("Admin API to retrieve table existence failed due to missing CF in meta."
            + " This should happen only when HBase master is running on 1.x and"
            + " current regionserver is on 2.x. Falling back to retrieve info from ZK.", e);
        return getTableStateFromZk(tableName, admin) != null;
      }
      throw e;
    }
  }

  private static ZooKeeperProtos.DeprecatedTableState.State getTableState(ZKWatcher zkw,
      TableName tableName) throws KeeperException, InterruptedException {
    String znode =
        ZNodePaths.joinZNode(zkw.getZNodePaths().tableZNode, tableName.getNameAsString());
    byte[] data = ZKUtil.getData(zkw, znode);
    if (data != null && data.length > 0) {
      try {
        ProtobufUtil.expectPBMagicPrefix(data);
        ZooKeeperProtos.DeprecatedTableState.Builder builder =
            ZooKeeperProtos.DeprecatedTableState.newBuilder();
        int magicLen = ProtobufUtil.lengthOfPBMagic();
        ProtobufUtil.mergeFrom(builder, data, magicLen, data.length - magicLen);
        return builder.getState();
      } catch (IOException ioe) {
        KeeperException ke = new KeeperException.DataInconsistencyException();
        ke.initCause(ioe);
        throw ke;
      } catch (DeserializationException de) {
        throw ZKUtil.convert(de);
      }
    } else {
      return null;
    }
  }

  private static TableState.State getTableStateFromZk(TableName tableName, Admin admin)
      throws IOException, InterruptedException {
    try (ZKWatcher zkWatcher = new ZKWatcher(admin.getConfiguration(), "phoenix-admin-fallback",
        null)) {
      ZooKeeperProtos.DeprecatedTableState.State state =
          getTableState(zkWatcher, tableName);
      if (state == null) {
        return null;
      }
      TableState.State tableState;
      switch (state) {
      case ENABLED:
        tableState = TableState.State.ENABLED;
        break;
      case DISABLED:
        tableState = TableState.State.DISABLED;
        break;
      case DISABLING:
        tableState = TableState.State.DISABLING;
        break;
      case ENABLING:
        tableState = TableState.State.ENABLING;
        break;
      default:
        throw new IOException("ZK state inconsistent");
      }
      return tableState;
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

}
