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
package org.apache.phoenix.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZKBasedMasterElectionUtil {

    private static final Log LOG = LogFactory.getLog(ZKBasedMasterElectionUtil.class);

    public static boolean acquireLock(ZooKeeperWatcher zooKeeperWatcher, String parentNode,
            String lockName) throws KeeperException, InterruptedException {
        // Create the parent node as Persistent
        LOG.info("Creating the parent lock node:" + parentNode);
        ZKUtil.createWithParents(zooKeeperWatcher, parentNode);

        // Create the ephemeral node
        String lockNode = parentNode + "/" + lockName;
        String nodeValue = getHostName() + "_" + UUID.randomUUID().toString();
        LOG.info("Trying to acquire the lock by creating node:" + lockNode + " value:" + nodeValue);
        // Create the ephemeral node
        try {
            zooKeeperWatcher.getRecoverableZooKeeper().create(lockNode, Bytes.toBytes(nodeValue),
                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("Could not acquire lock. Another process had already acquired the lock on Node "
                    + lockName);
            return false;
        }
        LOG.info("Obtained the lock :" + lockNode);
        return true;
    }

    private static String getHostName() {
        String host = "";
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOG.error("UnknownHostException while trying to get the Local Host address : ", e);
        }
        return host;
    }

}
