/**
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
package org.apache.phoenix.hbase.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.phoenix.hbase.index.ipc.PhoenixIndexRpcSchedulerFactory;

/**
 * Helper class to avoid loading HBase 0.98.3+ classes in older HBase installations
 */
public class IndexQosCompat {

    private static final Log LOG = LogFactory.getLog(IndexQosCompat.class);

    /**
     * Full class name of the RpcControllerFactory. This is copied here so we don't need the static reference, so we can work with older versions of HBase 0.98, which don't have this class
     */
    private static final String HBASE_RPC_CONTROLLER_CLASS_NAME =
            "org.apache.hadoop.hbase.ipc.RpcControllerFactory";
    private static volatile boolean checked = false;
    private static boolean rpcControllerExists = false;

    private IndexQosCompat() {
        // private ctor for util class
    }

    /**
     * @param tableName name of the index table
     * @return configuration key for if a table should have Index QOS writes (its a target index
     *         table)
     */
    public static String getTableIndexQosConfKey(String tableName) {
        return "phoenix.index.table.qos._" + tableName;
    }

    /**
     * Set the index rpc controller, if the rpc controller exists. No-op if there the RpcController
     * is not on the classpath.
     * @param conf to update
     */
    public static void setPhoenixIndexRpcController(Configuration conf) {
        if (rpcControllerExists()) {
            // then we can load the class just fine
            conf.set(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY,
                PhoenixIndexRpcSchedulerFactory.class.getName());
        }
    }

    private static boolean rpcControllerExists() {
        if (checked) {
            synchronized (IndexQosCompat.class) {
                if (!checked) {
                    // try loading the class
                    try {
                        Class.forName(HBASE_RPC_CONTROLLER_CLASS_NAME);
                        rpcControllerExists = true;
                    } catch (ClassNotFoundException e) {
                        LOG.warn("RpcControllerFactory doesn't exist, not setting custom index handler properties.");
                        rpcControllerExists = false;
                    }

                    checked = true;
                }
            }
        }
        return rpcControllerExists;
    }

    /**
     * Ensure that the given table is enabled for index QOS handling
     * @param conf configuration to read/update
     * @param tableName name of the table to configure for index handlers
     */
    public static void enableIndexQosForTable(Configuration conf, String tableName) {
        String confKey = IndexQosCompat.getTableIndexQosConfKey(tableName);
        if (conf.get(confKey) == null) {
            conf.setBoolean(confKey, true);
        }
    }
}