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

package org.apache.phoenix.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.util.JacksonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_ATTR_PREFIX;
import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.getLocalZkUrl;


/**
 * A tool which generates cluster role records into JSON file assuming this cluster is ACTIVE and
 * peer cluster with (default id=1) is STANDBY.
 */
public class ClusterRoleRecordGeneratorTool extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRoleRecordGeneratorTool.class);

    /** Key/attribute prefix for this static store. */
    private static final String GENERATOR_ATTR_PREFIX = PHOENIX_HA_ATTR_PREFIX + "role.generator.";
    /** The output JSON file name to write; if not configured, a temp file will be created. */
    public static final String PHOENIX_HA_GENERATOR_FILE_ATTR = GENERATOR_ATTR_PREFIX + "file";
    /** The key of all HA group names this static store will return, separated by comma. */
    public static final String PHOENIX_HA_GROUPS_ATTR = GENERATOR_ATTR_PREFIX + "groups";
    /** Config key format for the HA policy; should be formatted with a specific group name. */
    public static final String PHOENIX_HA_GROUP_POLICY_ATTR_FORMAT =
            GENERATOR_ATTR_PREFIX + "policy.%s";
    /** The replication peer cluster id for one HA group. */
    public static final String PHOENIX_HA_GROUP_STORE_PEER_ID_ATTR_FORMAT =
            GENERATOR_ATTR_PREFIX + "store.peer.id.%s";
    public static final String PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT = "1";

    @Override
    public int run(String[] args) throws Exception {
        try {
            String fileName = getConf().get(PHOENIX_HA_GENERATOR_FILE_ATTR);
            File file = StringUtils.isEmpty(fileName)
                    ? File.createTempFile("phoenix.ha.cluster.role.records", ".json")
                    : new File(fileName);
            JacksonUtil.getObjectWriterPretty().writeValue(file, listAllRecordsByZk());
            System.out.println("Created JSON file '" + file + "'");
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    List<ClusterRoleRecord> listAllRecordsByZk() throws IOException {
        /* This current cluster's full ZK url for HBase, in host:port:/hbase format. */
        String localZkUrl = getLocalZkUrl(getConf());
        final String[] haGroupNames = getConf().getStrings(PHOENIX_HA_GROUPS_ATTR);
        if (haGroupNames == null || haGroupNames.length == 0) {
            String msg = "No HA groups configured for this cluster via " + PHOENIX_HA_GROUPS_ATTR;
            LOG.error(msg);
            throw new IOException(msg);
        }

        List<ClusterRoleRecord> records = new ArrayList<>();
        for (String haGroupName : haGroupNames) {
            String peerZkUrl = getPeerZkUrl(getConf(), haGroupName);
            records.add(new ClusterRoleRecord(haGroupName, getHaPolicy(haGroupName),
                    localZkUrl, ClusterRole.ACTIVE,
                    peerZkUrl, ClusterRole.STANDBY,
                    1));
        }
        LOG.debug("Returning all cluster role records discovered: {}", records);
        return records;
    }

    /**
     * Helper method to get the replication peer's ZK URL (host:port) from Configuration.
     */
    private static String getPeerZkUrl(Configuration conf, String haGroupName) throws IOException {
        String key = String.format(PHOENIX_HA_GROUP_STORE_PEER_ID_ATTR_FORMAT, haGroupName);
        String peerId = conf.get(key, PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            return getPeerClusterKey(connection.getAdmin(), peerId);
        }
    }

    /**
     * Helper method to get the replication peer's cluster key from replication config.
     *
     * This assumes the peer has the static fixed given id.
     */
    @VisibleForTesting
    static String getPeerClusterKey(Admin admin, String id)
            throws IOException {
        ReplicationPeerConfig replicationConfig;
        try {
            replicationConfig = admin.getReplicationPeerConfig(id);
        } catch (IOException io) {
            String msg = "Can not get replication peer (id=" + id + ") config";
            LOG.error(msg, io);
            throw io;
        }
        String peerZk = replicationConfig.getClusterKey();
        if (StringUtils.isEmpty(peerZk)) {
            String msg = "Peer (id=" + id + ") ZK quorum is not set!";
            LOG.error(msg);
            throw new IOException(msg);
        }

        return peerZk;
    }

    /** Helper method to get the HA policy from configuration for the given HA group. */
    @VisibleForTesting
    HighAvailabilityPolicy getHaPolicy(String haGroupName) throws IOException {
        String key = String.format(PHOENIX_HA_GROUP_POLICY_ATTR_FORMAT, haGroupName);
        String value = getConf().get(key, HighAvailabilityPolicy.PARALLEL.name());
        try {
            return HighAvailabilityPolicy.valueOf(value);
        } catch (IllegalArgumentException e) {
            String msg = "Invalid HA policy name '" + value + "' for HA group " + haGroupName;
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int retCode = ToolRunner.run(conf, new ClusterRoleRecordGeneratorTool(), args);
        System.exit(retCode);
    }
}
