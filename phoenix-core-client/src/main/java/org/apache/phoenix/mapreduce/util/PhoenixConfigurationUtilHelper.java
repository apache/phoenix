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

package org.apache.phoenix.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.PhoenixRuntime;

public final class PhoenixConfigurationUtilHelper {
    // This relies on Hadoop Configuration to handle warning about deprecated configs and
    // to set the correct non-deprecated configs when an old one shows up.
    static {
        Configuration.addDeprecation("phoneix.mapreduce.output.cluster.quorum", PhoenixConfigurationUtilHelper.MAPREDUCE_OUTPUT_CLUSTER_QUORUM);
    }

    @Deprecated
    public static final String MAPREDUCE_INPUT_CLUSTER_QUORUM = "phoenix.mapreduce.input.cluster.quorum";
    @Deprecated
    public static final String MAPREDUCE_OUTPUT_CLUSTER_QUORUM = "phoenix.mapreduce.output.cluster.quorum";
    public static final String MAPREDUCE_INPUT_CLUSTER_URL = "phoenix.mapreduce.input.cluster.url";
    public static final String MAPREDUCE_OUTPUT_CLUSTER_URL = "phoenix.mapreduce.output.cluster.url";
    public static final String TRANSFORM_MONITOR_ENABLED = "phoenix.transform.monitor.enabled";
    public static final boolean DEFAULT_TRANSFORM_MONITOR_ENABLED = true;
    /**
     * Get the value of the <code>name</code> property as a set of comma-delimited
     * <code>long</code> values.
     * If no such property exists, null is returned.
     * Hadoop Configuration object has support for getting ints delimited by comma
     * but doesn't support for long.
     * @param name property name
     * @return property value interpreted as an array of comma-delimited
     *         <code>long</code> values
     */
    public static long[] getLongs(Configuration conf, String name) {
        String[] strings = conf.getTrimmedStrings(name);
        // Configuration#getTrimmedStrings will never return null.
        // If key is not found, it will return empty array.
        if (strings.length == 0) {
            return null;
        }
        long[] longs = new long[strings.length];
        for (int i = 0; i < strings.length; i++) {
            longs[i] = Long.parseLong(strings[i]);
        }
        return longs;
    }

    /**
     * Returns the ZooKeeper quorum string for the HBase cluster a Phoenix MapReduce job will read
     * from. If MAPREDUCE_OUTPUT_CLUSTER_QUORUM is not set, then it returns the value of
     * HConstants.ZOOKEEPER_QUORUM
     * @param configuration
     * @return ZooKeeper quorum string
     */
    @Deprecated
    public static String getInputCluster(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String quorum = configuration.get(MAPREDUCE_INPUT_CLUSTER_QUORUM);
        if (quorum == null) {
            quorum = configuration.get(HConstants.CLIENT_ZOOKEEPER_QUORUM);
        }
        if (quorum == null) {
            quorum = configuration.get(HConstants.ZOOKEEPER_QUORUM);
        }
        return quorum;
    }

    /**
     * Returns the Phoenix JDBC URL a Phoenix MapReduce job will read
     * from. If MAPREDUCE_INPUT_CLUSTER_URL is not set, then it returns the value of
     * "jdbc:phoenix"
     * @param configuration
     * @return URL string
     */
    public static String getInputClusterUrl(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String url = configuration.get(MAPREDUCE_INPUT_CLUSTER_URL);
        if (url == null) {
            url = PhoenixRuntime.JDBC_PROTOCOL;
        }
        return url;
    }

    /**
     * Returns the HBase Client Port
     * @param configuration
     * @return
     */
    @Deprecated
    public static Integer getClientPort(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String clientPortString = configuration.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        return clientPortString==null ? null : Integer.parseInt(clientPortString);
    }

    /**
     * Returns the HBase zookeeper znode parent
     * @param configuration
     * @return
     */
    @Deprecated
    public static String getZNodeParent(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
    }

    /**
     * Returns the ZooKeeper quorum string for the HBase cluster a Phoenix MapReduce job will write
     * to. If MAPREDUCE_OUTPUT_CLUSTER_QUORUM is not set, then it returns the value of
     * HConstants.ZOOKEEPER_QUORUM
     * @param configuration
     * @return ZooKeeper quorum string
     */
    @Deprecated
    public static String getOutputCluster(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String quorum = configuration.get(MAPREDUCE_OUTPUT_CLUSTER_QUORUM);
        if (quorum == null) {
            quorum = configuration.get(HConstants.CLIENT_ZOOKEEPER_QUORUM);
        }
        if (quorum == null) {
            quorum = configuration.get(HConstants.ZOOKEEPER_QUORUM);
        }
        return quorum;
    }

    /**
     * Returns the ZooKeeper quorum string for the HBase cluster a Phoenix MapReduce job will
     * read from
     * @param configuration
     * @return ZooKeeper quorum string if defined, null otherwise
     */
    @Deprecated
    public static String getInputClusterZkQuorum(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(MAPREDUCE_INPUT_CLUSTER_QUORUM);
    }


    /**
     * Returns the Phoenix JDBC URL a Phoenix MapReduce job will write to.
     * If MAPREDUCE_OUTPUT_CLUSTER_URL is not set, then it returns the value of
     * "jdbc:phoenix"
     * @param configuration
     * @return URL string
     */
    public static String getOutputClusterUrl(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String quorum = configuration.get(MAPREDUCE_OUTPUT_CLUSTER_URL);
        if (quorum == null) {
            quorum = PhoenixRuntime.JDBC_PROTOCOL;
        }
        return quorum;
    }

    /**
     * Returns the value of HConstants.ZOOKEEPER_QUORUM.
     * For tests only
     * @param configuration
     * @return ZooKeeper quorum string if defined, null otherwise
     */
    @Deprecated
    public static String getZKQuorum(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(HConstants.CLIENT_ZOOKEEPER_QUORUM,
            configuration.get(HConstants.ZOOKEEPER_QUORUM));
    }

    /**
     * Returns the ZooKeeper quorum override MAPREDUCE_OUTPUT_CLUSTER_QUORUM for mapreduce jobs
     * @param configuration
     * @return ZooKeeper quorum string if defined, null otherwise
     */
    @Deprecated
    public static String getOutputClusterZkQuorum(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(MAPREDUCE_OUTPUT_CLUSTER_QUORUM);
    }
}
