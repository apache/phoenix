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
package org.apache.phoenix.hive.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;

/**
 * Set of methods to obtain Connection depending on configuration
 */

public class PhoenixConnectionUtil {

    private static final Log LOG = LogFactory.getLog(PhoenixConnectionUtil.class);

    public static Connection getInputConnection(final Configuration conf, final Properties props)
            throws SQLException {
        String quorum = conf.get(PhoenixStorageHandlerConstants.ZOOKEEPER_QUORUM);
        quorum = quorum == null ? props.getProperty(PhoenixStorageHandlerConstants
                .ZOOKEEPER_QUORUM, PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_QUORUM) :
                quorum;

        int zooKeeperClientPort = conf.getInt(PhoenixStorageHandlerConstants.ZOOKEEPER_PORT, 0);
        zooKeeperClientPort = zooKeeperClientPort == 0 ?
                Integer.parseInt(props.getProperty(PhoenixStorageHandlerConstants.ZOOKEEPER_PORT,
                        String.valueOf(PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_PORT))) :
                zooKeeperClientPort;

        String zNodeParent = conf.get(PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT);
        zNodeParent = zNodeParent == null ? props.getProperty(PhoenixStorageHandlerConstants
                .ZOOKEEPER_PARENT, PhoenixStorageHandlerConstants.DEFAULT_ZOOKEEPER_PARENT) :
                zNodeParent;

        return getConnection(quorum, zooKeeperClientPort, zNodeParent, PropertiesUtil
                .combineProperties(props, conf));
    }

    public static Connection getConnection(final Table table) throws SQLException {
        Map<String, String> tableParameterMap = table.getParameters();

        String zookeeperQuorum = tableParameterMap.get(PhoenixStorageHandlerConstants
                .ZOOKEEPER_QUORUM);
        zookeeperQuorum = zookeeperQuorum == null ? PhoenixStorageHandlerConstants
                .DEFAULT_ZOOKEEPER_QUORUM : zookeeperQuorum;

        String clientPortString = tableParameterMap.get(PhoenixStorageHandlerConstants
                .ZOOKEEPER_PORT);
        int clientPort = clientPortString == null ? PhoenixStorageHandlerConstants
                .DEFAULT_ZOOKEEPER_PORT : Integer.parseInt(clientPortString);

        String zNodeParent = tableParameterMap.get(PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT);
        zNodeParent = zNodeParent == null ? PhoenixStorageHandlerConstants
                .DEFAULT_ZOOKEEPER_PARENT : zNodeParent;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            LOG.warn(e.getStackTrace());
        }
        return DriverManager.getConnection(QueryUtil.getUrl(zookeeperQuorum, clientPort,
                zNodeParent));
    }

    private static Connection getConnection(final String quorum, final Integer clientPort, String
            zNodeParent, Properties props) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connection attrs [quorum, port, znode] : " + quorum + ", " + clientPort +
                    ", " +
                    zNodeParent);
        }

        return DriverManager.getConnection(clientPort != null ? QueryUtil.getUrl(quorum,
                clientPort, zNodeParent) : QueryUtil.getUrl(quorum), props);
    }

    public static Configuration getConfiguration(JobConf jobConf) {
        Configuration conf = new Configuration(jobConf);
        String quorum = conf.get(PhoenixStorageHandlerConstants.ZOOKEEPER_QUORUM);
        if(quorum!=null) {
            conf.set(HConstants.ZOOKEEPER_QUORUM, quorum);
        }
        int zooKeeperClientPort = conf.getInt(PhoenixStorageHandlerConstants.ZOOKEEPER_PORT, 0);
        if(zooKeeperClientPort != 0) {
            conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zooKeeperClientPort);
        }
        String zNodeParent = conf.get(PhoenixStorageHandlerConstants.ZOOKEEPER_PARENT);
        if(zNodeParent != null) {
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zNodeParent);
        }
        return conf;
    }
}
