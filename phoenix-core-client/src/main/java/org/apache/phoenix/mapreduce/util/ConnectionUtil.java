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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.PropertiesUtil;

/**
 * Utility class to return a {@link Connection} .
 */
public class ConnectionUtil {

    /**
     * Retrieve the configured input Connection.
     * @param conf configuration containing connection information
     * @return the configured input connection
     */
    public static Connection getInputConnection(final Configuration conf) throws SQLException {
        Preconditions.checkNotNull(conf);
        return getInputConnection(conf, new Properties());
    }

    /**
     * Retrieve the configured input Connection.
     * @param conf configuration containing connection information
     * @param props custom connection properties
     * @return the configured input connection
     */
    public static Connection getInputConnection(final Configuration conf, final Properties props)
            throws SQLException {
        String inputQuorum = PhoenixConfigurationUtilHelper.getInputCluster(conf);
        if (inputQuorum != null) {
            // This will not override the quorum set with setInputClusterUrl
            Properties copyProps = PropertiesUtil.deepCopy(props);
            copyProps.setProperty(HConstants.CLIENT_ZOOKEEPER_QUORUM, inputQuorum);
            return DriverManager.getConnection(
                PhoenixConfigurationUtilHelper.getInputClusterUrl(conf),
                PropertiesUtil.combineProperties(copyProps, conf));
        }
        return DriverManager.getConnection(PhoenixConfigurationUtilHelper.getInputClusterUrl(conf),
            PropertiesUtil.combineProperties(props, conf));
    }

    /**
     * Create the configured output Connection.
     * @param conf configuration containing the connection information
     * @return the configured output connection
     */
    public static Connection getOutputConnection(final Configuration conf) throws SQLException {
        return getOutputConnection(conf, new Properties());
    }

    /**
     * Create the configured output Connection.
     * @param conf configuration containing the connection information
     * @param props custom connection properties
     * @return the configured output connection
     */
    public static Connection getOutputConnection(final Configuration conf, Properties props)
            throws SQLException {
        Preconditions.checkNotNull(conf);
        String outputQuorum = PhoenixConfigurationUtilHelper.getOutputCluster(conf);
        if (outputQuorum != null) {
            // This will not override the quorum set with setInputClusterUrl
            Properties copyProps = PropertiesUtil.deepCopy(props);
            copyProps.setProperty(HConstants.CLIENT_ZOOKEEPER_QUORUM, outputQuorum);
            return DriverManager.getConnection(
                PhoenixConfigurationUtilHelper.getInputClusterUrl(conf),
                PropertiesUtil.combineProperties(copyProps, conf));
        }
        return DriverManager.getConnection(PhoenixConfigurationUtilHelper.getOutputClusterUrl(conf),
            PropertiesUtil.combineProperties(props, conf));
    }
}
