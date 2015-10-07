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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;

/**
 * Utility class to return a {@link Connection} .
 */
public class ConnectionUtil {


    /**
     * Retrieve the configured input Connection.
     *
     * @param conf configuration containing connection information
     * @return the configured input connection
     */
    public static Connection getInputConnection(final Configuration conf) throws SQLException {
        return getInputConnection(conf, new Properties());
    }
    
    /**
     * Retrieve the configured input Connection.
     *
     * @param conf configuration containing connection information
     * @param props custom connection properties
     * @return the configured input connection
     */
    public static Connection getInputConnection(final Configuration conf , final Properties props) throws SQLException {
        Preconditions.checkNotNull(conf);
		return getConnection(PhoenixConfigurationUtil.getInputCluster(conf),
				PhoenixConfigurationUtil.getClientPort(conf),
				PhoenixConfigurationUtil.getZNodeParent(conf),
				PropertiesUtil.extractProperties(props, conf));
    }

    /**
     * Create the configured output Connection.
     *
     * @param conf configuration containing the connection information
     * @return the configured output connection
     */
    public static Connection getOutputConnection(final Configuration conf) throws SQLException {
        return getOutputConnection(conf, new Properties());
    }
    
    /**
     * Create the configured output Connection.
     *
     * @param conf configuration containing the connection information
     * @param props custom connection properties
     * @return the configured output connection
     */
    public static Connection getOutputConnection(final Configuration conf, Properties props) throws SQLException {
        Preconditions.checkNotNull(conf);
		return getConnection(PhoenixConfigurationUtil.getOutputCluster(conf),
				PhoenixConfigurationUtil.getClientPort(conf),
				PhoenixConfigurationUtil.getZNodeParent(conf),
				PropertiesUtil.extractProperties(props, conf));
    }

    /**
     * Returns the {@link Connection} from a ZooKeeper cluster string.
     *
     * @param quorum a ZooKeeper quorum connection string
     * @param clientPort a ZooKeeper client port
     * @param znodeParent a zookeeper znode parent
     * @return a Phoenix connection to the given connection string
     */
    private static Connection getConnection(final String quorum, final Integer clientPort, final String znodeParent, Properties props) throws SQLException {
        Preconditions.checkNotNull(quorum);
        return DriverManager.getConnection(QueryUtil.getUrl(quorum, clientPort, znodeParent), props);
    }

}
