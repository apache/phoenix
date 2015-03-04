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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.base.Preconditions;

/**
 * Utility class to return a {@link Connection} .
 */
public class ConnectionUtil {
    
    /**
     * Returns the {@link Connection} from Configuration
     * @param configuration
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        final Properties props = new Properties();
        Iterator<Map.Entry<String, String>> iterator = configuration.iterator();
        if(iterator != null) {
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                props.setProperty(entry.getKey(), entry.getValue());
            }
        }
        final Connection conn = DriverManager.getConnection(QueryUtil.getUrl(configuration.get(HConstants.ZOOKEEPER_QUORUM)), props);
        return conn;
    }

}
