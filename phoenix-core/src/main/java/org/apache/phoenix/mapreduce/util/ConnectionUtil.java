/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
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
import org.apache.phoenix.util.QueryUtil;

import com.google.common.base.Preconditions;

/**
 * Utility class to return a {#link Connection} 
 */
public final class ConnectionUtil {
    
    /**
     * Returns the {#link Connection} from Configuration
     * @param configuration
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        final Properties props = new Properties();
        final Connection conn = DriverManager.getConnection(QueryUtil.getUrl(configuration.get(HConstants.ZOOKEEPER_QUORUM)), props);
        return conn;
    }
    
}
