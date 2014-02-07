/*
 * Copyright 2014 The Apache Software Foundation
 *
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
package org.apache.phoenix.end2end;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;


/**
 * 
 * Implementation of ConnectionQueryServices for tests running against
 * the mini cluster
 *
 * 
 * @since 0.1
 */
public class ConnectionQueryServicesTestImpl extends ConnectionQueryServicesImpl {
    private HBaseTestingUtility util;

    public ConnectionQueryServicesTestImpl(QueryServices services, ConnectionInfo info) throws SQLException {
        super(services, info);
    }

    private Configuration setupServer(Configuration config) throws Exception {
        util = new HBaseTestingUtility(config);
        util.startMiniCluster();
        return util.getConfiguration();
    }
    
    private void teardownServer() throws Exception {
        util.shutdownMiniCluster();
    }
    
    @Override
    public void init(String url, Properties props) throws SQLException {
        try {
            setupServer(config);
            super.init(url, props);
            /**
             * Clear the server-side meta data cache on initialization. Otherwise, if we
             * query for meta data tables, we'll get nothing (since the server just came
             * up). However, our server-side cache (which is a singleton) will claim
             * that we do have tables and our create table calls will return the cached
             * meta data instead of creating new metadata.
             */
            clearCache();
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

        @Override
    public void close() throws SQLException {
        SQLException sqlE = null;
        try {
            super.close();
        } catch (SQLException e)  {
            sqlE = e;
        } finally {
            try {
                teardownServer();
            } catch (Exception e) {
                if (sqlE == null) {
                    sqlE = new SQLException(e);
                } else {
                    sqlE.setNextException(new SQLException(e));
                }
            } finally {
                if (sqlE != null) {
                    throw sqlE;
                }
            }
        }
    }
    
}
