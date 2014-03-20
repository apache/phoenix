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
package org.apache.phoenix.end2end;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
    protected int NUM_SLAVES_BASE = 1; // number of slaves for the cluster
    
    private HBaseTestingUtility util;

    public ConnectionQueryServicesTestImpl(QueryServices services, ConnectionInfo info) throws SQLException {
        super(services, info);
    }

    private Configuration setupServer(Configuration config) throws Exception {
        // The HBaseTestingUtility has some kind of memory leak in HBase 0.94.15+ on the Mac
        // so the pom will use 0.94.14 until this gets fixed.
        if(isDistributedCluster(config)){
            IntegrationTestingUtility util =  new IntegrationTestingUtility(config);
            util.initializeCluster(this.NUM_SLAVES_BASE);
            this.util = util;
            // remove all hbase tables
            HBaseAdmin admin = util.getHBaseAdmin();
            HTableDescriptor[] tables = admin.listTables();
            for(HTableDescriptor table : tables){
                util.deleteTable(table.getName());
            }
        } else {
            util = new HBaseTestingUtility(config);
            util.startMiniCluster();
        }
        return util.getConfiguration();
    }
    
    public boolean isDistributedCluster(Configuration conf) {
        boolean isDistributedCluster = false;
        isDistributedCluster = Boolean.parseBoolean(System.getProperty(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER, "false"));
        if (!isDistributedCluster) {
          isDistributedCluster = conf.getBoolean(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER, false);
        }
        return isDistributedCluster;
    }
    
    private void teardownServer() throws Exception {
        if(isDistributedCluster(util.getConfiguration())){
            // remove all hbase tables
            HBaseAdmin admin = util.getHBaseAdmin();
            HTableDescriptor[] tables = admin.listTables();
            for(HTableDescriptor table : tables){
                util.deleteTable(table.getName());
            }
        } else {
            util.shutdownMiniCluster();
        }
    }
    
    @Override
    public void init(String url, Properties props) throws SQLException {
        try {
            // during test, we don't do hbase.table.sanity.checks
            config.setBoolean("hbase.table.sanity.checks", false);
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
            try {
                // Attempt to fix apparent memory leak...
                clearCache();
            } finally {
                super.close();
            }
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
