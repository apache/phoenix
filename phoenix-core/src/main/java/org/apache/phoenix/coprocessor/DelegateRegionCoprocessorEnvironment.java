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
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.phoenix.util.ServerUtil.ConnectionFactory;
import org.apache.phoenix.util.ServerUtil.ConnectionType;

/**
 * Class to encapsulate {@link RegionCoprocessorEnvironment} for phoenix coprocessors. Often we
 * clone the configuration provided by the HBase coprocessor environment before modifying it. So
 * this class comes in handy where we have to return our custom config.
 */
public class DelegateRegionCoprocessorEnvironment implements RegionCoprocessorEnvironment {

    private final Configuration config;
    private RegionCoprocessorEnvironment delegate;
    private ConnectionType connectionType;

    public DelegateRegionCoprocessorEnvironment(RegionCoprocessorEnvironment delegate, ConnectionType connectionType) {
        this.delegate = delegate;
        this.connectionType = connectionType;
        this.config = ConnectionFactory.getTypeSpecificConfiguration(connectionType, delegate.getConfiguration());
    }

    @Override
    public int getVersion() {
        return delegate.getVersion();
    }

    @Override
    public String getHBaseVersion() {
        return delegate.getHBaseVersion();
    }

    @Override
    public int getPriority() {
        return delegate.getPriority();
    }

    @Override
    public int getLoadSequence() {
        return delegate.getLoadSequence();
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
    }

    @Override
    public Region getRegion() {
        return delegate.getRegion();
    }

    @Override
    public RegionInfo getRegionInfo() {
        return delegate.getRegionInfo();
    }

    @Override
    public ConcurrentMap<String, Object> getSharedData() {
        return delegate.getSharedData();
    }

    @Override
    public RegionCoprocessor getInstance() {
        return delegate.getInstance();
    }

    @Override
    public OnlineRegions getOnlineRegions() {
        return delegate.getOnlineRegions();
    }

    @Override
    public ServerName getServerName() {
        return delegate.getServerName();
    }

    @Override
    public Connection getConnection() {
        return ConnectionFactory.getConnection(connectionType, delegate);
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
        return delegate.getMetricRegistryForRegionServer();
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
        return delegate.createConnection(conf);
    }

    @Override
    public RawCellBuilder getCellBuilder() {
        return delegate.getCellBuilder();
    }
   
    
}
