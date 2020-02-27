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
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.phoenix.compat.hbase.CompatDelegateRegionCoprocessorEnvironment;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionType;

/**
 * Class to encapsulate {@link RegionCoprocessorEnvironment} for phoenix coprocessors. Often we
 * clone the configuration provided by the HBase coprocessor environment before modifying it. So
 * this class comes in handy where we have to return our custom config.
 */
public class DelegateRegionCoprocessorEnvironment extends CompatDelegateRegionCoprocessorEnvironment {

    private final Configuration config;
    private HTableFactory tableFactory;

    public DelegateRegionCoprocessorEnvironment(RegionCoprocessorEnvironment delegate, ConnectionType connectionType) {
        this.config = ServerUtil.ConnectionFactory.getTypeSpecificConfiguration(connectionType, delegate.getConfiguration());
        this.delegate = delegate;
        this.tableFactory = ServerUtil.getDelegateHTableFactory(this, connectionType);
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
    public Coprocessor getInstance() {
        return delegate.getInstance();
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
    public HTableInterface getTable(TableName tableName) throws IOException {
        return tableFactory.getTable(new ImmutableBytesPtr(tableName.getName()));
    }

    @Override
    public HTableInterface getTable(TableName tableName, ExecutorService service)
            throws IOException {
        return tableFactory.getTable(new ImmutableBytesPtr(tableName.getName()));
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
    public HRegionInfo getRegionInfo() {
        return delegate.getRegionInfo();
    }

    @Override
    public RegionServerServices getRegionServerServices() {
        return delegate.getRegionServerServices();
    }

    @Override
    public ConcurrentMap<String, Object> getSharedData() {
        return delegate.getSharedData();
    }

}
