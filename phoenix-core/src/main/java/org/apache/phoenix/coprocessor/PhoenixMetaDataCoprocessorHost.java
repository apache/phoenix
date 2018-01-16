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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;

public class PhoenixMetaDataCoprocessorHost
        extends CoprocessorHost<PhoenixMetaDataCoprocessorHost.PhoenixMetaDataControllerEnvironment> {
    private RegionCoprocessorEnvironment env;
    public static final String PHOENIX_META_DATA_COPROCESSOR_CONF_KEY =
            "hbase.coprocessor.phoenix.classes";
    public static final String DEFAULT_PHOENIX_META_DATA_COPROCESSOR_CONF_KEY="org.apache.phoenix.coprocessor.PhoenixAccessController";

    public PhoenixMetaDataCoprocessorHost(RegionCoprocessorEnvironment env) {
        super(null);
        this.env = env;
        this.conf = env.getConfiguration();
        boolean accessCheckEnabled = this.conf.getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
        if (this.conf.get(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY) == null && accessCheckEnabled) {
            this.conf.set(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY, DEFAULT_PHOENIX_META_DATA_COPROCESSOR_CONF_KEY);
        }
        loadSystemCoprocessors(conf, PHOENIX_META_DATA_COPROCESSOR_CONF_KEY);
    }

    private static abstract class CoprocessorOperation<T extends CoprocessorEnvironment> extends ObserverContext<T> {
        abstract void call(MetaDataEndpointObserver oserver, ObserverContext<T> ctx) throws IOException;

        public void postEnvCall(T env) {}
    }

    private boolean execOperation(
            final CoprocessorOperation<PhoenixMetaDataCoprocessorHost.PhoenixMetaDataControllerEnvironment> ctx)
            throws IOException {
        if (ctx == null) return false;
        boolean bypass = false;
        for (PhoenixMetaDataControllerEnvironment env : coprocessors) {
            if (env.getInstance() instanceof MetaDataEndpointObserver) {
                ctx.prepare(env);
                Thread currentThread = Thread.currentThread();
                ClassLoader cl = currentThread.getContextClassLoader();
                try {
                    currentThread.setContextClassLoader(env.getClassLoader());
                    ctx.call((MetaDataEndpointObserver)env.getInstance(), ctx);
                } catch (Throwable e) {
                    handleCoprocessorThrowable(env, e);
                } finally {
                    currentThread.setContextClassLoader(cl);
                }
                bypass |= ctx.shouldBypass();
                if (ctx.shouldComplete()) {
                    break;
                }
            }
            ctx.postEnvCall(env);
        }
        return bypass;
    }
    
    @Override
    protected void handleCoprocessorThrowable(final CoprocessorEnvironment env, final Throwable e) throws IOException {
        if (e instanceof IOException) {
            if (e.getCause() instanceof DoNotRetryIOException) { throw (IOException)e.getCause(); }
        }
        super.handleCoprocessorThrowable(env, e);
    }

    /**
     * Encapsulation of the environment of each coprocessor
     */
    static class PhoenixMetaDataControllerEnvironment extends CoprocessorHost.Environment
            implements RegionCoprocessorEnvironment {

        private RegionCoprocessorEnvironment env;

        public PhoenixMetaDataControllerEnvironment(RegionCoprocessorEnvironment env, Coprocessor instance,
                int priority, int sequence, Configuration conf) {
            super(instance, priority, sequence, conf);
            this.env = env;
        }

        @Override
        public RegionServerServices getRegionServerServices() {
            return env.getRegionServerServices();
        }

        public RegionCoprocessorHost getCoprocessorHost() {
            return env.getRegion().getCoprocessorHost();
        }

        @Override
        public Region getRegion() {
            return env.getRegion();
        }

        @Override
        public HRegionInfo getRegionInfo() {
            return env.getRegionInfo();
        }

        @Override
        public ConcurrentMap<String, Object> getSharedData() {
            return env.getSharedData();
        }

        @Override
        public MetricRegistry getMetricRegistryForRegionServer() {
            return env.getMetricRegistryForRegionServer();
        }
    }

    @Override
    public PhoenixMetaDataControllerEnvironment createEnvironment(Class<?> implClass, Coprocessor instance,
            int priority, int sequence, Configuration conf) {
        return new PhoenixMetaDataControllerEnvironment(env, instance, priority, sequence, conf);
    }

    public void preGetTable(final String tenantId, final String tableName, final TableName physicalTableName)
            throws IOException {
        execOperation(new CoprocessorOperation<PhoenixMetaDataControllerEnvironment>() {
            @Override
            public void call(MetaDataEndpointObserver observer,
                    ObserverContext<PhoenixMetaDataControllerEnvironment> ctx) throws IOException {
                observer.preGetTable(ctx, tenantId, tableName, physicalTableName);
            }
        });
    }

    public void preCreateTable(final String tenantId, final String tableName, final TableName physicalTableName,
            final TableName parentPhysicalTableName, final PTableType tableType, final Set<byte[]> familySet, final Set<TableName> indexes)
            throws IOException {
        execOperation(new CoprocessorOperation<PhoenixMetaDataControllerEnvironment>() {
            @Override
            public void call(MetaDataEndpointObserver observer,
                    ObserverContext<PhoenixMetaDataControllerEnvironment> ctx) throws IOException {
                observer.preCreateTable(ctx, tenantId, tableName, physicalTableName, parentPhysicalTableName, tableType,
                        familySet, indexes);
            }
        });
    }

    public void preDropTable(final String tenantId, final String tableName, final TableName physicalTableName,
            final TableName parentPhysicalTableName, final PTableType tableType, final List<PTable> indexes) throws IOException {
        execOperation(new CoprocessorOperation<PhoenixMetaDataControllerEnvironment>() {
            @Override
            public void call(MetaDataEndpointObserver observer,
                    ObserverContext<PhoenixMetaDataControllerEnvironment> ctx) throws IOException {
                observer.preDropTable(ctx, tenantId, tableName, physicalTableName, parentPhysicalTableName, tableType, indexes);
            }
        });
    }

    public void preAlterTable(final String tenantId, final String tableName, final TableName physicalTableName,
            final TableName parentPhysicalTableName, final PTableType type) throws IOException {
        execOperation(new CoprocessorOperation<PhoenixMetaDataControllerEnvironment>() {
            @Override
            public void call(MetaDataEndpointObserver observer,
                    ObserverContext<PhoenixMetaDataControllerEnvironment> ctx) throws IOException {
                observer.preAlterTable(ctx, tenantId, tableName, physicalTableName, parentPhysicalTableName, type);
            }
        });
    }

    public void preGetSchema(final String schemaName) throws IOException {
        execOperation(new CoprocessorOperation<PhoenixMetaDataControllerEnvironment>() {
            @Override
            public void call(MetaDataEndpointObserver observer,
                    ObserverContext<PhoenixMetaDataControllerEnvironment> ctx) throws IOException {
                observer.preGetSchema(ctx, schemaName);
            }
        });
    }

    public void preCreateSchema(final String schemaName) throws IOException {

        execOperation(new CoprocessorOperation<PhoenixMetaDataControllerEnvironment>() {
            @Override
            public void call(MetaDataEndpointObserver observer,
                    ObserverContext<PhoenixMetaDataControllerEnvironment> ctx) throws IOException {
                observer.preCreateSchema(ctx, schemaName);
            }
        });
    }

    public void preDropSchema(final String schemaName) throws IOException {
        execOperation(new CoprocessorOperation<PhoenixMetaDataControllerEnvironment>() {
            @Override
            public void call(MetaDataEndpointObserver observer,
                    ObserverContext<PhoenixMetaDataControllerEnvironment> ctx) throws IOException {
                observer.preDropSchema(ctx, schemaName);
            }
        });
    }

    public void preIndexUpdate(final String tenantId, final String indexName, final TableName physicalTableName,
            final TableName parentPhysicalTableName, final PIndexState newState) throws IOException {
        execOperation(new CoprocessorOperation<PhoenixMetaDataControllerEnvironment>() {
            @Override
            public void call(MetaDataEndpointObserver observer,
                    ObserverContext<PhoenixMetaDataControllerEnvironment> ctx) throws IOException {
                observer.preIndexUpdate(ctx, tenantId, indexName, physicalTableName, parentPhysicalTableName, newState);
            }
        });
    }
}
