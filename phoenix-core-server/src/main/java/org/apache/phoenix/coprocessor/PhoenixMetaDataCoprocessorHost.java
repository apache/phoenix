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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;


public class PhoenixMetaDataCoprocessorHost
        extends CoprocessorHost<PhoenixCoprocessor,PhoenixMetaDataCoprocessorHost.PhoenixMetaDataControllerEnvironment> {
    private RegionCoprocessorEnvironment env;
    public static final String PHOENIX_META_DATA_COPROCESSOR_CONF_KEY =
            "hbase.coprocessor.phoenix.classes";
    private static final String DEFAULT_PHOENIX_META_DATA_COPROCESSOR_CONF_KEY =
            "org.apache.phoenix.coprocessor.PhoenixAccessController";

    PhoenixMetaDataCoprocessorHost(RegionCoprocessorEnvironment env) throws IOException {
        super(null);
        this.env = env;
        this.conf = new Configuration();
        for (Entry<String, String> entry : env.getConfiguration()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        boolean accessCheckEnabled = this.conf.getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
        if (this.conf.get(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY) == null && accessCheckEnabled) {
            this.conf.set(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY, DEFAULT_PHOENIX_META_DATA_COPROCESSOR_CONF_KEY);
        }
       loadSystemCoprocessors(conf, PHOENIX_META_DATA_COPROCESSOR_CONF_KEY);
    }

    private ObserverGetter<PhoenixCoprocessor, MetaDataEndpointObserver> phoenixObserverGetter =
            PhoenixCoprocessor::getPhoenixObserver;
    
    private abstract class PhoenixObserverOperation extends ObserverOperationWithoutResult<MetaDataEndpointObserver> {
        PhoenixObserverOperation() {
            super(phoenixObserverGetter);
        }

        public PhoenixObserverOperation(User user) {
            super(phoenixObserverGetter, user);
        }

        public PhoenixObserverOperation(User user, boolean bypassable) {
            super(phoenixObserverGetter, user, bypassable);
        }
        
        void callObserver() throws IOException {
            Optional<MetaDataEndpointObserver> observer = phoenixObserverGetter.apply(getEnvironment().getInstance());
            if (observer.isPresent()) {
                call(observer.get());
            }
        }

        @Override
        protected void postEnvCall() {}
    }

    private boolean execOperation(
            final PhoenixObserverOperation ctx)
            throws IOException {
        if (ctx == null) return false;
        boolean bypass = false;
        for (PhoenixMetaDataControllerEnvironment env : coprocEnvironments) {
            if (env.getInstance() instanceof MetaDataEndpointObserver) {
                ctx.prepare(env);
                Thread currentThread = Thread.currentThread();
                ClassLoader cl = currentThread.getContextClassLoader();
                try {
                    currentThread.setContextClassLoader(env.getClassLoader());
                    ctx.callObserver();
                } catch (Throwable e) {
                    handleCoprocessorThrowable(env, e);
                } finally {
                    currentThread.setContextClassLoader(cl);
                }
                bypass |= ctx.shouldBypass();
                if (bypass) {
                    break;
                }
            }
            ctx.postEnvCall();
        }
        return bypass;
    }
    
    @Override
    protected void handleCoprocessorThrowable(final PhoenixMetaDataControllerEnvironment env, final Throwable e)
            throws IOException {
        if (e instanceof IOException) {
            if (e.getCause() instanceof DoNotRetryIOException) { throw (IOException)e.getCause(); }
        }
        super.handleCoprocessorThrowable(env, e);
    }

    /**
     * Encapsulation of the environment of each coprocessor
     */
    public static class PhoenixMetaDataControllerEnvironment extends BaseEnvironment<PhoenixCoprocessor>
            implements CoprocessorEnvironment<PhoenixCoprocessor> {

        private RegionCoprocessorEnvironment env;

        PhoenixMetaDataControllerEnvironment(RegionCoprocessorEnvironment env, PhoenixCoprocessor instance,
                int priority, int sequence, Configuration conf) {
            super(instance, priority, sequence, conf);
            this.env = env;
        }
        
        public RegionCoprocessorHost getCoprocessorHost() {
            return ((HRegion)env.getRegion()).getCoprocessorHost();
        }

        RegionCoprocessorEnvironment getRegionCoprocessorEnvironment() {
            return env;
        }
    }
    

    void preGetTable(final String tenantId, final String tableName, final TableName physicalTableName)
            throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preGetTable(this, tenantId, tableName, physicalTableName);
            }
        });
    }

    void preCreateTable(final String tenantId, final String tableName, final TableName physicalTableName,
            final TableName parentPhysicalTableName, final PTableType tableType, final Set<byte[]> familySet, final Set<TableName> indexes)
            throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preCreateTable(this, tenantId, tableName, physicalTableName, parentPhysicalTableName, tableType,
                        familySet, indexes);
            }
        });
    }

    void preCreateViewAddChildLink(final String tableName) throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preCreateViewAddChildLink(this, tableName);
            }
        });
    }

    void preDropTable(final String tenantId, final String tableName, final TableName physicalTableName,
            final TableName parentPhysicalTableName, final PTableType tableType, final List<PTable> indexes) throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preDropTable(this, tenantId, tableName, physicalTableName, parentPhysicalTableName, tableType, indexes);
            }
        });
    }

    void preAlterTable(final String tenantId, final String tableName, final TableName physicalTableName,
            final TableName parentPhysicalTableName, final PTableType type) throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preAlterTable(this, tenantId, tableName, physicalTableName, parentPhysicalTableName, type);
            }
        });
    }

    void preGetSchema(final String schemaName) throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preGetSchema(this, schemaName);
            }
        });
    }

    public void preCreateSchema(final String schemaName) throws IOException {

        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preCreateSchema(this, schemaName);
            }
        });
    }

    void preDropSchema(final String schemaName) throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preDropSchema(this, schemaName);
            }
        });
    }

    void preIndexUpdate(final String tenantId, final String indexName, final TableName physicalTableName,
            final TableName parentPhysicalTableName, final PIndexState newState) throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer) throws IOException {
                observer.preIndexUpdate(this, tenantId, indexName, physicalTableName, parentPhysicalTableName, newState);
            }
        });
    }

    @Override
    public PhoenixCoprocessor checkAndGetInstance(Class<?> implClass)
            throws InstantiationException, IllegalAccessException {
        if (PhoenixCoprocessor.class
                .isAssignableFrom(implClass)) { return (PhoenixCoprocessor)implClass.newInstance(); }
        return null;
    }

    @Override
    public PhoenixMetaDataControllerEnvironment createEnvironment(PhoenixCoprocessor instance, int priority,
            int sequence, Configuration conf) {
        return new PhoenixMetaDataControllerEnvironment(env, instance, priority, sequence, conf);
    }

    void preUpsertTaskDetails(final String tableName) throws IOException {
        execOperation(new PhoenixObserverOperation() {
            @Override
            public void call(MetaDataEndpointObserver observer)
                    throws IOException {
                observer.preUpsertTaskDetails(this, tableName);
            }
        });
    }
}
