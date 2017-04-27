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
package org.apache.phoenix.index;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.IndexMetaDataCache;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.cache.TenantCache;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;
import org.apache.tephra.Transaction;

public class PhoenixIndexMetaData implements IndexMetaData {
    private final Map<String, byte[]> attributes;
    private final IndexMetaDataCache indexMetaDataCache;
    private final boolean ignoreNewerMutations;
    private final boolean isImmutable;
    
    private static IndexMetaDataCache getIndexMetaData(RegionCoprocessorEnvironment env, Map<String, byte[]> attributes) throws IOException {
        if (attributes == null) { return IndexMetaDataCache.EMPTY_INDEX_META_DATA_CACHE; }
        byte[] uuid = attributes.get(PhoenixIndexCodec.INDEX_UUID);
        if (uuid == null) { return IndexMetaDataCache.EMPTY_INDEX_META_DATA_CACHE; }
        boolean useProto = false;
        byte[] md = attributes.get(PhoenixIndexCodec.INDEX_PROTO_MD);
        useProto = md != null;
        if (md == null) {
            md = attributes.get(PhoenixIndexCodec.INDEX_MD);
        }
        byte[] txState = attributes.get(BaseScannerRegionObserver.TX_STATE);
        if (md != null) {
            final List<IndexMaintainer> indexMaintainers = IndexMaintainer.deserialize(md, useProto);
            final Transaction txn = MutationState.decodeTransaction(txState);
            return new IndexMetaDataCache() {

                @Override
                public void close() throws IOException {}

                @Override
                public List<IndexMaintainer> getIndexMaintainers() {
                    return indexMaintainers;
                }

                @Override
                public Transaction getTransaction() {
                    return txn;
                }

            };
        } else {
            byte[] tenantIdBytes = attributes.get(PhoenixRuntime.TENANT_ID_ATTRIB);
            ImmutableBytesPtr tenantId = tenantIdBytes == null ? null : new ImmutableBytesPtr(tenantIdBytes);
            TenantCache cache = GlobalCache.getTenantCache(env, tenantId);
            IndexMetaDataCache indexCache = (IndexMetaDataCache)cache.getServerCache(new ImmutableBytesPtr(uuid));
            if (indexCache == null) {
                String msg = "key=" + ServerCacheClient.idToString(uuid) + " region=" + env.getRegion() + "host="
                        + env.getRegionServerServices().getServerName();
                SQLException e = new SQLExceptionInfo.Builder(SQLExceptionCode.INDEX_METADATA_NOT_FOUND).setMessage(msg)
                        .build().buildException();
                ServerUtil.throwIOException("Index update failed", e); // will not return
            }
            return indexCache;
        }

    }

    public PhoenixIndexMetaData(RegionCoprocessorEnvironment env, Map<String,byte[]> attributes) throws IOException {
        this.indexMetaDataCache = getIndexMetaData(env, attributes);
        boolean isImmutable = true;
        for (IndexMaintainer maintainer : indexMetaDataCache.getIndexMaintainers()) {
            isImmutable &= maintainer.isImmutableRows();
        }
        this.isImmutable = isImmutable;
        this.attributes = attributes;
        this.ignoreNewerMutations = attributes.get(BaseScannerRegionObserver.IGNORE_NEWER_MUTATIONS) != null;
    }
    
    public Transaction getTransaction() {
        return indexMetaDataCache.getTransaction();
    }
    
    public List<IndexMaintainer> getIndexMaintainers() {
        return indexMetaDataCache.getIndexMaintainers();
    }

    public Map<String, byte[]> getAttributes() {
        return attributes;
    }
    
    public boolean ignoreNewerMutations() {
        return ignoreNewerMutations;
    }

    @Override
    public boolean isImmutableRows() {
        return isImmutable;
    }
}
