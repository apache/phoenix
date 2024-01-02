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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.cache.IndexMetaDataCache;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.ReplayWrite;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.schema.transform.TransformMaintainer;
import org.apache.phoenix.transaction.PhoenixTransactionContext;

public class PhoenixIndexMetaData implements IndexMetaData {
    private final Map<String, byte[]> attributes;
    private final IndexMetaDataCache indexMetaDataCache;
    private final ReplayWrite replayWrite;
    private final boolean isImmutable;
    private final boolean hasNonPkColumns;
    private final boolean hasLocalIndexes;
    
    public static boolean isIndexRebuild(Map<String,byte[]> attributes) {
        return attributes.get(BaseScannerRegionObserverConstants.REPLAY_WRITES)
                == BaseScannerRegionObserverConstants.REPLAY_INDEX_REBUILD_WRITES;
    }
    
    public static ReplayWrite getReplayWrite(Map<String,byte[]> attributes) {
        return ReplayWrite.fromBytes(attributes.get(BaseScannerRegionObserverConstants.REPLAY_WRITES));
    }
    
    public PhoenixIndexMetaData(IndexMetaDataCache indexMetaDataCache, Map<String, byte[]> attributes) throws IOException {
        this.indexMetaDataCache = indexMetaDataCache;
        boolean isImmutable = true;
        boolean hasNonPkColumns = false;
        boolean hasLocalIndexes = false;
        for (IndexMaintainer maintainer : indexMetaDataCache.getIndexMaintainers()) {
            isImmutable &= maintainer.isImmutableRows();
            if (!(maintainer instanceof TransformMaintainer)) {
                hasNonPkColumns |= !maintainer.getIndexedColumns().isEmpty();
            }
            hasLocalIndexes |= maintainer.isLocalIndex();
        }
        this.isImmutable = isImmutable;
        this.hasNonPkColumns = hasNonPkColumns;
        this.attributes = attributes;
        this.replayWrite = getReplayWrite(attributes);
        this.hasLocalIndexes = hasLocalIndexes;
    }
    
    public PhoenixTransactionContext getTransactionContext() {
        return indexMetaDataCache.getTransactionContext();
    }
    
    public List<IndexMaintainer> getIndexMaintainers() {
        return indexMetaDataCache.getIndexMaintainers();
    }

    public Map<String, byte[]> getAttributes() {
        return attributes;
    }
    
    public int getClientVersion() {
        return indexMetaDataCache.getClientVersion();
    }
    
    @Override
    public ReplayWrite getReplayWrite() {
        return replayWrite;
    }
    
    public boolean isImmutableRows() {
        return isImmutable;
    }
    
    public boolean hasLocalIndexes() {
        return hasLocalIndexes;
    }

    @Override
    public boolean requiresPriorRowState(Mutation m) {
        return !isImmutable || (indexMetaDataCache.getIndexMaintainers().get(0).isRowDeleted(m) && hasNonPkColumns);
    }
}
