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

package org.apache.phoenix.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.util.ScanUtil;

public interface IndexMetaDataCache extends Closeable {
    public static final IndexMetaDataCache EMPTY_INDEX_META_DATA_CACHE = new IndexMetaDataCache() {

        @Override
        public void close() throws IOException {
        }

        @Override
        public List<IndexMaintainer> getIndexMaintainers() {
            return Collections.emptyList();
        }

        @Override
        public PhoenixTransactionContext getTransactionContext() {
            return null;
        }
        
        @Override
        public int getClientVersion() {
            return ScanUtil.UNKNOWN_CLIENT_VERSION;
        }
        
    };
    public List<IndexMaintainer> getIndexMaintainers();
    public PhoenixTransactionContext getTransactionContext();
    public int getClientVersion();
}
