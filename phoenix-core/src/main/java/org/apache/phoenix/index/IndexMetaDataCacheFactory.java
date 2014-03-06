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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.cache.IndexMetaDataCache;
import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;

public class IndexMetaDataCacheFactory implements ServerCacheFactory {
    public IndexMetaDataCacheFactory() {
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
    }

    @Override
    public Closeable newCache (ImmutableBytesWritable cachePtr, final MemoryChunk chunk) throws SQLException {
        // just use the standard keyvalue builder - this doesn't really need to be fast
        final List<IndexMaintainer> maintainers =
                IndexMaintainer.deserialize(cachePtr, GenericKeyValueBuilder.INSTANCE);
        return new IndexMetaDataCache() {

            @Override
            public void close() throws IOException {
                chunk.close();
            }

            @Override
            public List<IndexMaintainer> getIndexMaintainers() {
                return maintainers;
            }
        };
    }
}
