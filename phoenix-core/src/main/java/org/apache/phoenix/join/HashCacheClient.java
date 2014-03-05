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
package org.apache.phoenix.join;

import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;
import org.apache.phoenix.util.TupleUtil;
import org.xerial.snappy.Snappy;

/**
 * 
 * Client for adding cache of one side of a join to region servers
 * 
 * 
 * @since 0.1
 */
public class HashCacheClient  {
    private final ServerCacheClient serverCache;
    /**
     * Construct client used to create a serialized cached snapshot of a table and send it to each region server
     * for caching during hash join processing.
     * @param connection the client connection
     */
    public HashCacheClient(PhoenixConnection connection) {
        serverCache = new ServerCacheClient(connection);
    }

    /**
     * Send the results of scanning through the scanner to all
     * region servers for regions of the table that will use the cache
     * that intersect with the minMaxKeyRange.
     * @param scanner scanner for the table or intermediate results being cached
     * @return client-side {@link ServerCache} representing the added hash cache
     * @throws SQLException 
     * @throws MaxServerCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public ServerCache addHashCache(ScanRanges keyRanges, ResultIterator iterator, long estimatedSize, List<Expression> onExpressions, TableRef cacheUsingTableRef) throws SQLException {
        /**
         * Serialize and compress hashCacheTable
         */
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        serialize(ptr, iterator, estimatedSize, onExpressions);
        return serverCache.addServerCache(keyRanges, ptr, new HashCacheFactory(), cacheUsingTableRef);
    }
    
    private void serialize(ImmutableBytesWritable ptr, ResultIterator iterator, long estimatedSize, List<Expression> onExpressions) throws SQLException {
        long maxSize = serverCache.getConnection().getQueryServices().getProps().getLong(QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_SIZE);
        estimatedSize = Math.min(estimatedSize, maxSize);
        if (estimatedSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("Estimated size(" + estimatedSize + ") must not be greater than Integer.MAX_VALUE(" + Integer.MAX_VALUE + ")");
        }
        try {
            TrustedByteArrayOutputStream baOut = new TrustedByteArrayOutputStream((int)estimatedSize);
            DataOutputStream out = new DataOutputStream(baOut);
            // Write onExpressions first, for hash key evaluation along with deserialization
            out.writeInt(onExpressions.size());
            for (Expression expression : onExpressions) {
                WritableUtils.writeVInt(out, ExpressionType.valueOf(expression).ordinal());
                expression.write(out);                
            }
            int exprSize = baOut.size() + Bytes.SIZEOF_INT;
            out.writeInt(exprSize);
            int nRows = 0;
            out.writeInt(nRows); // In the end will be replaced with total number of rows            
            for (Tuple result = iterator.next(); result != null; result = iterator.next()) {
                TupleUtil.write(result, out);
                if (baOut.size() > maxSize) {
                    throw new MaxServerCacheSizeExceededException("Size of hash cache (" + baOut.size() + " bytes) exceeds the maximum allowed size (" + maxSize + " bytes)");
                }
                nRows++;
            }
            TrustedByteArrayOutputStream sizeOut = new TrustedByteArrayOutputStream(Bytes.SIZEOF_INT);
            DataOutputStream dataOut = new DataOutputStream(sizeOut);
            try {
                dataOut.writeInt(nRows);
                dataOut.flush();
                byte[] cache = baOut.getBuffer();
                // Replace number of rows written above with the correct value.
                System.arraycopy(sizeOut.getBuffer(), 0, cache, exprSize, sizeOut.size());
                // Reallocate to actual size plus compressed buffer size (which is allocated below)
                int maxCompressedSize = Snappy.maxCompressedLength(baOut.size());
                byte[] compressed = new byte[maxCompressedSize]; // size for worst case
                int compressedSize = Snappy.compress(baOut.getBuffer(), 0, baOut.size(), compressed, 0);
                // Last realloc to size of compressed buffer.
                ptr.set(compressed,0,compressedSize);
            } finally {
                dataOut.close();
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            iterator.close();
        }
    }
}
