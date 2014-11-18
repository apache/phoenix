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

import java.io.*;
import java.sql.SQLException;
import java.util.*;

import net.jcip.annotations.Immutable;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import org.apache.phoenix.cache.HashCache;
import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.*;

import org.iq80.snappy.CorruptionException;
import org.iq80.snappy.Snappy;

public class HashCacheFactory implements ServerCacheFactory {

    public HashCacheFactory() {
    }

    @Override
    public void readFields(DataInput input) throws IOException {
    }

    @Override
    public void write(DataOutput output) throws IOException {
    }

    @Override
    public Closeable newCache(ImmutableBytesWritable cachePtr, MemoryChunk chunk) throws SQLException {
        try {
            // This reads the uncompressed length from the front of the compressed input
            int uncompressedLen = Snappy.getUncompressedLength(cachePtr.get(), cachePtr.getOffset());
            byte[] uncompressed = new byte[uncompressedLen];
            Snappy.uncompress(cachePtr.get(), cachePtr.getOffset(), cachePtr.getLength(),
                uncompressed, 0);
            return new HashCacheImpl(uncompressed, chunk);
        } catch (CorruptionException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    @Immutable
    private class HashCacheImpl implements HashCache {
        private final Map<ImmutableBytesPtr,List<Tuple>> hashCache;
        private final MemoryChunk memoryChunk;
        private final boolean singleValueOnly;
        
        private HashCacheImpl(byte[] hashCacheBytes, MemoryChunk memoryChunk) {
            try {
                this.memoryChunk = memoryChunk;
                byte[] hashCacheByteArray = hashCacheBytes;
                int offset = 0;
                ByteArrayInputStream input = new ByteArrayInputStream(hashCacheByteArray, offset, hashCacheBytes.length);
                DataInputStream dataInput = new DataInputStream(input);
                int nExprs = dataInput.readInt();
                List<Expression> onExpressions = new ArrayList<Expression>(nExprs);
                for (int i = 0; i < nExprs; i++) {
                    int expressionOrdinal = WritableUtils.readVInt(dataInput);
                    Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
                    expression.readFields(dataInput);
                    onExpressions.add(expression);                        
                }
                boolean singleValueOnly = false;
                int exprSizeAndSingleValueOnly = dataInput.readInt();
                int exprSize = exprSizeAndSingleValueOnly;
                if (exprSize < 0) {
                    exprSize *= -1;
                    singleValueOnly = true;
                }
                this.singleValueOnly = singleValueOnly;
                offset += exprSize;
                int nRows = dataInput.readInt();
                long estimatedSize = SizedUtil.sizeOfMap(nRows, SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE, SizedUtil.RESULT_SIZE) + hashCacheBytes.length;
                this.memoryChunk.resize(estimatedSize);
                HashMap<ImmutableBytesPtr,List<Tuple>> hashCacheMap = new HashMap<ImmutableBytesPtr,List<Tuple>>(nRows * 5 / 4);
                offset += Bytes.SIZEOF_INT;
                // Build Map with evaluated hash key as key and row as value
                for (int i = 0; i < nRows; i++) {
                    int resultSize = (int)Bytes.readVLong(hashCacheByteArray, offset);
                    offset += WritableUtils.decodeVIntSize(hashCacheByteArray[offset]);
                    ImmutableBytesWritable value = new ImmutableBytesWritable(hashCacheByteArray,offset,resultSize);
                    Tuple result = new ResultTuple(ResultUtil.toResult(value));
                    ImmutableBytesPtr key = TupleUtil.getConcatenatedValue(result, onExpressions);
                    List<Tuple> tuples = hashCacheMap.get(key);
                    if (tuples == null) {
                        tuples = new LinkedList<Tuple>();
                        hashCacheMap.put(key, tuples);
                    }
                    tuples.add(result);
                    offset += resultSize;
                }
                this.hashCache = Collections.unmodifiableMap(hashCacheMap);
            } catch (IOException e) { // Not possible with ByteArrayInputStream
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            memoryChunk.close();
        }
        
        @Override
        public List<Tuple> get(ImmutableBytesPtr hashKey) throws IOException {
            List<Tuple> ret = hashCache.get(hashKey);
            if (singleValueOnly && ret != null && ret.size() > 1) {
                SQLException ex = new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS).build().buildException();
                ServerUtil.throwIOException(ex.getMessage(), ex);
            }
            
            return ret;
        }
    }
}

