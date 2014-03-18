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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.jcip.annotations.Immutable;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.cache.HashCache;
import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ResultUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.TupleUtil;
import org.xerial.snappy.Snappy;

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
            int size = Snappy.uncompressedLength(cachePtr.get());
            byte[] uncompressed = new byte[size];
            Snappy.uncompress(cachePtr.get(), 0, cachePtr.getLength(), uncompressed, 0);
            return new HashCacheImpl(uncompressed, chunk);
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    @Immutable
    private class HashCacheImpl implements HashCache {
        private final Map<ImmutableBytesPtr,List<Tuple>> hashCache;
        private final MemoryChunk memoryChunk;
        
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
                int exprSize = dataInput.readInt();
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
        public List<Tuple> get(ImmutableBytesPtr hashKey) {
            return hashCache.get(hashKey);
        }
    }
}

