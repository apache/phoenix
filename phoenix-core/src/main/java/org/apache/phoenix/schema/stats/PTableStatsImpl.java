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
package org.apache.phoenix.schema.stats;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.PrefixByteCodec;
import org.apache.phoenix.util.PrefixByteDecoder;
import org.apache.phoenix.util.SizedUtil;

import com.sun.istack.NotNull;
 
 /**
 * Implementation for PTableStats.
 */
public class PTableStatsImpl implements PTableStats {
    private final SortedMap<byte[], GuidePostsInfo> guidePosts;
    private final int estimatedSize;
    private final long timeStamp;

    public PTableStatsImpl() {
        this(new TreeMap<byte[], GuidePostsInfo>(Bytes.BYTES_COMPARATOR), MetaDataProtocol.MIN_TABLE_TIMESTAMP);
    }

    public PTableStatsImpl(@NotNull SortedMap<byte[], GuidePostsInfo> guidePosts, long timeStamp) {
        this.guidePosts = guidePosts;
        this.timeStamp = timeStamp;
        int estimatedSize = SizedUtil.OBJECT_SIZE + SizedUtil.INT_SIZE + SizedUtil.sizeOfTreeMap(guidePosts.size());
        for (Map.Entry<byte[], GuidePostsInfo> entry : guidePosts.entrySet()) {
            byte[] cf = entry.getKey();
            estimatedSize += SizedUtil.ARRAY_SIZE + cf.length;
            estimatedSize += entry.getValue().getGuidePosts().getLength();
            estimatedSize += SizedUtil.LONG_SIZE;
            estimatedSize += SizedUtil.INT_SIZE;
            estimatedSize += SizedUtil.INT_SIZE;
        }
        this.estimatedSize = estimatedSize;
    }

    @Override
    public SortedMap<byte[], GuidePostsInfo> getGuidePosts() {
        return guidePosts;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("PTableStats [");
        for (Map.Entry<byte[], GuidePostsInfo> entry : guidePosts.entrySet()) {
            buf.append(Bytes.toStringBinary(entry.getKey()));
            buf.append(":(");
            ImmutableBytesWritable keys = entry.getValue().getGuidePosts();
            ByteArrayInputStream stream = new ByteArrayInputStream(keys.get(), keys.getOffset(), keys.getLength());
            try {
                if (keys.getLength() != 0) {
                    DataInput input = new DataInputStream(stream);
                    PrefixByteDecoder decoder = new PrefixByteDecoder(entry.getValue().getMaxLength());
                    try {
                        while (true) {
                            ImmutableBytesWritable ptr = PrefixByteCodec.decode(decoder, input);
                            buf.append(Bytes.toStringBinary(ptr.get()));
                            buf.append(",");
                        }
                    } catch (EOFException e) { // Ignore as this signifies we're done

                    } finally {
                        Closeables.closeQuietly(stream);
                    }
                    buf.setLength(buf.length() - 1);
                }
                buf.append(")");
            } finally {
                Closeables.closeQuietly(stream);
            }
        }
        buf.append("]");
        return buf.toString();
    }

    @Override
    public int getEstimatedSize() {
        return estimatedSize;
    }

    @Override
    public long getTimestamp() {
        return timeStamp;
    }
}
