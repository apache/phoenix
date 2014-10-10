package org.apache.phoenix.schema.stats;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
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
/**
 *  A simple POJO class that holds the information related to GuidePosts serDe.
 */
public class GuidePostsInfo {
    private long byteCount; // Number of bytes traversed in the region
    private long keyByteSize; // Total number of bytes in keys stored in guidePosts
    private List<byte[]> guidePosts;

    public GuidePostsInfo(long byteCount, List<byte[]> guidePosts) {
        this.byteCount = byteCount;
        this.guidePosts = ImmutableList.copyOf(guidePosts);
        int size = 0;
        for (byte[] key : guidePosts) {
            size += key.length;
        }
        this.keyByteSize = size;
    }

    public boolean addGuidePost(byte[] row, long byteCount) {
        if (guidePosts.isEmpty() || Bytes.compareTo(row, guidePosts.get(guidePosts.size() - 1)) > 0) {
            List<byte[]> newGuidePosts = Lists.newArrayListWithExpectedSize(this.getGuidePosts().size() + 1);
            newGuidePosts.addAll(guidePosts);
            newGuidePosts.add(row);
            this.guidePosts = ImmutableList.copyOf(newGuidePosts);
            this.byteCount += byteCount;
            this.keyByteSize += row.length;
            return true;
        }
        return false;
    }
    
    public void combine(GuidePostsInfo oldInfo) {
        // FIXME: I don't think we need to do a merge sort here, as the keys won't be interleaved.
        // We just need to concatenate them in the correct way.
        this.guidePosts = ImmutableList.copyOf(Iterators.mergeSorted(ImmutableList.of(this.getGuidePosts().iterator(), oldInfo.getGuidePosts().iterator()), Bytes.BYTES_COMPARATOR));
        this.byteCount += oldInfo.getByteCount();
        this.keyByteSize += oldInfo.keyByteSize;
    }
    
    public long getByteCount() {
        return byteCount;
    }

    public List<byte[]> getGuidePosts() {
        return guidePosts;
    }

    public static GuidePostsInfo fromBytes(byte[] buf, int offset, int l) {
        try {
            ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf, offset, l);
            try {
                DataInputStream in = new DataInputStream(bytesIn);
                try {
                    long byteCount = in.readLong();
                    int guidepostsCount = in.readInt();
                    List<byte[]> guidePosts = Lists.newArrayListWithExpectedSize(guidepostsCount);
                    if (guidepostsCount > 0) {
                        for (int i = 0; i < guidepostsCount; i++) {
                            int length = WritableUtils.readVInt(in);
                            byte[] gp = new byte[length];
                            in.read(gp);
                            if (gp.length != 0) {
                                guidePosts.add(gp);
                            }
                        }
                    }
                    return new GuidePostsInfo(byteCount, guidePosts);
                } catch (IOException e) {
                    throw new RuntimeException(e); // not possible
                } finally {
                    try {
                        in.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e); // not possible
                    }
                }
            } finally {
                bytesIn.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // not possible
        }
    }
    
    public byte[] toBytes() {
        int size = guidePosts.size();
        // Serialize the number of bytes traversed, number of key bytes in the region,
        // number of guideposts for that family, <<guidepostSize><guidePostsArray>,<guidePostsSize> <guidePostArray>>
        // We will lose precision here?
        TrustedByteArrayOutputStream bs = new TrustedByteArrayOutputStream(
                (int)(Bytes.SIZEOF_LONG + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT + this.keyByteSize + (WritableUtils
                        .getVIntSize(size) * size)));
        DataOutputStream os = new DataOutputStream(bs);
        try {
            os.writeLong(this.getByteCount());
            os.writeInt(size);
            for (byte[] element : guidePosts) {
                WritableUtils.writeVInt(os, element.length);
                os.write(element);
            }
            return bs.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe); // not possible
        } finally {
            try {
                os.close();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe); // not possible
            }
        }
    }
}