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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
/**
 *  A class that holds the guidePosts of a region and also allows combining the 
 *  guidePosts of different regions when the GuidePostsInfo is formed for a table.
 */
public class GuidePostsInfo {

    /**
     * the total number of guidePosts for the table combining all the guidePosts per region per cf.
     */
    private List<byte[]> guidePosts;
    /**
     * The bytecount that is flattened across the total number of guide posts.
     */
    private long byteCount = 0;
    
    /**
     * The rowCount that is flattened across the total number of guide posts.
     */
    private long rowCount = 0;
    
    private long keyByteSize; // Total number of bytes in keys stored in guidePosts

    /**
     * Constructor that creates GuidePostsInfo per region
     * @param byteCount
     * @param guidePosts
     * @param rowCount
     */
    public GuidePostsInfo(long byteCount, List<byte[]> guidePosts, long rowCount) {
        this.guidePosts = ImmutableList.copyOf(guidePosts);
        int size = 0;
        for (byte[] key : guidePosts) {
            size += key.length;
        }
        this.keyByteSize = size;
        this.byteCount = byteCount;
        this.rowCount = rowCount;
    }
    
    public long getByteCount() {
        return byteCount;
    }

    public List<byte[]> getGuidePosts() {
        return guidePosts;
    }

    public long getRowCount() {
        return this.rowCount;
    }
    
    public void incrementRowCount() {
        this.rowCount++;
    }
    
    /**
     * Combines the GuidePosts per region into one.
     * @param oldInfo
     */
    public void combine(GuidePostsInfo oldInfo) {
        if (!oldInfo.getGuidePosts().isEmpty()) {
            byte[] newFirstKey = oldInfo.getGuidePosts().get(0);
            byte[] existingLastKey;
            if (!this.getGuidePosts().isEmpty()) {
                existingLastKey = this.getGuidePosts().get(this.getGuidePosts().size() - 1);
            } else {
                existingLastKey = HConstants.EMPTY_BYTE_ARRAY;
            }
            int size = oldInfo.getGuidePosts().size();
            // If the existing guidePosts is lesser than the new RegionInfo that we are combining
            // then add the new Region info to the end of the current GuidePosts.
            // If the new region info is smaller than the existing guideposts then add the existing
            // guide posts after the new guideposts.
            List<byte[]> newTotalGuidePosts = new ArrayList<byte[]>(this.getGuidePosts().size() + size);
            if (Bytes.compareTo(existingLastKey, newFirstKey) <= 0) {
                newTotalGuidePosts.addAll(this.getGuidePosts());
                newTotalGuidePosts.addAll(oldInfo.getGuidePosts());
            } else {
                newTotalGuidePosts.addAll(oldInfo.getGuidePosts());
                newTotalGuidePosts.addAll(this.getGuidePosts());
            }
            this.guidePosts = ImmutableList.copyOf(newTotalGuidePosts);
        }
        this.byteCount += oldInfo.getByteCount();
        this.keyByteSize += oldInfo.keyByteSize;
        this.rowCount += oldInfo.getRowCount();
    }
    
    /**
     * The guide posts, rowCount and byteCount are accumulated every time a guidePosts depth is
     * reached while collecting stats.
     * @param row
     * @param byteCount
     * @param rowCount
     * @return
     */
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
    
    /**
     * Deserializes the per row guidePosts info from the value part of each cell in the SYSTEM.STATS table
     * @param buf
     * @param offset
     * @param l
     * @return
     */
    public static GuidePostsInfo deserializeGuidePostsInfo(byte[] buf, int offset, int l, long rowCount) {
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
                    return new GuidePostsInfo(byteCount, guidePosts, rowCount);
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

    /**
     * Serailizes the guidePosts info as value in the SYSTEM.STATS table.
     * <br>
     * The format is,
     * <br>
     *  - number of bytes traversed
     * <br>
     *  - number of key bytes in the region
     * <br>
     *  - number of guideposts for that family
     * <br> u
     *  - [guidepostSize][guidePostsArray],[guidePostsSize][guidePostArray]
     * @return the byte[] to be serialized in the cell
     */
    public byte[] serializeGuidePostsInfo() {
        int size = guidePosts.size();
        // We will lose precision here?
        TrustedByteArrayOutputStream bs = new TrustedByteArrayOutputStream((int)(Bytes.SIZEOF_LONG + Bytes.SIZEOF_LONG
                + Bytes.SIZEOF_INT + this.keyByteSize + (WritableUtils.getVIntSize(size) * size)));
        DataOutputStream os = new DataOutputStream(bs);
        try {
            os.writeLong(byteCount);
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