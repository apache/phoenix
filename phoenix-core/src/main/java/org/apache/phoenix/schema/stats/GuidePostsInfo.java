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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.CodecUtils;
import org.apache.phoenix.util.PrefixByteEncoder;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;
/**
 *  A class that holds the guidePosts of a region and also allows combining the 
 *  guidePosts of different regions when the GuidePostsInfo is formed for a table.
 */
public class GuidePostsInfo {

    /**
     * the total number of guidePosts for the table combining all the guidePosts per region per cf.
     */
    private ImmutableBytesWritable guidePosts;
    /**
     * The bytecount that is flattened across the total number of guide posts.
     */
    private long byteCount = 0;
    
    /**
     * The rowCount that is flattened across the total number of guide posts.
     */
    private long rowCount = 0;
    
    /**
     * Maximum length of a guidePost collected
     */
    private int maxLength;
    
    public final static GuidePostsInfo EMPTY_GUIDEPOST = new GuidePostsInfo(0,
            new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY), 0, 0, 0);

    public int getMaxLength() {
        return maxLength;
    }

    private TrustedByteArrayOutputStream stream;
    private PrefixByteEncoder encoder;
    private DataOutputStream output;
    private byte[] lastRow;
    private int guidePostsCount;
    private boolean isStreamInitialized; 

    /**
     * Constructor that creates GuidePostsInfo per region
     * @param byteCount
     * @param guidePosts
     * @param rowCount
     */
    public GuidePostsInfo(long byteCount, ImmutableBytesWritable guidePosts, long rowCount, int maxLength, int guidePostsCount) {
        this.guidePosts = new ImmutableBytesWritable(guidePosts);
        this.maxLength = maxLength;
        this.byteCount = byteCount;
        this.rowCount = rowCount;
        this.guidePostsCount = guidePostsCount;
        lastRow = ByteUtil.EMPTY_BYTE_ARRAY;
    }
    
    
    public long getByteCount() {
        return byteCount;
    }

    public ImmutableBytesWritable getGuidePosts() {
        return guidePosts;
    }

    public long getRowCount() {
        return this.rowCount;
    }
    
    public void incrementRowCount() {
        this.rowCount++;
    }
    
    public int getGuidePostsCount() {
        return guidePostsCount;
    }

    /**
     * The guide posts, rowCount and byteCount are accumulated every time a guidePosts depth is
     * reached while collecting stats.
     * @param row
     * @param byteCount
     * @return
     * @throws IOException 
     */
    public boolean encodeAndCollectGuidePost(byte[] row, long byteCount, long rowCount) {
        if (row.length != 0 && Bytes.compareTo(lastRow, row) < 0) {
            try {
                if(!isStreamInitialized){
                    stream = new TrustedByteArrayOutputStream(guidePosts.getLength());
                    output = new DataOutputStream(stream);
                    stream.write(ByteUtil.copyKeyBytesIfNecessary(guidePosts));
                    encoder = new PrefixByteEncoder();
                    isStreamInitialized=true;
                }
                encoder.encode(output, row, 0, row.length);
                this.byteCount += byteCount;
                this.guidePostsCount++;
                this.maxLength = encoder.getMaxLength();
                this.rowCount += rowCount;
                lastRow = row;
                return true;
            } catch (IOException e) {
                return false;
            }
        }
        return false;
    }
  
    public boolean encodeAndCollectGuidePost(byte[] row){
        return encodeAndCollectGuidePost(row, 0, 0);
    }

    public boolean encodeAndCollectGuidePost(byte[] row, long byteCount){
        return encodeAndCollectGuidePost(row, byteCount, 0);
    }

    public void close() {
        CodecUtils.close(stream);
    }
    
    /*
     * This needs to be collect once all stats are encoded and stream buffer needs to be copied for retrieval before close()
     */
    public void updateGuidePosts() {
        if (stream != null) {
            this.guidePosts.set(Bytes.copy(stream.getBuffer(), 0, stream.size()));
        }
    }
    
    @Override
    public String toString() {
        return Bytes.toString(this.guidePosts.get());
    }
}