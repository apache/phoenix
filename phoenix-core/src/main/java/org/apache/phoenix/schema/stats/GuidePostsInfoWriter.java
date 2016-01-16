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

/*
 * Writer to help in writing guidePosts and creating guidePostInfo. This is used when we are collecting stats or reading stats for a table.
 * updateGuidePosts() is required to be invoked before calling getGuidePostInfo() in order to get the updated guidePosts
 */

public class GuidePostsInfoWriter {
    private PrefixByteEncoder encoder;
    private byte[] lastRow;
    private GuidePostsInfo guidePostInfo;
    private long byteCount = 0;
    private int guidePostsCount;
    
    /**
     * The rowCount that is flattened across the total number of guide posts.
     */
    private long rowCount = 0;
    
    /**
     * Maximum length of a guidePost collected
     */
    private int maxLength;
    private DataOutputStream output;
    private TrustedByteArrayOutputStream stream;
    private ImmutableBytesWritable guidePosts=new ImmutableBytesWritable();
    
    public final static GuidePostsInfo EMPTY_GUIDEPOST = new GuidePostsInfo(0,
            new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY), 0, 0, 0);

    public int getMaxLength() {
        return maxLength;
    }
    public GuidePostsInfoWriter(){
        this.stream = new TrustedByteArrayOutputStream(1);
        this.output = new DataOutputStream(stream);
        this.encoder=new PrefixByteEncoder();
        lastRow = ByteUtil.EMPTY_BYTE_ARRAY;
    }

    /**
     * The guide posts, rowCount and byteCount are accumulated every time a guidePosts depth is
     * reached while collecting stats.
     * @param row
     * @param byteCount
     * @return
     * @throws IOException 
     */
    public boolean writeGuidePosts( byte[] row, long byteCount, long rowCount) {
        if (row.length != 0 && Bytes.compareTo(lastRow, row) < 0) {
            try {
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
    
    public boolean writeGuidePosts(byte[] row){
        return writeGuidePosts(row, 0, 0);
    }

    public boolean writeGuidePosts(byte[] row, long byteCount){
        return writeGuidePosts(row, byteCount, 0);
    }
    
    /*
     * This needs to be called once all stats are encoded and stream buffer needs to be copied for retrieval before
     * close()
     */
    public void updateGuidePosts() {
        this.guidePosts.set(Bytes.copy(stream.getBuffer(), 0, stream.size()));
        this.guidePostInfo = new GuidePostsInfo(this.byteCount, this.guidePosts, this.rowCount, this.maxLength,
                this.guidePostsCount);
    }

    public GuidePostsInfo getGuidePostInfo() {
        return this.guidePostInfo;
    }

    public void close() {
        CodecUtils.close(stream);
    }

    public void incrementRowCount() {
        this.rowCount++;
    }
}
