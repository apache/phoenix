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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PrefixByteEncoder;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

/*
 * Builder to help in adding guidePosts and building guidePostInfo. This is used when we are collecting stats or reading stats for a table.
 */

public class GuidePostsInfoBuilder {
    private PrefixByteEncoder encoder;
    private ImmutableBytesWritable lastRow;
    private ImmutableBytesWritable guidePosts=new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
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
    private List<Long> rowCounts = new ArrayList<Long>();
    private List<Long> byteCounts = new ArrayList<Long>();

    public boolean isEmpty() {
        return rowCounts.size() == 0;
    }
    
    public List<Long> getRowCounts() {
        return rowCounts;
    }

    public List<Long> getByteCounts() {
        return byteCounts;
    }

    public int getMaxLength() {
        return maxLength;
    }
    public GuidePostsInfoBuilder(){
        this.stream = new TrustedByteArrayOutputStream(1);
        this.output = new DataOutputStream(stream);
        this.encoder=new PrefixByteEncoder();
        lastRow = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
    }

    /**
     * The guide posts, rowCount and byteCount are accumulated every time a guidePosts depth is
     * reached while collecting stats.
     * @param row
     * @param byteCount
     * @return
     * @throws IOException 
     */
    public boolean addGuidePosts(ImmutableBytesWritable row, long byteCount, long rowCount) {
        if (row.getLength() != 0 && lastRow.compareTo(row) < 0) {
            try {
                encoder.encode(output, row.get(), row.getOffset(), row.getLength());
                rowCounts.add(rowCount);
                byteCounts.add(byteCount);
                this.guidePostsCount++;
                this.maxLength = encoder.getMaxLength();
                lastRow = row;
                return true;
            } catch (IOException e) {
                return false;
            }
        }
        return false;
    }

    public boolean addGuidePosts(byte[] row) {
        return addGuidePosts(new ImmutableBytesWritable(row), 0, 0);
    }

    public boolean addGuidePosts(byte[] row, long byteCount) {
        return addGuidePosts(new ImmutableBytesWritable(row), byteCount, 0);
    }

    public boolean addGuidePosts(byte[] row, long byteCount, long rowCount) {
        return addGuidePosts(new ImmutableBytesWritable(row), byteCount, rowCount);
    }

    public GuidePostsInfo build() {
        this.guidePosts.set(stream.getBuffer(), 0, stream.size());
        GuidePostsInfo guidePostsInfo = new GuidePostsInfo(this.byteCounts, this.guidePosts, this.rowCounts,
                this.maxLength, this.guidePostsCount);
        return guidePostsInfo;
    }

    public void incrementRowCount() {
        this.rowCount++;
    }

    public void resetRowCount() {
        this.rowCount = 0;
    }

    public long getRowCount() {
        return rowCount;
    }

}