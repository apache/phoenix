/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.phoenix.index.IndexMaintainer;

/**
 * A facade for a {@link org.apache.hadoop.hbase.io.hfile.HFile.Reader} that serves up either the
 * top or bottom half of a HFile where 'bottom' is the first half of the file containing the keys
 * that sort lowest and 'top' is the second half of the file with keys that sort greater than those
 * of the bottom half. The top includes the split files midkey, of the key that follows if it does
 * not exist in the file.
 *
 * <p>
 * This type works in tandem with the {@link Reference} type. This class is used reading while
 * Reference is used writing.
 *
 * <p>
 * This file is not splitable. Calls to {@link #midkey()} return null.
 */

public class IndexHalfStoreFileReader extends StoreFile.Reader {
    private final boolean top;
    // This is the key we split around. Its the first possible entry on a row:
    // i.e. empty column and a timestamp of LATEST_TIMESTAMP.
    private final byte[] splitkey;
    private final byte[] splitRow;
    private final Map<ImmutableBytesWritable, IndexMaintainer> indexMaintainers;
    private final byte[][] viewConstants;
    private final int offset;
    private final HRegionInfo regionInfo;
    private final byte[] regionStartKeyInHFile;

    /**
     * @param fs
     * @param p
     * @param cacheConf
     * @param in
     * @param size
     * @param r
     * @param conf
     * @param indexMaintainers
     * @param viewConstants
     * @param regionInfo
     * @param regionStartKeyInHFile
     * @param splitKey
     * @throws IOException
     */
    public IndexHalfStoreFileReader(final FileSystem fs, final Path p, final CacheConfig cacheConf,
            final FSDataInputStreamWrapper in, long size, final Reference r,
            final Configuration conf,
            final Map<ImmutableBytesWritable, IndexMaintainer> indexMaintainers,
            final byte[][] viewConstants, final HRegionInfo regionInfo,
            byte[] regionStartKeyInHFile, byte[] splitKey) throws IOException {
        super(fs, p, in, size, cacheConf, conf);
        this.splitkey = splitKey == null ? r.getSplitKey() : splitKey;
        // Is it top or bottom half?
        this.top = Reference.isTopFileRegion(r.getFileRegion());
        this.splitRow = CellUtil.cloneRow(KeyValue.createKeyValueFromKey(splitkey));
        this.indexMaintainers = indexMaintainers;
        this.viewConstants = viewConstants;
        this.regionInfo = regionInfo;
        this.regionStartKeyInHFile = regionStartKeyInHFile;
        this.offset = regionStartKeyInHFile.length;
    }

    public int getOffset() {
        return offset;
    }

    public byte[][] getViewConstants() {
        return viewConstants;
    }

    public Map<ImmutableBytesWritable, IndexMaintainer> getIndexMaintainers() {
        return indexMaintainers;
    }

    public HRegionInfo getRegionInfo() {
        return regionInfo;
    }

    public byte[] getRegionStartKeyInHFile() {
        return regionStartKeyInHFile;
    }

    public byte[] getSplitkey() {
        return splitkey;
    }

    public byte[] getSplitRow() {
        return splitRow;
    }

    public boolean isTop() {
        return top;
    }
}