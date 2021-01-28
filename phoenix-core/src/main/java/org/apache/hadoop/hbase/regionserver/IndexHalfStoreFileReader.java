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

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SCAN_START_ROW_SUFFIX;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.index.IndexMaintainer;

/**
 * A facade for a {@link org.apache.hadoop.hbase.io.hfile.HFile.Reader} that serves up either the
 * top or bottom half of an HFile where 'bottom' is the first half of the file containing the keys
 * that sort lowest and 'top' is the second half of the file with keys that sort greater than those
 * of the bottom half. The top includes the split files midkey, of the key that follows if it does
 * not exist in the file.
 *
 * <p>
 * This type works in tandem with the {@link Reference} type. This class is used reading while
 * Reference is used writing.
 *
 * 
 * This file is not splitable. Calls to #midkey() return null.
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
    private final HRegionInfo childRegionInfo;
    private final byte[] regionStartKeyInHFile;
    private final HRegionInfo currentRegion;

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
     * @param childRegionInfo
     * @param regionStartKeyInHFile
     * @param splitKey
     * @throws IOException
     */
    public IndexHalfStoreFileReader(final FileSystem fs, final Path p, final CacheConfig cacheConf,
            final FSDataInputStreamWrapper in, long size, final Reference r,
            final Configuration conf,
            final Map<ImmutableBytesWritable, IndexMaintainer> indexMaintainers,
            final byte[][] viewConstants, final HRegionInfo childRegionInfo,
            byte[] regionStartKeyInHFile, byte[] splitKey, HRegionInfo currentRegion)
            throws IOException {
        super(fs, p, in, size, cacheConf, conf);
        this.splitkey = splitKey == null ? r.getSplitKey() : splitKey;
        // Is it top or bottom half?
        this.top = Reference.isTopFileRegion(r.getFileRegion());
        this.splitRow = CellUtil.cloneRow(KeyValue.createKeyValueFromKey(splitkey));
        this.indexMaintainers = indexMaintainers;
        this.viewConstants = viewConstants;
        this.childRegionInfo = childRegionInfo;
        this.regionStartKeyInHFile = regionStartKeyInHFile;
        this.offset = regionStartKeyInHFile.length;
        this.currentRegion = currentRegion;
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
        return childRegionInfo;
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

    // These implement the 1.3, and 1.4+ versions of the getStoreFileScanner() interface
    // The @Override is missing, since we do not know which version will be active at compile time

    //The 1.4+ version
    public StoreFileScanner getStoreFileScanner(boolean cacheBlocks, boolean pread,
            boolean isCompaction, long readPt,
            long scannerOrder, boolean canOptimizeForNonNullColumn) {
        return new LocalIndexStoreFileScanner(this, getScanner(cacheBlocks, pread, isCompaction),
            true, getHFileReader().hasMVCCInfo(), readPt,
            scannerOrder, canOptimizeForNonNullColumn);
    }

    //The 1.3 version
    public StoreFileScanner getStoreFileScanner(boolean cacheBlocks, boolean pread,
           boolean isCompaction, long readPt) {
        return new LocalIndexStoreFileScanner(this, getScanner(cacheBlocks, pread, isCompaction),
                true, getHFileReader().hasMVCCInfo(), readPt, 0, false);
    }

    @Override
    public boolean passesKeyRangeFilter(Scan scan) {
        if (scan.getAttribute(SCAN_START_ROW_SUFFIX) == null) {
            // Scan from compaction.
            return true;
        }
        byte[] startKey = currentRegion.getStartKey();
        byte[] endKey = currentRegion.getEndKey();
        // If the region start key is not the prefix of the scan start row then we can return empty
        // scanners. This is possible during merge where one of the child region scan should not return any
        // results as we go through merged region.
        int prefixLength =
                scan.getStartRow().length - scan.getAttribute(SCAN_START_ROW_SUFFIX).length;
        if (Bytes.compareTo(scan.getStartRow(), 0, prefixLength,
                (startKey.length == 0 ? new byte[endKey.length] : startKey), 0,
                (startKey.length == 0 ? endKey.length : startKey.length)) != 0) {
            return false;
        }
        return true;
    }
}