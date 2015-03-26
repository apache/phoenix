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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
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
    private static final int ROW_KEY_LENGTH = 2;
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
            final Reference r, final Configuration conf,
            final Map<ImmutableBytesWritable, IndexMaintainer> indexMaintainers,
            final byte[][] viewConstants, final HRegionInfo regionInfo,
            final byte[] regionStartKeyInHFile, byte[] splitKey) throws IOException {
        super(fs, p, cacheConf, conf);
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

    protected boolean isTop() {
        return this.top;
    }

    @Override
    public HFileScanner getScanner(final boolean cacheBlocks, final boolean pread,
            final boolean isCompaction) {
        final HFileScanner s = super.getScanner(cacheBlocks, pread, isCompaction);
        return new HFileScanner() {
            final HFileScanner delegate = s;
            public boolean atEnd = false;

            public ByteBuffer getKey() {
                if (atEnd) {
                    return null;
                }
                boolean changeBottomKeys =
                        regionInfo.getStartKey().length == 0 && splitRow.length != offset;
                if (!top) {
                    // For first region we are prepending empty byte array of length region end key.
                    // So if split row length is not equal to region end key length then we need to
                    // replace empty bytes of split row length. Because after split end key is the split
                    // row.
                    if(!changeBottomKeys) return delegate.getKey();
                }
                // If it is top store file replace the StartKey of the Key with SplitKey
                return getChangedKey(delegate.getKeyValue(), changeBottomKeys);
            }
            
            private ByteBuffer getChangedKey(Cell kv, boolean changeBottomKeys) {
                // new KeyValue(row, family, qualifier, timestamp, type, value)
                byte[] newRowkey = getNewRowkeyByRegionStartKeyReplacedWithSplitKey(kv, changeBottomKeys);
                KeyValue newKv =
                        new KeyValue(newRowkey, 0, newRowkey.length, kv.getFamilyArray(),
                                kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(),
                                kv.getQualifierOffset(), kv.getQualifierLength(),
                                kv.getTimestamp(), Type.codeToType(kv.getTypeByte()), null, 0, 0);
                ByteBuffer keyBuffer = ByteBuffer.wrap(newKv.getKey());
                return keyBuffer;
            }

            private byte[] getNewRowkeyByRegionStartKeyReplacedWithSplitKey(Cell kv, boolean changeBottomKeys) {
                int lenOfRemainingKey = kv.getRowLength() - offset;
                byte[] keyReplacedStartKey = new byte[lenOfRemainingKey + splitRow.length];
                System.arraycopy(changeBottomKeys ? new byte[splitRow.length] : splitRow, 0,
                    keyReplacedStartKey, 0, splitRow.length);
                System.arraycopy(kv.getRowArray(), kv.getRowOffset() + offset, keyReplacedStartKey,
                    splitRow.length, lenOfRemainingKey);
                return keyReplacedStartKey;
            }

            public String getKeyString() {
                if (atEnd) {
                    return null;
                }
                return Bytes.toStringBinary(getKey());
            }

            public ByteBuffer getValue() {
                if (atEnd) {
                    return null;
                }
                return delegate.getValue();
            }

            public String getValueString() {
                if (atEnd) {
                    return null;
                }
                return Bytes.toStringBinary(getValue());
            }

            public Cell getKeyValue() {
                if (atEnd) {
                    return null;
                }
                Cell kv = delegate.getKeyValue();
                boolean changeBottomKeys =
                        regionInfo.getStartKey().length == 0 && splitRow.length != offset;
                if (!top) {
                    if(!changeBottomKeys) return kv;
                }
                // If it is a top store file change the StartKey with SplitKey in Key
                // and produce the new value corresponding to the change in key
                byte[] changedKey = getNewRowkeyByRegionStartKeyReplacedWithSplitKey(kv, changeBottomKeys);
                KeyValue changedKv =
                        new KeyValue(changedKey, 0, changedKey.length, kv.getFamilyArray(),
                                kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(),
                                kv.getQualifierOffset(), kv.getQualifierLength(),
                                kv.getTimestamp(), Type.codeToType(kv.getTypeByte()),
                                kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(),
                                kv.getTagsArray(), kv.getTagsOffset(), kv.getTagsLength());
                return changedKv;
            }

            public boolean next() throws IOException {
                if (atEnd) {
                    return false;
                }
                while (true) {
                    boolean b = delegate.next();
                    if (!b) {
                        atEnd = true;
                        return b;
                    }
                    // We need to check whether the current KV pointed by this reader is
                    // corresponding to
                    // this split or not.
                    // In case of top store file if the ActualRowKey >= SplitKey
                    // In case of bottom store file if the ActualRowKey < Splitkey
                    if (isSatisfiedMidKeyCondition(delegate.getKeyValue())) {
                        return true;
                    }
                }
            }

            public boolean seekBefore(byte[] key) throws IOException {
                return seekBefore(key, 0, key.length);
            }

            public boolean seekBefore(byte[] key, int offset, int length) throws IOException {

                if (top) {
                    byte[] fk = getFirstKey();
                    // This will be null when the file is empty in which we can not seekBefore to
                    // any key
                    if (fk == null) {
                        return false;
                    }
                    if (getComparator().compare(key, offset, length, fk, 0, fk.length) <= 0) {
                        return false;
                    }
                    KeyValue replacedKey = getKeyPresentInHFiles(key);
                    return this.delegate.seekBefore(replacedKey);
                } else {
                    // The equals sign isn't strictly necessary just here to be consistent with
                    // seekTo
                    if (getComparator().compare(key, offset, length, splitkey, 0, splitkey.length) >= 0) {
                        return this.delegate.seekBefore(splitkey, 0, splitkey.length);
                    }
                }
                return this.delegate.seekBefore(key, offset, length);
            }

            @Override
            public boolean seekBefore(Cell cell) throws IOException {
                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                return seekBefore(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
            }

            public boolean seekTo() throws IOException {
                boolean b = delegate.seekTo();
                if (!b) {
                    atEnd = true;
                    return b;
                }
                while (true) {
                    // We need to check the first occurrence of satisfying the condition
                    // In case of top store file if the ActualRowKey >= SplitKey
                    // In case of bottom store file if the ActualRowKey < Splitkey
                    if (isSatisfiedMidKeyCondition(delegate.getKeyValue())) {
                        return true;
                    }
                    b = delegate.next();
                    if (!b) {
                        return b;
                    }
                }
            }

            public int seekTo(byte[] key) throws IOException {
                return seekTo(key, 0, key.length);
            }

            public int seekTo(byte[] key, int offset, int length) throws IOException {
                if (top) {
                    if (getComparator().compare(key, offset, length, splitkey, 0, splitkey.length) < 0) {
                        return -1;
                    }
                    KeyValue replacedKey = getKeyPresentInHFiles(key);

                    int seekTo =
                            delegate.seekTo(replacedKey.getBuffer(), replacedKey.getKeyOffset(),
                                replacedKey.getKeyLength());
                    return seekTo;
                    /*
                     * if (seekTo == 0 || seekTo == -1) { return seekTo; } else if (seekTo == 1) {
                     * boolean next = this.next(); }
                     */
                } else {
                    if (getComparator().compare(key, offset, length, splitkey, 0, splitkey.length) >= 0) {
                        // we would place the scanner in the second half.
                        // it might be an error to return false here ever...
                        boolean res = delegate.seekBefore(splitkey, 0, splitkey.length);
                        if (!res) {
                            throw new IOException(
                                    "Seeking for a key in bottom of file, but key exists in top of file, failed on seekBefore(midkey)");
                        }
                        return 1;
                    }
                }
                return delegate.seekTo(key, offset, length);
            }

            @Override
            public int seekTo(Cell cell) throws IOException {
                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                return seekTo(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
            }

            public int reseekTo(byte[] key) throws IOException {
                return reseekTo(key, 0, key.length);
            }

            public int reseekTo(byte[] key, int offset, int length) throws IOException {
                if (top) {
                    if (getComparator().compare(key, offset, length, splitkey, 0, splitkey.length) < 0) {
                        return -1;
                    }
                    KeyValue replacedKey = getKeyPresentInHFiles(key);
                    return delegate.reseekTo(replacedKey.getBuffer(), replacedKey.getKeyOffset(),
                        replacedKey.getKeyLength());
                } else {
                    if (getComparator().compare(key, offset, length, splitkey, 0, splitkey.length) >= 0) {
                        // we would place the scanner in the second half.
                        // it might be an error to return false here ever...
                        boolean res = delegate.seekBefore(splitkey, 0, splitkey.length);
                        if (!res) {
                            throw new IOException(
                                    "Seeking for a key in bottom of file, but key exists in top of file, failed on seekBefore(midkey)");
                        }
                        return 1;
                    }
                }
                return delegate.reseekTo(key, offset, length);
            }

            @Override
            public int reseekTo(Cell cell) throws IOException {
                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                return reseekTo(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
            }

            public org.apache.hadoop.hbase.io.hfile.HFile.Reader getReader() {
                return this.delegate.getReader();
            }

            // TODO: Need to change as per IndexHalfStoreFileReader
            public boolean isSeeked() {
                return this.delegate.isSeeked();
            }

            // Added for compatibility with HBASE-13109
            // Once we drop support for older versions, add an @override annotation here
            // and figure out how to get the next indexed key
            public Cell getNextIndexedKey() {
                return null; // indicate that we cannot use the optimization
            }
        };
    }

    private boolean isSatisfiedMidKeyCondition(Cell kv) {
        if (CellUtil.isDelete(kv) && kv.getValueLength() == 0) {
            // In case of a Delete type KV, let it be going to both the daughter regions.
            // No problems in doing so. In the correct daughter region where it belongs to, this delete
            // tomb will really delete a KV. In the other it will just hang around there with no actual
            // kv coming for which this is a delete tomb. :)
            return true;
        }
        ImmutableBytesWritable rowKey =
                new ImmutableBytesWritable(kv.getRowArray(), kv.getRowOffset() + offset,
                        kv.getRowLength() - offset);
        Entry<ImmutableBytesWritable, IndexMaintainer> entry = indexMaintainers.entrySet().iterator().next();
        IndexMaintainer indexMaintainer = entry.getValue();
        byte[] viewIndexId = indexMaintainer.getViewIndexIdFromIndexRowKey(rowKey);
        IndexMaintainer actualIndexMaintainer = indexMaintainers.get(new ImmutableBytesWritable(viewIndexId));
        byte[] dataRowKey = actualIndexMaintainer.buildDataRowKey(rowKey, this.viewConstants);
        int compareResult = Bytes.compareTo(dataRowKey, splitRow);
        if (top) {
            if (compareResult >= 0) {
                return true;
            }
        } else {
            if (compareResult < 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * In case of top half store, the passed key will be with the start key of the daughter region.
     * But in the actual HFiles, the key will be with the start key of the old parent region. In
     * order to make the real seek in the HFiles, we need to build the old key. 
     * 
     * The logic here is just replace daughter region start key with parent region start key
     * in the key part.
     * 
     * @param key
     * 
     */
    private KeyValue getKeyPresentInHFiles(byte[] key) {
        KeyValue keyValue = new KeyValue(key);
        int rowLength = keyValue.getRowLength();
        int rowOffset = keyValue.getRowOffset();

        int daughterStartKeyLength =
                regionInfo.getStartKey().length == 0 ? regionInfo.getEndKey().length : regionInfo
                        .getStartKey().length;

        // This comes incase of deletefamily
        if (top
                && 0 == keyValue.getValueLength()
                && keyValue.getTimestamp() == HConstants.LATEST_TIMESTAMP
                && Bytes.compareTo(keyValue.getRowArray(), keyValue.getRowOffset(),
                    keyValue.getRowLength(), splitRow, 0, splitRow.length) == 0
                && CellUtil.isDeleteFamily(keyValue)) {
            KeyValue createFirstDeleteFamilyOnRow =
                    KeyValueUtil.createFirstDeleteFamilyOnRow(regionStartKeyInHFile,
                            keyValue.getFamily());
            return createFirstDeleteFamilyOnRow;
        }

        short length = (short) (keyValue.getRowLength() - daughterStartKeyLength + offset);
        byte[] replacedKey =
                new byte[length + key.length - (rowOffset + rowLength) + ROW_KEY_LENGTH];
        System.arraycopy(Bytes.toBytes(length), 0, replacedKey, 0, ROW_KEY_LENGTH);
        System.arraycopy(regionStartKeyInHFile, 0, replacedKey, ROW_KEY_LENGTH, offset);
        System.arraycopy(keyValue.getRowArray(), keyValue.getRowOffset() + daughterStartKeyLength,
            replacedKey, offset + ROW_KEY_LENGTH, keyValue.getRowLength()
                    - daughterStartKeyLength);
        System.arraycopy(key, rowOffset + rowLength, replacedKey,
            offset + keyValue.getRowLength() - daughterStartKeyLength
                    + ROW_KEY_LENGTH, key.length - (rowOffset + rowLength));
        return KeyValue.createKeyValueFromKey(replacedKey);
    }

    @Override
    public byte[] getLastKey() {
        // This method won't get used for the index region. There is no need to call
        // getClosestRowBefore on the index table. Also this is a split region. Can not be further
        // split
        throw new UnsupportedOperationException("Method is not implemented!");
    }

    @Override
    public byte[] midkey() throws IOException {
        // Returns null to indicate file is not splitable.
        return null;
    }

    @Override
    public byte[] getFirstKey() {
        return super.getFirstKey();
    }

    @Override
    public boolean passesKeyRangeFilter(Scan scan) {
        return true;
    }
}
