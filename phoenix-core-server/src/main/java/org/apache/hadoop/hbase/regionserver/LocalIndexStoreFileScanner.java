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
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compat.hbase.CompatLocalIndexStoreFileScanner;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.util.PhoenixKeyValueUtil;

import static org.apache.hadoop.hbase.KeyValue.ROW_LENGTH_SIZE;

public class LocalIndexStoreFileScanner extends CompatLocalIndexStoreFileScanner {

    private IndexHalfStoreFileReader reader;
    private boolean changeBottomKeys;
    private CellComparatorImpl comparator;
    @SuppressWarnings("deprecation")
    public LocalIndexStoreFileScanner(IndexHalfStoreFileReader reader, boolean cacheBlocks, boolean pread,
            boolean isCompaction, long readPt, long scannerOrder,
            boolean canOptimizeForNonNullColumn) {
        super(reader, cacheBlocks, pread, isCompaction, readPt, scannerOrder,
            canOptimizeForNonNullColumn);
        this.reader = reader;
        this.changeBottomKeys =
                this.reader.getRegionInfo().getStartKey().length == 0
                        && this.reader.getSplitRow().length != this.reader.getOffset();
        this.comparator = (CellComparatorImpl) reader.getComparator();
    }

    @Override
    public Cell next() throws IOException {
        Cell next = super.next();
        while(next !=null && !isSatisfiedMidKeyCondition(next)) {
            next = super.next();
        }
        while(super.peek() != null && !isSatisfiedMidKeyCondition(super.peek())) {
            super.next();
        }
        if (next!=null && (reader.isTop() || changeBottomKeys)) {
            next = getChangedKey(next,  !reader.isTop() && changeBottomKeys);
        } 
        return next;
    }

    @Override
    public Cell peek() {
        Cell peek = super.peek();
        if (peek != null && (reader.isTop() || changeBottomKeys)) {
            peek = getChangedKey(peek, !reader.isTop() && changeBottomKeys);
        } 
        return peek;
    }

    private Cell getChangedKey(Cell next, boolean changeBottomKeys) {
        // If it is a top store file change the StartKey with SplitKey in Key
        //and produce the new value corresponding to the change in key
        byte[] changedKey = getNewRowkeyByRegionStartKeyReplacedWithSplitKey(next, changeBottomKeys);
        KeyValue changedKv =
                new KeyValue(changedKey, 0, changedKey.length, next.getFamilyArray(),
                    next.getFamilyOffset(), next.getFamilyLength(), next.getQualifierArray(),
                    next.getQualifierOffset(), next.getQualifierLength(),
                    next.getTimestamp(), Type.codeToType(next.getTypeByte()),
                    next.getValueArray(), next.getValueOffset(), next.getValueLength(),
                    next.getTagsArray(), next.getTagsOffset(), next.getTagsLength());
        return changedKv;
    }

    /**
     * Enforce seek all the time for local index store file scanner otherwise some times hbase
     * might return fake kvs not in physical files.
     */
    @Override
    public boolean requestSeek(Cell kv, boolean forward, boolean useBloom) throws IOException {
        boolean requestSeek = super.requestSeek(kv, forward, useBloom);
        if(requestSeek) {
            Cell peek = super.peek();
            if (Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
                peek.getRowArray(), peek.getRowOffset(), peek.getRowLength()) == 0) {
                return forward ? reseek(kv): seek(kv);
            }
        }
        return requestSeek;
    }

    @Override
    public boolean seek(Cell key) throws IOException {
        return seekOrReseek(key, true);
    }

    @Override
    public boolean reseek(Cell key) throws IOException {
        return seekOrReseek(key, false);
    }

    @Override
    public boolean seekToPreviousRow(Cell key) throws IOException {
        KeyValue kv = PhoenixKeyValueUtil.maybeCopyCell(key);
        if (reader.isTop()) {
            Optional<Cell> firstKey = reader.getFirstKey();
            // This will be null when the file is empty in which we can not seekBefore to
            // any key
            if (firstKey.isPresent()) {
                return false;
            }
            if (this.comparator.compare(kv, firstKey.get(), true) <= 0) {
                return super.seekToPreviousRow(key);
            }
            Cell replacedKey = getKeyPresentInHFiles(kv);
            boolean seekToPreviousRow = super.seekToPreviousRow(replacedKey);
            while(super.peek()!=null && !isSatisfiedMidKeyCondition(super.peek())) {
                seekToPreviousRow = super.seekToPreviousRow(super.peek());
            }
            return seekToPreviousRow;
        } else {
            // The equals sign isn't strictly necessary just here to be consistent with
            // seekTo
            KeyValue splitKeyValue = new KeyValue.KeyOnlyKeyValue(reader.getSplitkey());
            if (this.comparator.compare(kv, splitKeyValue, true) >= 0) {
                boolean seekToPreviousRow = super.seekToPreviousRow(kv);
                while(super.peek()!=null && !isSatisfiedMidKeyCondition(super.peek())) {
                    seekToPreviousRow = super.seekToPreviousRow(super.peek());
                }
                return seekToPreviousRow;
            }
        }
        boolean seekToPreviousRow = super.seekToPreviousRow(kv);
        while(super.peek()!=null && !isSatisfiedMidKeyCondition(super.peek())) {
            seekToPreviousRow = super.seekToPreviousRow(super.peek());
        }
        return seekToPreviousRow;
    }

    @Override
    public boolean seekToLastRow() throws IOException {
        boolean seekToLastRow = super.seekToLastRow();
        while(super.peek()!=null && !isSatisfiedMidKeyCondition(super.peek())) {
            seekToLastRow = super.seekToPreviousRow(super.peek());
        }
        return seekToLastRow;
    }

    private boolean isSatisfiedMidKeyCondition(Cell kv) {
        ImmutableBytesWritable rowKey =
                new ImmutableBytesWritable(kv.getRowArray(), kv.getRowOffset() + reader.getOffset(),
                        kv.getRowLength() - reader.getOffset());
        Entry<ImmutableBytesWritable, IndexMaintainer> entry = reader.getIndexMaintainers().entrySet().iterator().next();
        IndexMaintainer indexMaintainer = entry.getValue();
        byte[] viewIndexId = indexMaintainer.getViewIndexIdFromIndexRowKey(rowKey);
        IndexMaintainer actualIndexMaintainer = reader.getIndexMaintainers().get(new ImmutableBytesWritable(viewIndexId));
        if(actualIndexMaintainer != null) {
            byte[] dataRowKey = actualIndexMaintainer.buildDataRowKey(rowKey, reader.getViewConstants());

            int compareResult = Bytes.compareTo(dataRowKey, reader.getSplitRow());
            if (reader.isTop()) {
                if (compareResult >= 0) {
                    return true;
                }
            } else {
                if (compareResult < 0) {
                    return true;
                }
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
    private KeyValue getKeyPresentInHFiles(Cell keyValue) {
        int rowLength = keyValue.getRowLength();
        int rowOffset = keyValue.getRowOffset();

        short length = (short) (rowLength - reader.getSplitRow().length + reader.getOffset());
        byte[] replacedKey =
                new byte[length + keyValue.getRowArray().length - (rowOffset + rowLength) + ROW_LENGTH_SIZE];
        System.arraycopy(Bytes.toBytes(length), 0, replacedKey, 0, ROW_LENGTH_SIZE);
        System.arraycopy(reader.getRegionStartKeyInHFile(), 0, replacedKey, ROW_LENGTH_SIZE, reader.getOffset());
        System.arraycopy(keyValue.getRowArray(), keyValue.getRowOffset() + reader.getSplitRow().length,
            replacedKey, reader.getOffset() + ROW_LENGTH_SIZE, rowLength
                    - reader.getSplitRow().length);
        System.arraycopy(keyValue.getRowArray(), rowOffset + rowLength, replacedKey,
            reader.getOffset() + keyValue.getRowLength() - reader.getSplitRow().length
                    + ROW_LENGTH_SIZE, keyValue.getRowArray().length - (rowOffset + rowLength));
        return new KeyValue.KeyOnlyKeyValue(replacedKey);
    }
    
    /**
     * 
     * @param cell
     * @param isSeek pass true for seek, false for reseek.
     * @return 
     * @throws IOException
     */
    public boolean seekOrReseek(Cell cell, boolean isSeek) throws IOException{
        Cell keyToSeek = cell;
        KeyValue splitKeyValue = new KeyValue.KeyOnlyKeyValue(reader.getSplitkey());
        if (reader.isTop()) {
            if(this.comparator.compare(cell, splitKeyValue, true) < 0){
                if(!isSeek && realSeekDone()) {
                    return true;
                }
                return seekOrReseekToProperKey(isSeek, keyToSeek);
            }
            keyToSeek = getKeyPresentInHFiles(cell);
            return seekOrReseekToProperKey(isSeek, keyToSeek);
        } else {
            if (this.comparator.compare(cell, splitKeyValue, true) >= 0) {
                close();
                return false;
            }
            if(!isSeek && reader.getRegionInfo().getStartKey().length == 0 && reader.getSplitRow().length > reader.getRegionStartKeyInHFile().length) {
                keyToSeek = getKeyPresentInHFiles(cell);
            }
        }
        return seekOrReseekToProperKey(isSeek, keyToSeek);
    }

    private boolean seekOrReseekToProperKey(boolean isSeek, Cell kv)
            throws IOException {
        boolean seekOrReseek = isSeek ? super.seek(kv) : super.reseek(kv);
        while (seekOrReseek && super.peek() != null
                && !isSatisfiedMidKeyCondition(super.peek())) {
            super.next();
            seekOrReseek = super.peek() != null;
        }
        return seekOrReseek;
    }

    private byte[] getNewRowkeyByRegionStartKeyReplacedWithSplitKey(Cell kv, boolean changeBottomKeys) {
        int lenOfRemainingKey = kv.getRowLength() - reader.getOffset();
        byte[] keyReplacedStartKey = new byte[lenOfRemainingKey + reader.getSplitRow().length];
        System.arraycopy(changeBottomKeys ? new byte[reader.getSplitRow().length] : reader.getSplitRow(), 0,
                keyReplacedStartKey, 0, reader.getSplitRow().length);
        System.arraycopy(kv.getRowArray(), kv.getRowOffset() + reader.getOffset(), keyReplacedStartKey,
                reader.getSplitRow().length, lenOfRemainingKey);
        return keyReplacedStartKey;
    }
}
