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

package org.apache.phoenix.schema;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CACHE_SIZE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INCREMENT_BY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.SequenceRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

public class Sequence {
    public static final int SUCCESS = 0;
    
    public enum Action {VALIDATE, RESERVE};
    
    // Pre-compute index of sequence key values to prevent binary search
    private static final KeyValue CURRENT_VALUE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CURRENT_VALUE_BYTES);
    private static final KeyValue INCREMENT_BY_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, INCREMENT_BY_BYTES);
    private static final KeyValue CACHE_SIZE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CACHE_SIZE_BYTES);
    private static final List<KeyValue> SEQUENCE_KV_COLUMNS = Arrays.<KeyValue>asList(
            CURRENT_VALUE_KV,
            INCREMENT_BY_KV,
            CACHE_SIZE_KV
            );
    static {
        Collections.sort(SEQUENCE_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    private static final int CURRENT_VALUE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CURRENT_VALUE_KV);
    private static final int INCREMENT_BY_INDEX = SEQUENCE_KV_COLUMNS.indexOf(INCREMENT_BY_KV);
    private static final int CACHE_SIZE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CACHE_SIZE_KV);

    private static final int SEQUENCE_KEY_VALUES = SEQUENCE_KV_COLUMNS.size();
    private static final EmptySequenceCacheException EMPTY_SEQUENCE_CACHE_EXCEPTION = new EmptySequenceCacheException();
    
    private final SequenceKey key;
    private final ReentrantLock lock;
    private List<SequenceValue> values;
    
    public Sequence(SequenceKey key) {
        if (key == null) throw new NullPointerException();
        this.key = key;
        this.lock = new ReentrantLock();
    }

    private void insertSequenceValue(SequenceValue value) {
        if (values == null) {
            values = Lists.newArrayListWithExpectedSize(1);
            values.add(value);
        } else {
            int i = values.size()-1;
            while (i >= 0 && values.get(i).timestamp > value.timestamp) {
                i--;
            }
            // Don't insert another value if there's one at the same timestamp that is a delete
            if (i >= 0 && values.get(i).timestamp == value.timestamp) {
                if (values.get(i).isDeleted()) {
                    throw new IllegalStateException("Unexpected delete marker at timestamp " + value.timestamp + " for "+ key);
                }
                values.set(i, value);
            } else {
                values.add(i+1, value);
            }
        }
    }
    
    private SequenceValue findSequenceValue(long timestamp) {
        if (values == null) {
            return null;
        }
        int i = values.size()-1;
        while (i >= 0 && values.get(i).timestamp >= timestamp) {
            i--;
        }
        if (i < 0) {
            return null;
        }
        SequenceValue value = values.get(i);
        return value.isDeleted() ? null : value;
    }
    
    public long incrementValue(long timestamp, int factor, Action action) throws EmptySequenceCacheException {
        SequenceValue value = findSequenceValue(timestamp);
        if (value == null) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        if (value.currentValue == value.nextValue) {
            if (action == Action.VALIDATE) {
                return value.currentValue;
            }
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        long returnValue = value.currentValue;
        value.currentValue += factor * value.incrementBy;
        return returnValue;
    }

    public List<Append> newReturns() {
        if (values == null) {
            return Collections.emptyList();
        }
        List<Append> appends = Lists.newArrayListWithExpectedSize(values.size());
        for (SequenceValue value : values) {
            if (value.isInitialized() && value.currentValue != value.nextValue) {
                appends.add(newReturn(value));
            }
        }
        return appends;
    }
    
    public Append newReturn(long timestamp) throws EmptySequenceCacheException {
        SequenceValue value = findSequenceValue(timestamp);
        if (value == null) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        if (value.currentValue == value.nextValue) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        return newReturn(value);
    }

    private Append newReturn(SequenceValue value) {
        byte[] key = SchemaUtil.getSequenceKey(this.key.getTenantId(), this.key.getSchemaName(), this.key.getSequenceName());
        Append append = new Append(key);
        byte[] opBuf = new byte[] {(byte)SequenceRegionObserver.Op.RETURN_SEQUENCE.ordinal()};
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, opBuf);
        append.setAttribute(SequenceRegionObserver.CURRENT_VALUE_ATTRIB, PDataType.LONG.toBytes(value.nextValue));
        Map<byte[], List<Cell>> familyMap = append.getFamilyCellMap();
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<Cell>asList(
                (Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, value.timestamp, PDataType.LONG.toBytes(value.currentValue))
                ));
        return append;
    }
    
    public long currentValue(long timestamp) throws EmptySequenceCacheException {
        SequenceValue value = findSequenceValue(timestamp);
        if (value == null || value.isUnitialized()) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        return value.currentValue - value.incrementBy;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public SequenceKey getKey() {
        return key;
    }

    public long incrementValue(Result result, int factor) throws SQLException {
        // In this case, we don't definitely know the timestamp of the deleted sequence,
        // but we know anything older is likely deleted. Worse case, we remove a sequence
        // from the cache that we shouldn't have which will cause a gap in sequence values.
        // In that case, we might get an error that a curr value was done on a sequence
        // before a next val was. Not sure how to prevent that.
        if (result.rawCells().length == 1) {
            Cell errorKV = result.rawCells()[0];
            int errorCode = PDataType.INTEGER.getCodec().decodeInt(errorKV.getValueArray(), errorKV.getValueOffset(), SortOrder.getDefault());
            SQLExceptionCode code = SQLExceptionCode.fromErrorCode(errorCode);
            // TODO: We could have the server return the timestamps of the
            // delete markers and we could insert them here, but this seems
            // like overkill.
            // if (code == SQLExceptionCode.SEQUENCE_UNDEFINED) {
            // }
            throw new SQLExceptionInfo.Builder(code)
                .setSchemaName(key.getSchemaName())
                .setTableName(key.getSequenceName())
                .build().buildException();
        }
        // If we found the sequence, we update our cache with the new value
        SequenceValue value = new SequenceValue(result);
        insertSequenceValue(value);
        long currentValue = value.currentValue;
        value.currentValue += factor * value.incrementBy;
        return currentValue;
    }

    @SuppressWarnings("deprecation")
    public Increment newIncrement(long timestamp, Sequence.Action action) {
        Increment inc = new Increment(SchemaUtil.getSequenceKey(key.getTenantId(), key.getSchemaName(), key.getSequenceName()));
        // It doesn't matter what we set the amount too - we always use the values we get
        // from the Get we do to prevent any race conditions. All columns that get added
        // are returned with their current value
        try {
            inc.setTimeRange(MetaDataProtocol.MIN_TABLE_TIMESTAMP, timestamp);
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        for (KeyValue kv : SEQUENCE_KV_COLUMNS) {
            // We don't care about the amount, as we'll add what gets looked up on the server-side
            inc.addColumn(kv.getFamily(), kv.getQualifier(), action.ordinal());
        }
        return inc;
    }
    
    public static KeyValue getCurrentValueKV(List<KeyValue> kvs) {
        assert(kvs.size() == SEQUENCE_KEY_VALUES);
        return kvs.get(CURRENT_VALUE_INDEX);
    }
    
    public static KeyValue getIncrementByKV(List<KeyValue> kvs) {
        assert(kvs.size() == SEQUENCE_KEY_VALUES);
        return kvs.get(INCREMENT_BY_INDEX);
    }
    
    public static KeyValue getCacheSizeKV(List<KeyValue> kvs) {
        assert(kvs.size() == SEQUENCE_KEY_VALUES);
        return kvs.get(CACHE_SIZE_INDEX);
    }
    
    public static KeyValue getCurrentValueKV(Result r) {
        Cell[] kvs = r.rawCells();
        assert(kvs.length == SEQUENCE_KEY_VALUES);
        return org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(kvs[CURRENT_VALUE_INDEX]);
    }
    
    public static KeyValue getIncrementByKV(Result r) {
        Cell[] kvs = r.rawCells();
        assert(kvs.length == SEQUENCE_KEY_VALUES);
        return org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(kvs[INCREMENT_BY_INDEX]);
    }
    
    public static KeyValue getCacheSizeKV(Result r) {
        Cell[] kvs = r.rawCells();
        assert(kvs.length == SEQUENCE_KEY_VALUES);
        return org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(kvs[CACHE_SIZE_INDEX]);
    }
    
    public static Result replaceCurrentValueKV(Result r, KeyValue currentValueKV) {
        Cell[] kvs = r.rawCells();
        List<Cell> newkvs = Lists.newArrayList(kvs);
        newkvs.set(CURRENT_VALUE_INDEX, currentValueKV);
        return Result.create(newkvs);
    }
    
    private static final class SequenceValue {
        public final long incrementBy;
        public final long cacheSize;
        public final long timestamp;
        
        public long currentValue;
        public long nextValue;
        
        public SequenceValue(long timestamp) {
            this(timestamp, false);
        }
        
        public SequenceValue(long timestamp, boolean isDeleted) {
            this.timestamp = timestamp;
            this.incrementBy = isDeleted ? -1 : 0;
            this.cacheSize = 0;
        }
        
        public boolean isInitialized() {
            return this.incrementBy > 0;
        }
        
        public boolean isUnitialized() {
            return this.incrementBy == 0;
        }
        
        public boolean isDeleted() {
            return this.incrementBy < 0;
        }
        
        public SequenceValue(Result r) {
            KeyValue currentValueKV = getCurrentValueKV(r);
            KeyValue incrementByKV = getIncrementByKV(r);
            KeyValue cacheSizeKV = getCacheSizeKV(r);
            timestamp = currentValueKV.getTimestamp();
            nextValue = PDataType.LONG.getCodec().decodeLong(currentValueKV.getValueArray(), currentValueKV.getValueOffset(), SortOrder.getDefault());
            incrementBy = PDataType.LONG.getCodec().decodeLong(incrementByKV.getValueArray(), incrementByKV.getValueOffset(), SortOrder.getDefault());
            cacheSize = PDataType.LONG.getCodec().decodeLong(cacheSizeKV.getValueArray(), cacheSizeKV.getValueOffset(), SortOrder.getDefault());
            currentValue = nextValue - incrementBy * cacheSize;
        }
    }

    public boolean returnValue(Result result) throws SQLException {
        Cell statusKV = result.rawCells()[0];
        if (statusKV.getValueLength() == 0) { // No error, but unable to return sequence values
            return false;
        }
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getValueArray(), statusKV.getValueOffset(), SortOrder.getDefault());
        if (statusCode == SUCCESS) {  // Success - update nextValue down to currentValue
            SequenceValue value = findSequenceValue(timestamp);
            if (value == null) {
                throw new EmptySequenceCacheException(key.getSchemaName(),key.getSequenceName());
            }
            value.nextValue = value.currentValue;
            return true;
        }
        SQLExceptionCode code = SQLExceptionCode.fromErrorCode(statusCode);
        // TODO: We could have the server return the timestamps of the
        // delete markers and we could insert them here, but this seems
        // like overkill.
        // if (code == SQLExceptionCode.SEQUENCE_UNDEFINED) {
        // }
        throw new SQLExceptionInfo.Builder(code)
            .setSchemaName(key.getSchemaName())
            .setTableName(key.getSequenceName())
            .build().buildException();
    }

    public Append createSequence(long startWith, long incrementBy, long cacheSize, long timestamp) {
        byte[] key = SchemaUtil.getSequenceKey(this.key.getTenantId(), this.key.getSchemaName(), this.key.getSequenceName());
        Append append = new Append(key);
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, new byte[] {(byte)SequenceRegionObserver.Op.CREATE_SEQUENCE.ordinal()});
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
            append.setAttribute(SequenceRegionObserver.MAX_TIMERANGE_ATTRIB, Bytes.toBytes(timestamp));
        }
        Map<byte[], List<Cell>> familyMap = append.getFamilyCellMap();
        byte[] startWithBuf = PDataType.LONG.toBytes(startWith);
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<Cell>asList(
                (Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timestamp, ByteUtil.EMPTY_BYTE_ARRAY),
                (Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, timestamp, startWithBuf),
                (Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.START_WITH_BYTES, timestamp, startWithBuf),
                (Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.INCREMENT_BY_BYTES, timestamp, PDataType.LONG.toBytes(incrementBy)),
                (Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CACHE_SIZE_BYTES, timestamp, PDataType.LONG.toBytes(cacheSize))
                ));
        return append;
    }

    public long createSequence(Result result) throws SQLException {
        Cell statusKV = result.rawCells()[0];
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getValueArray(), statusKV.getValueOffset(), SortOrder.getDefault());
        if (statusCode == 0) {  // Success - add sequence value and return timestamp
            SequenceValue value = new SequenceValue(timestamp);
            insertSequenceValue(value);
            return timestamp;
        }
        SQLExceptionCode code = SQLExceptionCode.fromErrorCode(statusCode);
        throw new SQLExceptionInfo.Builder(code)
            .setSchemaName(key.getSchemaName())
            .setTableName(key.getSequenceName())
            .build().buildException();
    }

    public Append dropSequence(long timestamp) {
        byte[] key = SchemaUtil.getSequenceKey(this.key.getTenantId(), this.key.getSchemaName(), this.key.getSequenceName());
        Append append = new Append(key);
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, new byte[] {(byte)SequenceRegionObserver.Op.DROP_SEQUENCE.ordinal()});
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
            append.setAttribute(SequenceRegionObserver.MAX_TIMERANGE_ATTRIB, Bytes.toBytes(timestamp));
        }
        Map<byte[], List<Cell>> familyMap = append.getFamilyCellMap();
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<Cell>asList(
                (Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timestamp, ByteUtil.EMPTY_BYTE_ARRAY)));
        return append;
    }

    public long dropSequence(Result result) throws SQLException {
        Cell statusKV = result.rawCells()[0];
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getValueArray(), statusKV.getValueOffset(), SortOrder.getDefault());
        SQLExceptionCode code = statusCode == 0 ? null : SQLExceptionCode.fromErrorCode(statusCode);
        if (code == null) {
            // Insert delete marker so that point-in-time sequences work
            insertSequenceValue(new SequenceValue(timestamp, true));
            return timestamp;
        }
        // TODO: We could have the server return the timestamps of the
        // delete markers and we could insert them here, but this seems
        // like overkill.
        // if (code == SQLExceptionCode.SEQUENCE_UNDEFINED) {
        // }
        throw new SQLExceptionInfo.Builder(code)
            .setSchemaName(key.getSchemaName())
            .setTableName(key.getSequenceName())
            .build().buildException();
    }
}
