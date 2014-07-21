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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CYCLE_FLAG_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INCREMENT_BY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.START_WITH_BYTES;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

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
import org.apache.phoenix.util.SequenceUtil;

import com.google.common.collect.Lists;
import com.google.common.math.LongMath;

public class Sequence {
    public static final int SUCCESS = 0;
    
    public enum ValueOp {VALIDATE_SEQUENCE, RESERVE_SEQUENCE};
    public enum MetaOp {CREATE_SEQUENCE, DROP_SEQUENCE, RETURN_SEQUENCE};
    
    // create empty Sequence key values used while created a sequence row
    private static final KeyValue CURRENT_VALUE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CURRENT_VALUE_BYTES);
    private static final KeyValue INCREMENT_BY_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, INCREMENT_BY_BYTES);
    private static final KeyValue CACHE_SIZE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CACHE_SIZE_BYTES);
    private static final KeyValue START_WITH_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, START_WITH_BYTES);
    private static final KeyValue MIN_VALUE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, MIN_VALUE_BYTES);
    private static final KeyValue MAX_VALUE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, MAX_VALUE_BYTES);
    private static final KeyValue CYCLE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CYCLE_FLAG_BYTES);
    private static final List<KeyValue> SEQUENCE_KV_COLUMNS = Arrays.<KeyValue>asList(
            CURRENT_VALUE_KV,
            INCREMENT_BY_KV,
            CACHE_SIZE_KV,
            START_WITH_KV,
            // the following three columns were added in 3.0/4.0
            MIN_VALUE_KV,
            MAX_VALUE_KV,
            CYCLE_KV
            );
    static {
        Collections.sort(SEQUENCE_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    // Pre-compute index of sequence key values to prevent binary search
    private static final int CURRENT_VALUE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CURRENT_VALUE_KV);
    private static final int INCREMENT_BY_INDEX = SEQUENCE_KV_COLUMNS.indexOf(INCREMENT_BY_KV);
    private static final int CACHE_SIZE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CACHE_SIZE_KV);
    private static final int START_WITH_INDEX = SEQUENCE_KV_COLUMNS.indexOf(START_WITH_KV);
    private static final int MIN_VALUE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(MIN_VALUE_KV);
    private static final int MAX_VALUE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(MAX_VALUE_KV);
    private static final int CYCLE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CYCLE_KV);

    private static final int NUM_SEQUENCE_KEY_VALUES = SEQUENCE_KV_COLUMNS.size();
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
                if (values.get(i).isDeleted) {
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
        return value.isDeleted ? null : value;
    }
    
    private long increment(SequenceValue value, int factor) throws SQLException {       
        boolean increasingSeq = value.incrementBy > 0;
        // check if the the sequence has already reached the min/max limit
        if (value.limitReached) {           
            if (value.cycle) {
                value.limitReached=false;
                throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
            } else {
                SQLExceptionCode code =
                        increasingSeq ? SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE
                                : SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE;
                throw SequenceUtil.getException(this.key.getSchemaName(),
                    this.key.getSequenceName(), code);
            }
        }
        
        long returnValue = value.currentValue;
        if (factor != 0) {
            --value.unusedValues;
            boolean overflowOrUnderflow=false;
            // advance currentValue while checking for overflow
            try {
                long incrementValue = LongMath.checkedMultiply(value.incrementBy, factor);
                value.currentValue = LongMath.checkedAdd(value.currentValue, incrementValue);
            } catch (ArithmeticException e) {
                overflowOrUnderflow = true;
            }
                          
            // set the limitReached flag (which will be checked the next time increment is called)
            // if overflow or limit was reached
            if (overflowOrUnderflow || (increasingSeq && value.currentValue > value.maxValue)
                    || (!increasingSeq && value.currentValue < value.minValue)) {
                value.limitReached=true;
            }
        }
        return returnValue;
    }

    public long incrementValue(long timestamp, int factor, ValueOp action) throws SQLException {
        SequenceValue value = findSequenceValue(timestamp);
        if (value == null) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        if (value.unusedValues == 0) {
            if (action == ValueOp.VALIDATE_SEQUENCE) {
                return value.currentValue;
            }
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }    
        return increment(value, factor);
    }

    public List<Append> newReturns() {
        if (values == null) {
            return Collections.emptyList();
        }
        List<Append> appends = Lists.newArrayListWithExpectedSize(values.size());
        for (SequenceValue value : values) {
            if (value.isInitialized() && value.unusedValues>0) {
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
        if (value.unusedValues==0) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        return newReturn(value);
    }

    private Append newReturn(SequenceValue value) {
        byte[] key = SchemaUtil.getSequenceKey(this.key.getTenantId(), this.key.getSchemaName(), this.key.getSequenceName());
        Append append = new Append(key);
        byte[] opBuf = new byte[] {(byte)MetaOp.RETURN_SEQUENCE.ordinal()};
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, opBuf);
        append.setAttribute(SequenceRegionObserver.CURRENT_VALUE_ATTRIB, PDataType.LONG.toBytes(value.startValue));
        Map<byte[], List<KeyValue>> familyMap = append.getFamilyMap();
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<KeyValue>asList(
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, value.timestamp, ByteUtil.EMPTY_BYTE_ARRAY),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.START_WITH_BYTES, value.timestamp, PDataType.LONG.toBytes(value.currentValue))
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
        if (result.raw().length == 1) {
            KeyValue errorKV = result.raw()[0];
            int errorCode = PDataType.INTEGER.getCodec().decodeInt(errorKV.getBuffer(), errorKV.getValueOffset(), SortOrder.getDefault());
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
        return increment(value, factor);
    }

    public Increment newIncrement(long timestamp, Sequence.ValueOp action) {
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
    
    /**
     * Returns a KeyValue from the input result row
     * @param kv an empty KeyValue used only to get the column family and column qualifier of the
     *            key value to be returned (if the sequence row is from a previous version)
     * @param cellIndex index of the KeyValue to be returned (if the sequence row is from a previous version
     * @return KeyValue
     */
    private static KeyValue getKeyValue(Result r, KeyValue kv, int cellIndex) {
        KeyValue[] kvs = r.raw();
        // if the sequence row is from a previous version then MIN_VALUE, MAX_VALUE and CYCLE key values are not present,
        // the sequence row has only four columns (START_VALUE, INCREMENT_BY, CACHE_SIZE and CURRENT_VALUE) and the order of the cells 
        // in the array returned by rawCells() is not what what we expect so use getColumnLatestCell() to get the cell we want
        return kvs.length != NUM_SEQUENCE_KEY_VALUES ?  r.getColumnLatest(kv.getFamily(), kv.getQualifier()) : (kvs[cellIndex]);
    }
    
    private static KeyValue getKeyValue(Result r, KeyValue kv) {
        return getKeyValue(r, kv, SEQUENCE_KV_COLUMNS.indexOf(kv));
    }
    
    public static KeyValue getCurrentValueKV(Result r) {
        return getKeyValue(r, CURRENT_VALUE_KV, CURRENT_VALUE_INDEX);
    }
    
    public static KeyValue getIncrementByKV(Result r) {
        return getKeyValue(r, INCREMENT_BY_KV, INCREMENT_BY_INDEX);
    }
    
    public static KeyValue getCacheSizeKV(Result r) {
        return getKeyValue(r, CACHE_SIZE_KV, CACHE_SIZE_INDEX);
    }
    
    public static KeyValue getStartValueKV(Result r) {
        return getKeyValue(r, START_WITH_KV, START_WITH_INDEX);
    }
    
    public static KeyValue getMinValueKV(Result r) {
        return getKeyValue(r, MIN_VALUE_KV, MIN_VALUE_INDEX);
    }
    
    public static KeyValue getMaxValueKV(Result r) {
        return getKeyValue(r, MAX_VALUE_KV, MAX_VALUE_INDEX);
    }
    
    public static KeyValue getCycleKV(Result r) {
        return getKeyValue(r, CYCLE_KV, CYCLE_INDEX);
    }
    
    public static void replaceCurrentValueKV(List<KeyValue> kvs, KeyValue currentValueKV) {
        kvs.set(CURRENT_VALUE_INDEX, currentValueKV);
    }
    
    public static void replaceMinValueKV(List<KeyValue> kvs, KeyValue minValueKV) {
        kvs.set(MIN_VALUE_INDEX, minValueKV);
    }

    public static void replaceMaxValueKV(List<KeyValue> kvs, KeyValue maxValueKV) {
        kvs.set(MAX_VALUE_INDEX, maxValueKV);
    }

    public static void replaceCycleValueKV(List<KeyValue> kvs, KeyValue cycleValueKV) {
        kvs.set(CYCLE_INDEX, cycleValueKV);
    }
    
    /**
     * Returns a KeyValue[] for the result row. Handles empty MIN_VALUE, MAX_VALUE and CYCLE
     * KeyValues if the sequence row is from a previous version
     */
    public static List<KeyValue> getCells(Result r) {
        // if the sequence row is from a previous version
        if (r.raw().length == NUM_SEQUENCE_KEY_VALUES )
            return Lists.newArrayList(r.raw());
        // else we need to handle missing MIN_VALUE, MAX_VALUE and CYCLE KeyValues
        List<KeyValue> kvList = Lists.newArrayListWithCapacity(NUM_SEQUENCE_KEY_VALUES);
        for (KeyValue kv : SEQUENCE_KV_COLUMNS) {
            kvList.add(getKeyValue(r,kv));
        }
        return kvList;
    }
    
    private static final class SequenceValue {
        public final long incrementBy;
        public final long timestamp;
        
        public long currentValue;
        // start value of the current batch 
        public long startValue;
        public long minValue;
        public long maxValue;
        public boolean cycle;
        // number of values left in current batch
        public long unusedValues;
        public boolean isDeleted;
        public boolean limitReached;
        
        public SequenceValue(long timestamp, long minValue, long maxValue, boolean cycle) {
            this(timestamp, false);
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.cycle = cycle;
        }
        
        public SequenceValue(long timestamp, boolean isDeleted) {
            this.timestamp = timestamp;
            this.isDeleted = isDeleted;
            this.incrementBy = 0;
            this.limitReached = false;
        }
        
        public boolean isInitialized() {
            return this.incrementBy != 0;
        }
        
        public boolean isUnitialized() {
            return this.incrementBy == 0;
        }
        
        public SequenceValue(Result r) {
            KeyValue currentValueKV = getCurrentValueKV(r);
            KeyValue incrementByKV = getIncrementByKV(r);
            KeyValue cacheSizeKV = getCacheSizeKV(r);
            KeyValue minValueKV = getMinValueKV(r);
            KeyValue maxValueKV = getMaxValueKV(r);
            KeyValue cycleKV = getCycleKV(r);
            this.timestamp = currentValueKV.getTimestamp();
            this.currentValue = PDataType.LONG.getCodec().decodeLong(currentValueKV.getBuffer(), currentValueKV.getValueOffset(), SortOrder.getDefault());
            this.incrementBy = PDataType.LONG.getCodec().decodeLong(incrementByKV.getBuffer(), incrementByKV.getValueOffset(), SortOrder.getDefault());
            this.unusedValues = PDataType.LONG.getCodec().decodeLong(cacheSizeKV.getBuffer(), cacheSizeKV.getValueOffset(), SortOrder.getDefault());
            this.minValue = PDataType.LONG.getCodec().decodeLong(minValueKV.getBuffer(), minValueKV.getValueOffset(), SortOrder.getDefault());
            this.maxValue = PDataType.LONG.getCodec().decodeLong(maxValueKV.getBuffer(), maxValueKV.getValueOffset(), SortOrder.getDefault());
            this.cycle = (Boolean)PDataType.BOOLEAN.toObject(cycleKV.getBuffer(), cycleKV.getValueOffset(), cycleKV.getValueLength());
            this.limitReached = false;
            // store the start value of this batch of sequence values, so that it can be used to
            // determine if we can return unused sequence values when we close the connection
            this.startValue = this.currentValue;
        }
    }

    public boolean returnValue(Result result) throws SQLException {
        KeyValue statusKV = result.raw()[0];
        if (statusKV.getValueLength() == 0) { // No error, but unable to return sequence values
            return false;
        }
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getBuffer(), statusKV.getValueOffset(), SortOrder.getDefault());
        if (statusCode == SUCCESS) {  // Success - update nextValue down to currentValue
            SequenceValue value = findSequenceValue(timestamp);
            if (value == null) {
                throw new EmptySequenceCacheException(key.getSchemaName(),key.getSequenceName());
            }
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

    public Append createSequence(long startWith, long incrementBy, long cacheSize, long timestamp, long minValue, long maxValue, boolean cycle) {
        byte[] key = SchemaUtil.getSequenceKey(this.key.getTenantId(), this.key.getSchemaName(), this.key.getSequenceName());
        Append append = new Append(key);
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, new byte[] {(byte)MetaOp.CREATE_SEQUENCE.ordinal()});
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
            append.setAttribute(SequenceRegionObserver.MAX_TIMERANGE_ATTRIB, Bytes.toBytes(timestamp));
        }
        Map<byte[], List<KeyValue>> familyMap = append.getFamilyMap();
        byte[] startWithBuf = PDataType.LONG.toBytes(startWith);
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<KeyValue>asList(
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timestamp, ByteUtil.EMPTY_BYTE_ARRAY),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, timestamp, ByteUtil.EMPTY_BYTE_ARRAY),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.START_WITH_BYTES, timestamp, startWithBuf),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.INCREMENT_BY_BYTES, timestamp, PDataType.LONG.toBytes(incrementBy)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CACHE_SIZE_BYTES, timestamp, PDataType.LONG.toBytes(cacheSize)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_VALUE_BYTES, timestamp, PDataType.LONG.toBytes(minValue)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_VALUE_BYTES, timestamp, PDataType.LONG.toBytes(maxValue)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CYCLE_FLAG_BYTES, timestamp, PDataType.BOOLEAN.toBytes(cycle))
                ));
        return append;
    }


    public long createSequence(Result result, long minValue, long maxValue, boolean cycle) throws SQLException {
        KeyValue statusKV = result.raw()[0];
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getBuffer(), statusKV.getValueOffset(), SortOrder.getDefault());
        if (statusCode == 0) {  // Success - add sequence value and return timestamp
            SequenceValue value = new SequenceValue(timestamp, minValue, maxValue, cycle);
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
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, new byte[] {(byte)MetaOp.DROP_SEQUENCE.ordinal()});
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
            append.setAttribute(SequenceRegionObserver.MAX_TIMERANGE_ATTRIB, Bytes.toBytes(timestamp));
        }
        Map<byte[], List<KeyValue>> familyMap = append.getFamilyMap();
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<KeyValue>asList(
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timestamp, ByteUtil.EMPTY_BYTE_ARRAY)));
        return append;
    }

    public long dropSequence(Result result) throws SQLException {
        KeyValue statusKV = result.raw()[0];
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getBuffer(), statusKV.getValueOffset(), SortOrder.getDefault());
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
