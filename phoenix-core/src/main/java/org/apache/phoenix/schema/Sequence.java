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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE_BYTES;
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
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.SequenceUtil;

import com.google.common.collect.Lists;
import com.google.common.math.LongMath;

public class Sequence {
    public static final int SUCCESS = 0;
    
    public enum ValueOp {VALIDATE_SEQUENCE, RESERVE_SEQUENCE, INCREMENT_SEQUENCE};
    public enum MetaOp {CREATE_SEQUENCE, DROP_SEQUENCE, RETURN_SEQUENCE};
    
    // create empty Sequence key values used while created a sequence row
    private static final KeyValue CURRENT_VALUE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CURRENT_VALUE_BYTES);
    private static final KeyValue INCREMENT_BY_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, INCREMENT_BY_BYTES);
    private static final KeyValue CACHE_SIZE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CACHE_SIZE_BYTES);
    private static final KeyValue MIN_VALUE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, MIN_VALUE_BYTES);
    private static final KeyValue MAX_VALUE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, MAX_VALUE_BYTES);
    private static final KeyValue CYCLE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CYCLE_FLAG_BYTES);
    private static final KeyValue LIMIT_REACHED_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, LIMIT_REACHED_FLAG_BYTES);
    private static final List<KeyValue> SEQUENCE_KV_COLUMNS = Arrays.<KeyValue>asList(
            CURRENT_VALUE_KV,
            INCREMENT_BY_KV,
            CACHE_SIZE_KV,
            // The following three columns were added in 3.1/4.1
            MIN_VALUE_KV,
            MAX_VALUE_KV,
            CYCLE_KV,
            LIMIT_REACHED_KV
            );
    static {
        Collections.sort(SEQUENCE_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    // Pre-compute index of sequence key values to prevent binary search
    private static final int CURRENT_VALUE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CURRENT_VALUE_KV);
    private static final int INCREMENT_BY_INDEX = SEQUENCE_KV_COLUMNS.indexOf(INCREMENT_BY_KV);
    private static final int CACHE_SIZE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CACHE_SIZE_KV);
    private static final int MIN_VALUE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(MIN_VALUE_KV);
    private static final int MAX_VALUE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(MAX_VALUE_KV);
    private static final int CYCLE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CYCLE_KV);
    private static final int LIMIT_REACHED_INDEX = SEQUENCE_KV_COLUMNS.indexOf(LIMIT_REACHED_KV);

    public static final int NUM_SEQUENCE_KEY_VALUES = SEQUENCE_KV_COLUMNS.size();
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
    
    private long increment(SequenceValue value, ValueOp op) throws SQLException {       
        boolean increasingSeq = value.incrementBy > 0 && op != ValueOp.VALIDATE_SEQUENCE;
        // check if the the sequence has already reached the min/max limit
        if (value.limitReached && op != ValueOp.VALIDATE_SEQUENCE) {           
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
        if (op == ValueOp.INCREMENT_SEQUENCE) {
            boolean overflowOrUnderflow=false;
            // advance currentValue while checking for overflow
            try {
                value.currentValue = LongMath.checkedAdd(value.currentValue, value.incrementBy);
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

    public long incrementValue(long timestamp, ValueOp op) throws SQLException {
        SequenceValue value = findSequenceValue(timestamp);
        if (value == null) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        if (value.currentValue == value.nextValue) {
            if (op == ValueOp.VALIDATE_SEQUENCE) {
                return value.currentValue;
            }
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }    
        return increment(value, op);
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
        byte[] key = this.key.getKey();
        Append append = new Append(key);
        byte[] opBuf = new byte[] {(byte)MetaOp.RETURN_SEQUENCE.ordinal()};
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, opBuf);
        append.setAttribute(SequenceRegionObserver.CURRENT_VALUE_ATTRIB, PLong.INSTANCE.toBytes(value.nextValue));
        Map<byte[], List<Cell>> familyMap = append.getFamilyCellMap();
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<Cell>asList(
        		(Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, value.timestamp, PLong.INSTANCE.toBytes(value.currentValue)),
        		// set LIMIT_REACHED flag to false since we are returning unused sequence values
        		(Cell)KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG_BYTES, value.timestamp, PDataType.FALSE_BYTES)
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

    public long incrementValue(Result result, ValueOp op) throws SQLException {
        // In this case, we don't definitely know the timestamp of the deleted sequence,
        // but we know anything older is likely deleted. Worse case, we remove a sequence
        // from the cache that we shouldn't have which will cause a gap in sequence values.
        // In that case, we might get an error that a curr value was done on a sequence
        // before a next val was. Not sure how to prevent that.
        if (result.rawCells().length == 1) {
            Cell errorKV = result.rawCells()[0];
            int errorCode = PInteger.INSTANCE.getCodec().decodeInt(errorKV.getValueArray(), errorKV.getValueOffset(), SortOrder.getDefault());
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
        SequenceValue value = new SequenceValue(result, op);
        insertSequenceValue(value);
        return increment(value, op);
    }

    @SuppressWarnings("deprecation")
    public Increment newIncrement(long timestamp, Sequence.ValueOp action) {
        Increment inc = new Increment(key.getKey());
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
        Cell[] cells = r.rawCells();
        // if the sequence row is from a previous version then MIN_VALUE, MAX_VALUE, CYCLE and LIMIT_REACHED key values are not present,
        // the sequence row has only three columns (INCREMENT_BY, CACHE_SIZE and CURRENT_VALUE) and the order of the cells 
        // in the array returned by rawCells() is not what what we expect so use getColumnLatestCell() to get the cell we want
        Cell cell = cells.length == NUM_SEQUENCE_KEY_VALUES ? cells[cellIndex] :
        	r.getColumnLatestCell(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
        return org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(cell);
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
    
    public static KeyValue getMinValueKV(Result r) {
        return getKeyValue(r, MIN_VALUE_KV, MIN_VALUE_INDEX);
    }
    
    public static KeyValue getMaxValueKV(Result r) {
        return getKeyValue(r, MAX_VALUE_KV, MAX_VALUE_INDEX);
    }
    
    public static KeyValue getCycleKV(Result r) {
        return getKeyValue(r, CYCLE_KV, CYCLE_INDEX);
    }

    public static KeyValue getLimitReachedKV(Result r) {
        return getKeyValue(r, LIMIT_REACHED_KV, LIMIT_REACHED_INDEX);
    }
    
    public static void replaceCurrentValueKV(List<Cell> kvs, KeyValue currentValueKV) {
        kvs.set(CURRENT_VALUE_INDEX, currentValueKV);
    }
    
    public static void replaceMinValueKV(List<Cell> kvs, KeyValue minValueKV) {
        kvs.set(MIN_VALUE_INDEX, minValueKV);
    }

    public static void replaceMaxValueKV(List<Cell> kvs, KeyValue maxValueKV) {
        kvs.set(MAX_VALUE_INDEX, maxValueKV);
    }

    public static void replaceCycleValueKV(List<Cell> kvs, KeyValue cycleValueKV) {
        kvs.set(CYCLE_INDEX, cycleValueKV);
    }
    public static void replaceLimitReachedKV(List<Cell> kvs, KeyValue limitReachedKV) {
        kvs.set(LIMIT_REACHED_INDEX, limitReachedKV);
    }
    
    /**
     * Returns the KeyValues of r if it contains the expected number of KeyValues,
     * else returns a list of KeyValues corresponding to SEQUENCE_KV_COLUMNS 
     */
    public static List<Cell> getCells(Result r, int numKVs) {
        // if the sequence row is from a previous version
        if (r.rawCells().length == numKVs )
            return Lists.newArrayList(r.rawCells());
        // else we need to handle missing MIN_VALUE, MAX_VALUE, CYCLE and LIMIT_REACHED KeyValues
        List<Cell> cellList = Lists.newArrayListWithCapacity(NUM_SEQUENCE_KEY_VALUES);
        for (KeyValue kv : SEQUENCE_KV_COLUMNS) {
            cellList.add(getKeyValue(r,kv));
        }
        return cellList;
    }    
    
    private static final class SequenceValue {
        public final long incrementBy;
        public final long timestamp;
        public final long cacheSize;
        
        public long currentValue;
        public long nextValue;
        public long minValue;
        public long maxValue;
        public boolean cycle;
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
            this.cacheSize = 0;
        }
        
        public boolean isInitialized() {
            return this.incrementBy != 0;
        }
        
        public boolean isUnitialized() {
            return this.incrementBy == 0;
        }
        
        public SequenceValue(Result r, ValueOp op) {
            KeyValue currentValueKV = getCurrentValueKV(r);
            KeyValue incrementByKV = getIncrementByKV(r);
            KeyValue cacheSizeKV = getCacheSizeKV(r);
            KeyValue minValueKV = getMinValueKV(r);
            KeyValue maxValueKV = getMaxValueKV(r);
            KeyValue cycleKV = getCycleKV(r);
            this.timestamp = currentValueKV.getTimestamp();
            this.nextValue = PLong.INSTANCE.getCodec().decodeLong(currentValueKV.getValueArray(), currentValueKV.getValueOffset(), SortOrder.getDefault());
            this.incrementBy = PLong.INSTANCE.getCodec().decodeLong(incrementByKV.getValueArray(), incrementByKV.getValueOffset(), SortOrder.getDefault());
            this.cacheSize = PLong.INSTANCE.getCodec().decodeLong(cacheSizeKV.getValueArray(), cacheSizeKV.getValueOffset(), SortOrder.getDefault());
            this.minValue = PLong.INSTANCE.getCodec().decodeLong(minValueKV.getValueArray(), minValueKV.getValueOffset(), SortOrder.getDefault());
            this.maxValue = PLong.INSTANCE.getCodec().decodeLong(maxValueKV.getValueArray(), maxValueKV.getValueOffset(), SortOrder.getDefault());
            this.cycle = (Boolean) PBoolean.INSTANCE.toObject(cycleKV.getValueArray(), cycleKV.getValueOffset(), cycleKV.getValueLength());
            this.limitReached = false;
            currentValue = nextValue;
            if (op != ValueOp.VALIDATE_SEQUENCE) {
                currentValue -= incrementBy * cacheSize;
            }
        }
    }

    public boolean returnValue(Result result) throws SQLException {
        Cell statusKV = result.rawCells()[0];
        if (statusKV.getValueLength() == 0) { // No error, but unable to return sequence values
            return false;
        }
        long timestamp = statusKV.getTimestamp();
        int statusCode = PInteger.INSTANCE.getCodec().decodeInt(statusKV.getValueArray(), statusKV.getValueOffset(), SortOrder.getDefault());
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
        byte[] key = this.key.getKey();
        Append append = new Append(key);
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, new byte[] {(byte)MetaOp.CREATE_SEQUENCE.ordinal()});
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
            append.setAttribute(SequenceRegionObserver.MAX_TIMERANGE_ATTRIB, Bytes.toBytes(timestamp));
        }
        Map<byte[], List<Cell>> familyMap = append.getFamilyCellMap();
        byte[] startWithBuf = PLong.INSTANCE.toBytes(startWith);
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<Cell>asList(
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timestamp, ByteUtil.EMPTY_BYTE_ARRAY),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, timestamp, startWithBuf),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.START_WITH_BYTES, timestamp, startWithBuf),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.INCREMENT_BY_BYTES, timestamp, PLong.INSTANCE.toBytes(incrementBy)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CACHE_SIZE_BYTES, timestamp, PLong.INSTANCE.toBytes(cacheSize)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_VALUE_BYTES, timestamp, PLong.INSTANCE.toBytes(minValue)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_VALUE_BYTES, timestamp, PLong.INSTANCE.toBytes(maxValue)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CYCLE_FLAG_BYTES, timestamp, PBoolean.INSTANCE.toBytes(cycle)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG_BYTES, timestamp, PDataType.FALSE_BYTES)
                ));
        return append;
    }

    public long createSequence(Result result, long minValue, long maxValue, boolean cycle) throws SQLException {
        Cell statusKV = result.rawCells()[0];
        long timestamp = statusKV.getTimestamp();
        int statusCode = PInteger.INSTANCE.getCodec().decodeInt(statusKV.getValueArray(), statusKV.getValueOffset(), SortOrder.getDefault());
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
        byte[] key =  this.key.getKey();
        Append append = new Append(key);
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, new byte[] {(byte)MetaOp.DROP_SEQUENCE.ordinal()});
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
        int statusCode = PInteger.INSTANCE.getCodec().decodeInt(statusKV.getValueArray(), statusKV.getValueOffset(), SortOrder.getDefault());
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

    public static String getCreateTableStatement(int nSaltBuckets) {
        if (nSaltBuckets <= 0) {
            return QueryConstants.CREATE_SEQUENCE_METADATA;
        }
        return QueryConstants.CREATE_SEQUENCE_METADATA + "," + PhoenixDatabaseMetaData.SALT_BUCKETS + "=" + nSaltBuckets;
    }
}
