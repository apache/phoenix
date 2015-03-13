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

package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SequenceUtil;
import org.apache.phoenix.util.ServerUtil;

import com.google.common.collect.Lists;

/**
 * 
 * Region observer coprocessor for sequence operations:
 * 1) For creating a sequence, as checkAndPut does not allow us to scope the
 * Get done for the check with a TimeRange.
 * 2) For incrementing a sequence, as increment does not a) allow us to set the
 * timestamp of the key value being incremented and b) recognize when the key
 * value being incremented does not exist
 * 3) For deleting a sequence, as checkAndDelete does not allow us to scope
 * the Get done for the check with a TimeRange.
 *
 * 
 * @since 3.0.0
 */
public class SequenceRegionObserver extends BaseRegionObserver {
    public static final String OPERATION_ATTRIB = "SEQUENCE_OPERATION";
    public static final String MAX_TIMERANGE_ATTRIB = "MAX_TIMERANGE";
    public static final String CURRENT_VALUE_ATTRIB = "CURRENT_VALUE";
    private static final byte[] SUCCESS_VALUE = PInteger.INSTANCE.toBytes(Integer.valueOf(Sequence.SUCCESS));
    
    private static Result getErrorResult(byte[] row, long timestamp, int errorCode) {
        byte[] errorCodeBuf = new byte[PInteger.INSTANCE.getByteSize()];
        PInteger.INSTANCE.getCodec().encodeInt(errorCode, errorCodeBuf, 0);
        return  Result.create(Collections.singletonList(
                (Cell)KeyValueUtil.newKeyValue(row, 
                        PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, 
                        QueryConstants.EMPTY_COLUMN_BYTES, timestamp, errorCodeBuf)));
    }
    
    private static void acquireLock(HRegion region, byte[] key, List<RowLock> locks)
        throws IOException {
        RowLock rowLock = region.getRowLock(key);
        if (rowLock == null) {
            throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(key));
        }
        locks.add(rowLock);
    }
    
    /**
     * Use PreIncrement hook of BaseRegionObserver to overcome deficiencies in Increment
     * implementation (HBASE-10254):
     * 1) Lack of recognition and identification of when the key value to increment doesn't exist
     * 2) Lack of the ability to set the timestamp of the updated key value.
     * Works the same as existing region.increment(), except assumes there is a single column to
     * increment and uses Phoenix LONG encoding.
     * 
     * @since 3.0.0
     */
    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment) throws IOException {
        RegionCoprocessorEnvironment env = e.getEnvironment();
        // We need to set this to prevent region.increment from being called
        e.bypass();
        e.complete();
        HRegion region = env.getRegion();
        byte[] row = increment.getRow();
        List<RowLock> locks = Lists.newArrayList();
        TimeRange tr = increment.getTimeRange();
        region.startRegionOperation();
        try {
            acquireLock(region, row, locks);
            try {
                long maxTimestamp = tr.getMax();
                boolean validateOnly = true;
                Get get = new Get(row);
                get.setTimeRange(tr.getMin(), tr.getMax());
                for (Map.Entry<byte[], List<Cell>> entry : increment.getFamilyCellMap().entrySet()) {
                    byte[] cf = entry.getKey();
                    for (Cell cq : entry.getValue()) {
                    	long value = Bytes.toLong(cq.getValueArray(), cq.getValueOffset());
                        get.addColumn(cf, CellUtil.cloneQualifier(cq));
                        validateOnly &= (Sequence.ValueOp.VALIDATE_SEQUENCE.ordinal() == value);
                    }
                }
                Result result = region.get(get);
                if (result.isEmpty()) {
                    return getErrorResult(row, maxTimestamp, SQLExceptionCode.SEQUENCE_UNDEFINED.getErrorCode());
                }
                if (validateOnly) {
                    return result;
                }
                
                KeyValue currentValueKV = Sequence.getCurrentValueKV(result);
                KeyValue incrementByKV = Sequence.getIncrementByKV(result);
                KeyValue cacheSizeKV = Sequence.getCacheSizeKV(result);
                
                long currentValue = PLong.INSTANCE.getCodec().decodeLong(currentValueKV.getValueArray(), currentValueKV.getValueOffset(), SortOrder.getDefault());
                long incrementBy = PLong.INSTANCE.getCodec().decodeLong(incrementByKV.getValueArray(), incrementByKV.getValueOffset(), SortOrder.getDefault());
                long cacheSize = PLong.INSTANCE.getCodec().decodeLong(cacheSizeKV.getValueArray(), cacheSizeKV.getValueOffset(), SortOrder.getDefault());
                
                // Hold timestamp constant for sequences, so that clients always only see the latest
                // value regardless of when they connect.
                long timestamp = currentValueKV.getTimestamp();
				Put put = new Put(row, timestamp);
                
				int numIncrementKVs = increment.getFamilyCellMap().get(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES).size();
                // creates the list of KeyValues used for the Result that will be returned
                List<Cell> cells = Sequence.getCells(result, numIncrementKVs);
                
                //if client is 3.0/4.0 preserve the old behavior (older clients won't have newer columns present in the increment)
                if (numIncrementKVs != Sequence.NUM_SEQUENCE_KEY_VALUES) {
                	currentValue += incrementBy * cacheSize;
                    // Hold timestamp constant for sequences, so that clients always only see the latest value
                    // regardless of when they connect.
                    KeyValue newCurrentValueKV = createKeyValue(row, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, currentValue, timestamp);
                    put.add(newCurrentValueKV);
                    Sequence.replaceCurrentValueKV(cells, newCurrentValueKV);
                }
                else {
	                KeyValue cycleKV = Sequence.getCycleKV(result);
	                KeyValue limitReachedKV = Sequence.getLimitReachedKV(result);
	                KeyValue minValueKV = Sequence.getMinValueKV(result);
	                KeyValue maxValueKV = Sequence.getMaxValueKV(result);
	                
	                boolean increasingSeq = incrementBy > 0 ? true : false;
	                
	                // if the minValue, maxValue, cycle and limitReached is null this sequence has been upgraded from
	                // a lower version. Set minValue, maxValue, cycle and limitReached to Long.MIN_VALUE, Long.MAX_VALUE, true and false
	                // respectively in order to maintain existing behavior and also update the KeyValues on the server 
	                boolean limitReached;
	                if (limitReachedKV == null) {
	                	limitReached = false;
	                	KeyValue newLimitReachedKV = createKeyValue(row, PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG_BYTES, limitReached, timestamp);
	                	put.add(newLimitReachedKV);
	                	Sequence.replaceLimitReachedKV(cells, newLimitReachedKV);
	                }
	                else {
	                	limitReached = (Boolean) PBoolean.INSTANCE.toObject(limitReachedKV.getValueArray(),
	                			limitReachedKV.getValueOffset(), limitReachedKV.getValueLength());
	                }
	                long minValue;
	                if (minValueKV == null) {
	                    minValue = Long.MIN_VALUE;
	                    KeyValue newMinValueKV = createKeyValue(row, PhoenixDatabaseMetaData.MIN_VALUE_BYTES, minValue, timestamp);
	                    put.add(newMinValueKV);
	                    Sequence.replaceMinValueKV(cells, newMinValueKV);
	                }
	                else {
	                    minValue = PLong.INSTANCE.getCodec().decodeLong(minValueKV.getValueArray(),
	                                minValueKV.getValueOffset(), SortOrder.getDefault());
	                }           
	                long maxValue;
	                if (maxValueKV == null) {
	                    maxValue = Long.MAX_VALUE;
	                    KeyValue newMaxValueKV = createKeyValue(row, PhoenixDatabaseMetaData.MAX_VALUE_BYTES, maxValue, timestamp);
	                    put.add(newMaxValueKV);
	                    Sequence.replaceMaxValueKV(cells, newMaxValueKV);
	                }
	                else {
	                    maxValue =  PLong.INSTANCE.getCodec().decodeLong(maxValueKV.getValueArray(),
	                            maxValueKV.getValueOffset(), SortOrder.getDefault());
	                }
	                boolean cycle;
	                if (cycleKV == null) {
	                    cycle = false;
	                    KeyValue newCycleKV = createKeyValue(row, PhoenixDatabaseMetaData.CYCLE_FLAG_BYTES, cycle, timestamp);
	                    put.add(newCycleKV);
	                    Sequence.replaceCycleValueKV(cells, newCycleKV);
	                }
	                else {
	                    cycle = (Boolean) PBoolean.INSTANCE.toObject(cycleKV.getValueArray(),
	                            cycleKV.getValueOffset(), cycleKV.getValueLength());
	                }
	                
	                // return if we have run out of sequence values 
					if (limitReached) {
						if (cycle) {
							// reset currentValue of the Sequence row to minValue/maxValue
							currentValue = increasingSeq ? minValue : maxValue;
						}
						else {
							SQLExceptionCode code = increasingSeq ? SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE
									: SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE;
							return getErrorResult(row, maxTimestamp, code.getErrorCode());
						}
					}
	                
	                // check if the limit was reached
					limitReached = SequenceUtil.checkIfLimitReached(currentValue, minValue, maxValue, incrementBy, cacheSize);
	                // update currentValue
					currentValue += incrementBy * cacheSize;
					// update the currentValue of the Result row
					KeyValue newCurrentValueKV = createKeyValue(row, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, currentValue, timestamp);
		            Sequence.replaceCurrentValueKV(cells, newCurrentValueKV);
		            put.add(newCurrentValueKV);
					// set the LIMIT_REACHED column to true, so that no new values can be used
					KeyValue newLimitReachedKV = createKeyValue(row, PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG_BYTES, limitReached, timestamp);
		            put.add(newLimitReachedKV);
                }
                // update the KeyValues on the server
                Mutation[] mutations = new Mutation[]{put};
                region.batchMutate(mutations);
                // return a Result with the updated KeyValues
                return Result.create(cells);
            } finally {
                region.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException("Increment of sequence " + Bytes.toStringBinary(row), t);
            return null; // Impossible
        } finally {
            region.closeRegionOperation();
        }
    }
    
	/**
	 * Creates a new KeyValue for a long value
	 * 
	 * @param key
	 *            key used while creating KeyValue
	 * @param cqBytes
	 *            column qualifier of KeyValue
	 * @return return the KeyValue that was created
	 */
	KeyValue createKeyValue(byte[] key, byte[] cqBytes, long value, long timestamp) {
		byte[] valueBuffer = new byte[PLong.INSTANCE.getByteSize()];
    PLong.INSTANCE.getCodec().encodeLong(value, valueBuffer, 0);
		return KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, cqBytes, timestamp, valueBuffer);
	}
    
	/**
	 * Creates a new KeyValue for a boolean value and adds it to the given put
	 * 
	 * @param key
	 *            key used while creating KeyValue
	 * @param cqBytes
	 *            column qualifier of KeyValue
	 * @return return the KeyValue that was created
	 */
	private KeyValue createKeyValue(byte[] key, byte[] cqBytes, boolean value, long timestamp) throws IOException {
		// create new key value for put
		return KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, cqBytes, 
				timestamp, value ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
	}

    /**
     * Override the preAppend for checkAndPut and checkAndDelete, as we need the ability to
     * a) set the TimeRange for the Get being done and
     * b) return something back to the client to indicate success/failure
     */
    @SuppressWarnings("deprecation")
    @Override
    public Result preAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
            final Append append) throws IOException {
        byte[] opBuf = append.getAttribute(OPERATION_ATTRIB);
        if (opBuf == null) {
            return null;
        }
        Sequence.MetaOp op = Sequence.MetaOp.values()[opBuf[0]];
        Cell keyValue = append.getFamilyCellMap().values().iterator().next().iterator().next();

        long clientTimestamp = HConstants.LATEST_TIMESTAMP;
        long minGetTimestamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
        long maxGetTimestamp = HConstants.LATEST_TIMESTAMP;
        boolean hadClientTimestamp;
        byte[] clientTimestampBuf = null;
        if (op == Sequence.MetaOp.RETURN_SEQUENCE) {
            // When returning sequences, this allows us to send the expected timestamp
            // of the sequence to make sure we don't reset any other sequence
            hadClientTimestamp = true;
            clientTimestamp = minGetTimestamp = keyValue.getTimestamp();
            maxGetTimestamp = minGetTimestamp + 1;
        } else {
            clientTimestampBuf = append.getAttribute(MAX_TIMERANGE_ATTRIB);
            if (clientTimestampBuf != null) {
                clientTimestamp = maxGetTimestamp = Bytes.toLong(clientTimestampBuf);
            }
            hadClientTimestamp = (clientTimestamp != HConstants.LATEST_TIMESTAMP);
            if (hadClientTimestamp) {
                // Prevent race condition of creating two sequences at the same timestamp
                // by looking for a sequence at or after the timestamp at which it'll be
                // created.
                if (op == Sequence.MetaOp.CREATE_SEQUENCE) {
                    maxGetTimestamp = clientTimestamp + 1;
                }            
            } else {
                clientTimestamp = maxGetTimestamp = EnvironmentEdgeManager.currentTimeMillis();
                clientTimestampBuf = Bytes.toBytes(clientTimestamp);
            }
        }

        RegionCoprocessorEnvironment env = e.getEnvironment();
        // We need to set this to prevent region.append from being called
        e.bypass();
        e.complete();
        HRegion region = env.getRegion();
        byte[] row = append.getRow();
        List<RowLock> locks = Lists.newArrayList();
        region.startRegionOperation();
        try {
            acquireLock(region, row, locks);
            try {
                byte[] family = CellUtil.cloneFamily(keyValue);
                byte[] qualifier = CellUtil.cloneQualifier(keyValue);

                Get get = new Get(row);
                get.setTimeRange(minGetTimestamp, maxGetTimestamp);
                get.addColumn(family, qualifier);
                Result result = region.get(get);
                if (result.isEmpty()) {
                    if (op == Sequence.MetaOp.DROP_SEQUENCE || op == Sequence.MetaOp.RETURN_SEQUENCE) {
                        return getErrorResult(row, clientTimestamp, SQLExceptionCode.SEQUENCE_UNDEFINED.getErrorCode());
                    }
                } else {
                    if (op == Sequence.MetaOp.CREATE_SEQUENCE) {
                        return getErrorResult(row, clientTimestamp, SQLExceptionCode.SEQUENCE_ALREADY_EXIST.getErrorCode());
                    }
                }
                Mutation m = null;
                switch (op) {
                case RETURN_SEQUENCE:
                    KeyValue currentValueKV = result.raw()[0];
                    long expectedValue = PLong.INSTANCE.getCodec().decodeLong(append.getAttribute(CURRENT_VALUE_ATTRIB), 0, SortOrder.getDefault());
                    long value = PLong.INSTANCE.getCodec().decodeLong(currentValueKV.getValueArray(),
                      currentValueKV.getValueOffset(), SortOrder.getDefault());
                    // Timestamp should match exactly, or we may have the wrong sequence
                    if (expectedValue != value || currentValueKV.getTimestamp() != clientTimestamp) {
                        return Result.create(Collections.singletonList(
                          (Cell)KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, 
                            QueryConstants.EMPTY_COLUMN_BYTES, currentValueKV.getTimestamp(), ByteUtil.EMPTY_BYTE_ARRAY)));
                    }
                    m = new Put(row, currentValueKV.getTimestamp());
                    m.getFamilyCellMap().putAll(append.getFamilyCellMap());
                    break;
                case DROP_SEQUENCE:
                    m = new Delete(row, clientTimestamp);
                    break;
                case CREATE_SEQUENCE:
                    m = new Put(row, clientTimestamp);
                    m.getFamilyCellMap().putAll(append.getFamilyCellMap());
                    break;
                }
                if (!hadClientTimestamp) {
                    for (List<Cell> kvs : m.getFamilyCellMap().values()) {
                        for (Cell kv : kvs) {
                            ((KeyValue)kv).updateLatestStamp(clientTimestampBuf);
                        }
                    }
                }
                Mutation[] mutations = new Mutation[]{m};
                region.batchMutate(mutations);
                long serverTimestamp = MetaDataUtil.getClientTimeStamp(m);
                // Return result with single KeyValue. The only piece of information
                // the client cares about is the timestamp, which is the timestamp of
                // when the mutation was actually performed (useful in the case of .
                return Result.create(Collections.singletonList(
                  (Cell)KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, serverTimestamp, SUCCESS_VALUE)));
            } finally {
                region.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException("Increment of sequence " + Bytes.toStringBinary(row), t);
            return null; // Impossible
        } finally {
            region.closeRegionOperation();
        }
    }

}
