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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SequenceUtil;
import org.apache.phoenix.util.ServerUtil;

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
    private static final byte[] SUCCESS_VALUE = PDataType.INTEGER.toBytes(Integer.valueOf(Sequence.SUCCESS));
    
    private static Result getErrorResult(byte[] row, long timestamp, int errorCode) {
        byte[] errorCodeBuf = new byte[PDataType.INTEGER.getByteSize()];
        PDataType.INTEGER.getCodec().encodeInt(errorCode, errorCodeBuf, 0);
        return new Result(Collections.singletonList(
                KeyValueUtil.newKeyValue(row, 
                        PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, 
                        QueryConstants.EMPTY_COLUMN_BYTES, timestamp, errorCodeBuf)));
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
        TimeRange tr = increment.getTimeRange();
        region.startRegionOperation();
        try {
            Integer lid = region.getLock(null, row, true);
            try {
                long maxTimestamp = tr.getMax();
                boolean validateOnly = true;
                Get get = new Get(row);
                get.setTimeRange(tr.getMin(), tr.getMax());
                for (Map.Entry<byte[],NavigableMap<byte[], Long>> entry : increment.getFamilyMap().entrySet()) {
                    byte[] cf = entry.getKey();
                    for (Map.Entry<byte[],Long> kvEntry : entry.getValue().entrySet()) {
                        get.addColumn(cf, kvEntry.getKey());
                        validateOnly &= (Sequence.ValueOp.VALIDATE_SEQUENCE.ordinal() == kvEntry.getValue().intValue());
                    }
                }
                Result result = region.get(get);
                if (result.isEmpty()) {
                    return getErrorResult(row, maxTimestamp, SQLExceptionCode.SEQUENCE_UNDEFINED.getErrorCode());
                }
                if (validateOnly) {
                    return result;
                }
                
                KeyValue incrementByKV = Sequence.getIncrementByKV(result);
                KeyValue currentValueKV = Sequence.getCurrentValueKV(result);               
                KeyValue cacheSizeKV = Sequence.getCacheSizeKV(result);        
                KeyValue cycleKV = Sequence.getCycleKV(result);
                KeyValue minValueKV = Sequence.getMinValueKV(result);
                KeyValue maxValueKV = Sequence.getMaxValueKV(result);
                
                // Hold timestamp constant for sequences, so that clients always only see the latest
                // value regardless of when they connect.
                Put put = new Put(row, currentValueKV.getTimestamp());
                
                // create a copy of the key values, used for the new Return
                List<KeyValue> newkvs = Sequence.getCells(result);
                
                long incrementBy =
                        PDataType.LONG.getCodec().decodeLong(incrementByKV.getBuffer(),
                            incrementByKV.getValueOffset(), SortOrder.getDefault());
                     
                long cacheSize =
                        PDataType.LONG.getCodec().decodeLong(cacheSizeKV.getBuffer(),
                            cacheSizeKV.getValueOffset(), SortOrder.getDefault());
                
                // if the minValue, maxValue, or cycle is null this sequence has been upgraded from
                // a lower version. Set minValue, maxValue and cycle to Long.MIN_VALUE, Long.MAX_VALUE and true 
                // respectively in order to maintain existing behavior and also update the KeyValues on the server 
                long minValue;
                if (minValueKV == null) {
                    minValue = Long.MIN_VALUE;
                    // create new key value for put
                    byte[] newMinValueBuffer = new byte[PDataType.LONG.getByteSize()];
                    PDataType.LONG.getCodec().encodeLong(minValue, newMinValueBuffer, 0);
                    KeyValue newMinValueKV = KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES,
                                PhoenixDatabaseMetaData.MIN_VALUE_BYTES, currentValueKV.getTimestamp(), newMinValueBuffer);
                    put.add(newMinValueKV);
                    // update key value in returned Result
                    Sequence.replaceMinValueKV(newkvs, newMinValueKV);
                }
                else {
                    minValue = PDataType.LONG.getCodec().decodeLong(minValueKV.getBuffer(),
                                minValueKV.getValueOffset(), SortOrder.getDefault());
                }           
                long maxValue;
                if (maxValueKV == null) {
                    maxValue = Long.MAX_VALUE;
                    // create new key value for put
                    byte[] newMaxValueBuffer = new byte[PDataType.LONG.getByteSize()];
                    PDataType.LONG.getCodec().encodeLong(maxValue, newMaxValueBuffer, 0);
                    KeyValue newMaxValueKV = KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.MAX_VALUE_BYTES, currentValueKV.getTimestamp(), newMaxValueBuffer);
                    put.add(newMaxValueKV);
                    // update key value in returned Result
                    Sequence.replaceMaxValueKV(newkvs, newMaxValueKV);
                }
                else {
                    maxValue =  PDataType.LONG.getCodec().decodeLong(maxValueKV.getBuffer(),
                            maxValueKV.getValueOffset(), SortOrder.getDefault());
                }
                boolean cycle;
                if (cycleKV == null) {
                    cycle = false;
                    // create new key value for put
                    KeyValue newCycleKV = KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.CYCLE_FLAG_BYTES, currentValueKV.getTimestamp(), PDataType.FALSE_BYTES);
                    put.add(newCycleKV);
                    // update key value in returned Result
                    Sequence.replaceCycleValueKV(newkvs, newCycleKV);
                }
                else {
                    cycle = (Boolean) PDataType.BOOLEAN.toObject(cycleKV.getBuffer(),
                            cycleKV.getValueOffset(), cycleKV.getValueLength());
                }
                long currentValue;
                // initialize current value to start value
                if (currentValueKV.getValueLength()==0) {
                    KeyValue startValueKV = Sequence.getStartValueKV(result);
                    currentValue =
                            PDataType.LONG.getCodec().decodeLong(startValueKV.getBuffer(),
                                startValueKV.getValueOffset(), SortOrder.getDefault());
                }
                else {
                    currentValue =
                            PDataType.LONG.getCodec().decodeLong(currentValueKV.getBuffer(),
                                currentValueKV.getValueOffset(), SortOrder.getDefault());      
                    try {
                        // set currentValue to nextValue
                        currentValue =
                                SequenceUtil.getNextValue(currentValue, minValue, maxValue,
                                    incrementBy, cacheSize, cycle);
                    } catch (SQLException sqlE) {
                        return getErrorResult(row, maxTimestamp, sqlE.getErrorCode());
                    }
                }
                byte[] newCurrentValueBuffer = new byte[PDataType.LONG.getByteSize()];
                PDataType.LONG.getCodec().encodeLong(currentValue, newCurrentValueBuffer, 0);
                KeyValue newCurrentValueKV = KeyValueUtil.newKeyValue(row, currentValueKV, newCurrentValueBuffer);
                put.add(newCurrentValueKV);
                Sequence.replaceCurrentValueKV(newkvs, newCurrentValueKV);
                
                // update the KeyValues on the server
                @SuppressWarnings("unchecked")
                Pair<Mutation,Integer>[] mutations = new Pair[1];
                mutations[0] = new Pair<Mutation,Integer>(put, lid);
                region.batchMutate(mutations);
                // return a Result with the updated KeyValues
                return new Result(newkvs);
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException("Increment of sequence " + Bytes.toStringBinary(row), t);
            return null; // Impossible
        } finally {
            region.closeRegionOperation();
        }
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
        KeyValue keyValue = append.getFamilyMap().values().iterator().next().iterator().next();

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
        region.startRegionOperation();
        try {
            Integer lid = region.getLock(null, row, true);
            try {
                byte[] family = keyValue.getFamily();
                byte[] qualifier = keyValue.getQualifier();

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
                    long expectedValue = PDataType.LONG.getCodec().decodeLong(append.getAttribute(CURRENT_VALUE_ATTRIB), 0, SortOrder.getDefault());
                    long value = PDataType.LONG.getCodec().decodeLong(currentValueKV.getBuffer(), currentValueKV.getValueOffset(), SortOrder.getDefault());
                    // Timestamp should match exactly, or we may have the wrong sequence
                    if (expectedValue != value || currentValueKV.getTimestamp() != clientTimestamp) {
                        return new Result(Collections.singletonList(KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, currentValueKV.getTimestamp(), ByteUtil.EMPTY_BYTE_ARRAY)));
                    }
                    m = new Put(row, currentValueKV.getTimestamp());
                    m.getFamilyMap().putAll(append.getFamilyMap());
                    break;
                case DROP_SEQUENCE:
                    m = new Delete(row, clientTimestamp, null);
                    break;
                case CREATE_SEQUENCE:
                    m = new Put(row, clientTimestamp);
                    m.getFamilyMap().putAll(append.getFamilyMap());
                    break;
                }
                if (!hadClientTimestamp) {
                    for (List<KeyValue> kvs : m.getFamilyMap().values()) {
                        for (KeyValue kv : kvs) {
                            kv.updateLatestStamp(clientTimestampBuf);
                        }
                    }
                }
                @SuppressWarnings("unchecked")
                Pair<Mutation,Integer>[] mutations = new Pair[1];
                mutations[0] = new Pair<Mutation,Integer>(m, lid);
                region.batchMutate(mutations);
                long serverTimestamp = MetaDataUtil.getClientTimeStamp(m);
                // Return result with single KeyValue. The only piece of information
                // the client cares about is the timestamp, which is the timestamp of
                // when the mutation was actually performed (useful in the case of .
                return new Result(Collections.singletonList(KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, serverTimestamp, SUCCESS_VALUE)));
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException("Increment of sequence " + Bytes.toStringBinary(row), t);
            return null; // Impossible
        } finally {
            region.closeRegionOperation();
        }
    }

}
