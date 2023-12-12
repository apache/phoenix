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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
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
 * @since 3.0.0
 */
public class SequenceRegionObserver implements RegionObserver, RegionCoprocessor {
    public static final String OPERATION_ATTRIB = "SEQUENCE_OPERATION";
    public static final String MAX_TIMERANGE_ATTRIB = "MAX_TIMERANGE";
    public static final String CURRENT_VALUE_ATTRIB = "CURRENT_VALUE";
    public static final String NUM_TO_ALLOCATE = "NUM_TO_ALLOCATE";
    private static final byte[] SUCCESS_VALUE = PInteger.INSTANCE.toBytes(Integer.valueOf(Sequence.SUCCESS));

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment)
            throws IOException {
        final RegionCoprocessorEnvironment env = e.getEnvironment();
        final Region region = env.getRegion();
        // We need to set this to prevent region.increment from being called
        e.bypass();
        // Start the region operation before acquiring locks
        region.startRegionOperation();
        final List<Region.RowLock> locks = new ArrayList<>(1);
        ServerUtil.acquireLock(region, increment.getRow(), locks);
        try {
            return doIncrement(region, increment);
        } finally {
            ServerUtil.releaseRowLocks(locks);
            region.closeRegionOperation();
        }
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append)
            throws IOException {
        final RegionCoprocessorEnvironment env = e.getEnvironment();
        final Region region = env.getRegion();
        // We need to set this to prevent region.append from being called
        e.bypass();
        // Start the region operation before acquiring locks
        region.startRegionOperation();
        final List<Region.RowLock> locks = new ArrayList<>(1);
        ServerUtil.acquireLock(region, append.getRow(), locks);
        try {
            return doAppend(region, append);
        } finally {
          ServerUtil.releaseRowLocks(locks);
          region.closeRegionOperation();
        }
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> e,
            MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        final RegionCoprocessorEnvironment env = e.getEnvironment();
        final Region region = env.getRegion();
        final List<Integer> positions = new ArrayList<>();
        // Find all append and increment operations in the batch that must be transformed.
        // The transformation will remove them from the list of operations to process by
        // the normal batch handling so we will not execute them doubly via this hook and
        // the preAppend() or preIncrement() hooks, which must also be registered to capture
        // increment and append actions taken by other API calls.
        for (int i = 0; i < miniBatchOp.size(); i++) {
          // Only consider operations that have not run yet.
          if (miniBatchOp.getOperationStatus(i) != OperationStatus.NOT_RUN) {
              continue;
          }
          // We pass through any mutation that is not an Append or Increment for standard
          // handling.
          final Mutation m = miniBatchOp.getOperation(i);
          if (m instanceof Append || m instanceof Increment) {
              positions.add(i);
          }
        }
        // Early exit if it turns out we don't have any updates for sequences.
        if (positions.isEmpty()) {
            return;
        }
        // Start the region operation before acquiring locks
        region.startRegionOperation();
        final List<Region.RowLock> locks = new ArrayList<>(positions.size());
        try {
            // Lock all the rows we will touch. (We will pass through any mutation that is not an
            // Append or Increment for standard handling.)
            for (int i: positions) {
                ServerUtil.acquireLock(region, miniBatchOp.getOperation(i).getRow(), locks);
            }
            // Now we are ready to execute any submitted append or increment operations ahead of
            // all the others in the batch, substituting Phoenix logic for standard HBase
            // handling. We set the operation status in the mini batch to indicate either success
            // or failure, which indicates to HBase that the batch entry does not need to be
            // processed (again).
            for (int i: positions) {
                final Mutation m = miniBatchOp.getOperation(i);
                Result r = null;
                try {
                    if (m instanceof Append) {
                        r = doAppend(region, (Append) m);
                    } else if (m instanceof Increment) {
                        r = doIncrement(region, (Increment) m);
                    } else {
                        // Should not happen but handle this case if a bug has been introduced.
                        throw new IOException("Unexpected mutation type at batch offset " + i + ": " + m);
                    }
                    miniBatchOp.setOperationStatus(i, new OperationStatus(OperationStatusCode.SUCCESS, r));
                } catch (Exception ex) {
                    miniBatchOp.setOperationStatus(i, new OperationStatus(OperationStatusCode.FAILURE, ex));
                }
            }
        } finally {
            // Release whatever locks we have acquired.
            ServerUtil.releaseRowLocks(locks);
            region.closeRegionOperation();
        }
    }

    /*
     * Overcome deficiencies in Increment implementation (HBASE-10254):
     * 1) Lack of recognition and identification of when the key value to increment doesn't exist
     * 2) Lack of the ability to set the timestamp of the updated key value.
     * Works the same as existing region.increment(), except assumes there is a single column to
     * increment and uses Phoenix LONG encoding.
     * Take a row lock before calling.
     */
    private Result doIncrement(Region region, Increment increment) throws IOException {
        final byte[] row = increment.getRow();
        final TimeRange tr = increment.getTimeRange();
        long maxTimestamp = tr.getMax();
        boolean validateOnly = true;
        try {
            final Get get = new Get(row);
            get.setTimeRange(tr.getMin(), tr.getMax());
            for (Map.Entry<byte[], List<Cell>> entry : increment.getFamilyCellMap().entrySet()) {
                final byte[] cf = entry.getKey();
                for (Cell cq : entry.getValue()) {
                    final long value = Bytes.toLong(cq.getValueArray(), cq.getValueOffset());
                    final long cellTimestamp = cq.getTimestamp();
                    get.addColumn(cf, CellUtil.cloneQualifier(cq));
                    // Workaround HBASE-15698 by using the lowest of the timestamps found
                    // on the Increment or any of its Cells.
                    if (cellTimestamp > 0 && cellTimestamp < maxTimestamp) {
                        maxTimestamp = cellTimestamp;
                        get.setTimeRange(MetaDataProtocol.MIN_TABLE_TIMESTAMP, maxTimestamp);
                    }
                    validateOnly &= (Sequence.ValueOp.VALIDATE_SEQUENCE.ordinal() == value);
                }
            }
            try (RegionScanner scanner = region.getScanner(new Scan(get))) {
                final List<Cell> currentCells = new ArrayList<>();
                scanner.next(currentCells);
                // These cells are returned by this method, and may be backed by ByteBuffers
                // that we free when the RegionScanner is closed on return
                PhoenixKeyValueUtil.maybeCopyCellList(currentCells);
                if (currentCells.isEmpty()) {
                    return getErrorResult(row, maxTimestamp, SQLExceptionCode.SEQUENCE_UNDEFINED.getErrorCode());
                }
                final Result result = Result.create(currentCells);
                final Cell currentValueKV = Sequence.getCurrentValueKV(result);
                final Cell incrementByKV = Sequence.getIncrementByKV(result);
                final Cell cacheSizeKV = Sequence.getCacheSizeKV(result);
                long currentValue = PLong.INSTANCE.getCodec().decodeLong(currentValueKV.getValueArray(),
                    currentValueKV.getValueOffset(), SortOrder.getDefault());
                long incrementBy = PLong.INSTANCE.getCodec().decodeLong(incrementByKV.getValueArray(),
                    incrementByKV.getValueOffset(), SortOrder.getDefault());
                long cacheSize = PLong.INSTANCE.getCodec().decodeLong(cacheSizeKV.getValueArray(),
                    cacheSizeKV.getValueOffset(), SortOrder.getDefault());
                // Hold timestamp constant for sequences, so that clients always only see the latest
                // value regardless of when they connect.
                final long timestamp = currentValueKV.getTimestamp();
                final Put put = new Put(row, timestamp);
                final int numIncrementKVs = increment.getFamilyCellMap()
                    .get(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_FAMILY_BYTES).size();
                // Creates the list of KeyValues used for the Result that will be returned
                final List<Cell> cells = Sequence.getCells(result, numIncrementKVs);
                // if client is 3.0/4.0 preserve the old behavior (older clients won't have newer
                // columns present in the increment)
                if (numIncrementKVs != Sequence.NUM_SEQUENCE_KEY_VALUES) {
                    currentValue += incrementBy * cacheSize;
                    // Hold timestamp constant for sequences, so that clients always only see the
                    // latest value regardless of when they connect.
                    final Cell newCurrentValueKV = createKeyValue(row, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES,
                        currentValue, timestamp);
                    put.add(newCurrentValueKV);
                    Sequence.replaceCurrentValueKV(cells, newCurrentValueKV);
                } else {
                    // New behavior for newer clients.
                    final Cell cycleKV = Sequence.getCycleKV(result);
                    final Cell limitReachedKV = Sequence.getLimitReachedKV(result);
                    final Cell minValueKV = Sequence.getMinValueKV(result);
                    final Cell maxValueKV = Sequence.getMaxValueKV(result);
                    final boolean increasingSeq = incrementBy > 0 ? true : false;
                    // if the minValue, maxValue, cycle and limitReached is null this sequence has
                    // been upgraded from a lower version. Set minValue, maxValue, cycle and
                    // limitReached to Long.MIN_VALUE, Long.MAX_VALUE, true and false respectively
                    // in order to maintain existing behavior and also update the KeyValues on the
                    // server.
                    boolean limitReached;
                    if (limitReachedKV == null) {
                        limitReached = false;
                        Cell newLimitReachedKV =
                            createKeyValue(row, PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG_BYTES, limitReached,
                                timestamp);
                        put.add(newLimitReachedKV);
                        Sequence.replaceLimitReachedKV(cells, newLimitReachedKV);
                    } else {
                        limitReached = (Boolean) PBoolean.INSTANCE.toObject(limitReachedKV.getValueArray(),
                            limitReachedKV.getValueOffset(), limitReachedKV.getValueLength());
                    }
                    long minValue;
                    if (minValueKV == null) {
                        minValue = Long.MIN_VALUE;
                        final Cell newMinValueKV = createKeyValue(row, PhoenixDatabaseMetaData.MIN_VALUE_BYTES,
                            minValue, timestamp);
                        put.add(newMinValueKV);
                        Sequence.replaceMinValueKV(cells, newMinValueKV);
                    } else {
                        minValue = PLong.INSTANCE.getCodec().decodeLong(minValueKV.getValueArray(),
                            minValueKV.getValueOffset(), SortOrder.getDefault());
                    }
                    long maxValue;
                    if (maxValueKV == null) {
                        maxValue = Long.MAX_VALUE;
                        final Cell newMaxValueKV = createKeyValue(row, PhoenixDatabaseMetaData.MAX_VALUE_BYTES,
                            maxValue, timestamp);
                        put.add(newMaxValueKV);
                        Sequence.replaceMaxValueKV(cells, newMaxValueKV);
                    } else {
                        maxValue =  PLong.INSTANCE.getCodec().decodeLong(maxValueKV.getValueArray(),
                            maxValueKV.getValueOffset(), SortOrder.getDefault());
                    }
                    boolean cycle;
                    if (cycleKV == null) {
                        cycle = false;
                        final Cell newCycleKV = createKeyValue(row, PhoenixDatabaseMetaData.CYCLE_FLAG_BYTES,
                            cycle, timestamp);
                        put.add(newCycleKV);
                        Sequence.replaceCycleValueKV(cells, newCycleKV);
                    } else {
                        cycle = (Boolean) PBoolean.INSTANCE.toObject(cycleKV.getValueArray(),
                            cycleKV.getValueOffset(), cycleKV.getValueLength());
                    }
                    final long numSlotsToAllocate = calculateNumSlotsToAllocate(increment);
                    // We do not support Bulk Allocations on sequences that have the CYCLE flag
                    // set to true.
                    if (cycle && !SequenceUtil.isCycleAllowed(numSlotsToAllocate)) {
                        return getErrorResult(row, maxTimestamp,
                            SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_NOT_SUPPORTED.getErrorCode());
                    }
                    // Bulk Allocations are expressed by NEXT <n> VALUES FOR
                    if (SequenceUtil.isBulkAllocation(numSlotsToAllocate)) {
                        if (SequenceUtil.checkIfLimitReached(currentValue, minValue, maxValue, incrementBy,
                                cacheSize, numSlotsToAllocate)) {
                            // If we try to allocate more slots than the limit we return an error.
                            // Allocating sequence values in bulk should be an all or nothing operation.
                            // If the operation succeeds clients are guaranteed that they have reserved
                            // all the slots requested.
                          return getErrorResult(row, maxTimestamp,
                              SequenceUtil.getLimitReachedErrorCode(increasingSeq).getErrorCode());
                        }
                    }
                    if (validateOnly) {
                        return result;
                    }
                    // return if we have run out of sequence values
                    if (limitReached) {
                        if (cycle) {
                            // reset currentValue of the Sequence row to minValue/maxValue
                            currentValue = increasingSeq ? minValue : maxValue;
                        } else {
                            return getErrorResult(row, maxTimestamp,
                                SequenceUtil.getLimitReachedErrorCode(increasingSeq).getErrorCode());
                        }
                    }
                    // check if the limit was reached
                    limitReached = SequenceUtil.checkIfLimitReached(currentValue, minValue, maxValue,
                        incrementBy, cacheSize, numSlotsToAllocate);
                    // update currentValue
                    currentValue += incrementBy * (SequenceUtil.isBulkAllocation(numSlotsToAllocate) ?
                        numSlotsToAllocate : cacheSize);
                    // update the currentValue of the Result row
                    final Cell newCurrentValueKV = createKeyValue(row, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES,
                        currentValue, timestamp);
                    Sequence.replaceCurrentValueKV(cells, newCurrentValueKV);
                    put.add(newCurrentValueKV);
                    // set the LIMIT_REACHED column to true, so that no new values can be used
                    final Cell newLimitReachedKV = createKeyValue(row, PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG_BYTES,
                        limitReached, timestamp);
                    put.add(newLimitReachedKV);
                }
                // update the KeyValues on the server
                final Mutation[] mutations = new Mutation[]{put};
                region.batchMutate(mutations);
                // return a Result with the updated KeyValues
                return Result.create(cells);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException("Increment of sequence " + Bytes.toStringBinary(row), t);
            return null; // Impossible
        }
    }

    /*
     * Override the preAppend for checkAndPut and checkAndDelete, as we need the ability to
     * a) set the TimeRange for the Get being done and
     * b) return something back to the client to indicate success/failure
     * Take a row lock before calling.
     */
    private Result doAppend(Region region, Append append) throws IOException {
        final byte[] opBuf = append.getAttribute(OPERATION_ATTRIB);
        if (opBuf == null) {
            return null;
        }
        final Sequence.MetaOp op = Sequence.MetaOp.values()[opBuf[0]];
        // TODO: Fix next line, it looks like magic
        final Cell keyValue = append.getFamilyCellMap().values().iterator().next().iterator().next();
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
                clientTimestamp = EnvironmentEdgeManager.currentTimeMillis();
                maxGetTimestamp = clientTimestamp + 1;
                clientTimestampBuf = Bytes.toBytes(clientTimestamp);
            }
        }
        final byte[] row = append.getRow();
        final byte[] family = CellUtil.cloneFamily(keyValue);
        final byte[] qualifier = CellUtil.cloneQualifier(keyValue);
        final Get get = new Get(row);
        get.setTimeRange(minGetTimestamp, maxGetTimestamp);
        get.addColumn(family, qualifier);
        try (RegionScanner scanner = region.getScanner(new Scan(get))) {
            final List<Cell> cells = new ArrayList<>();
            scanner.next(cells);
            if (cells.isEmpty()) {
                if (op == Sequence.MetaOp.DROP_SEQUENCE || op == Sequence.MetaOp.RETURN_SEQUENCE) {
                    return getErrorResult(row, clientTimestamp,
                        SQLExceptionCode.SEQUENCE_UNDEFINED.getErrorCode());
                }
            } else {
                if (op == Sequence.MetaOp.CREATE_SEQUENCE) {
                    return getErrorResult(row, clientTimestamp,
                        SQLExceptionCode.SEQUENCE_ALREADY_EXIST.getErrorCode());
                }
            }
            Mutation m = null;
            switch (op) {
                case RETURN_SEQUENCE:
                    final KeyValue currentValueKV = PhoenixKeyValueUtil.maybeCopyCell(cells.get(0));
                    final long expectedValue = PLong.INSTANCE.getCodec()
                        .decodeLong(append.getAttribute(CURRENT_VALUE_ATTRIB), 0, SortOrder.getDefault());
                    final long value = PLong.INSTANCE.getCodec().decodeLong(currentValueKV.getValueArray(),
                    currentValueKV.getValueOffset(), SortOrder.getDefault());
                    // Timestamp should match exactly, or we may have the wrong sequence
                    if (expectedValue != value || currentValueKV.getTimestamp() != clientTimestamp) {
                        return Result.create(Collections.singletonList(
                            PhoenixKeyValueUtil.newKeyValue(row,
                                PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_FAMILY_BYTES,
                                QueryConstants.EMPTY_COLUMN_BYTES,
                                currentValueKV.getTimestamp(),
                                ByteUtil.EMPTY_BYTE_ARRAY)));
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
                        ((ExtendedCell)kv).setTimestamp(clientTimestampBuf);
                    }
                }
            }
            final Mutation[] mutations = new Mutation[]{m};
            region.batchMutate(mutations);
            long serverTimestamp = MetaDataUtil.getClientTimeStamp(m);
            // Return result with single KeyValue. The only piece of information
            // the client cares about is the timestamp, which is the timestamp of
            // when the mutation was actually performed (useful in the case of .
            return Result.create(Collections.singletonList(
                PhoenixKeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_FAMILY_BYTES,
                    QueryConstants.EMPTY_COLUMN_BYTES, serverTimestamp, SUCCESS_VALUE)));
        } catch (Throwable t) {
            ServerUtil.throwIOException("Increment of sequence " + Bytes.toStringBinary(row), t);
            return null; // Impossible
        }
    }

    private static Result getErrorResult(byte[] row, long timestamp, int errorCode) {
        byte[] errorCodeBuf = new byte[PInteger.INSTANCE.getByteSize()];
        PInteger.INSTANCE.getCodec().encodeInt(errorCode, errorCodeBuf, 0);
        return Result.create(Collections.singletonList(
            PhoenixKeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_FAMILY_BYTES,
                QueryConstants.EMPTY_COLUMN_BYTES, timestamp, errorCodeBuf)));
    }

	  Cell createKeyValue(byte[] key, byte[] cqBytes, long value, long timestamp) {
		byte[] valueBuffer = new byte[PLong.INSTANCE.getByteSize()];
		    PLong.INSTANCE.getCodec().encodeLong(value, valueBuffer, 0);
		    return PhoenixKeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_FAMILY_BYTES,
		        cqBytes, timestamp, valueBuffer);
	}

	  private Cell createKeyValue(byte[] key, byte[] cqBytes, boolean value, long timestamp)
	          throws IOException {
		// create new key value for put
		  return PhoenixKeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_FAMILY_BYTES,
		      cqBytes,  timestamp, value ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
	}

    /**
     * Determines whether a request for incrementing the sequence was a bulk allocation and if so
     * what the number of slots to allocate is. This is triggered by the NEXT <n> VALUES FOR expression.
     * For backwards compatibility with older clients, we default the value to 1 which preserves
     * existing behavior when invoking NEXT VALUE FOR. 
     */
    private long calculateNumSlotsToAllocate(final Increment increment) {
        long numToAllocate = 1;
        byte[] numToAllocateBytes = increment.getAttribute(SequenceRegionObserver.NUM_TO_ALLOCATE);
        if (numToAllocateBytes != null) {
            numToAllocate = Bytes.toLong(numToAllocateBytes);
        }
        return numToAllocate;
    }

}
