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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_OLD_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.REBUILT_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RESULT_TABLE_COLUMN_FAMILY;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.SCANNED_DATA_ROW_COUNT_BYTES;

public class IndexToolVerificationResult {

    public void setScannedDataRowCount(long scannedDataRowCount) {
        this.scannedDataRowCount = scannedDataRowCount;
    }

    public void setRebuiltIndexRowCount(long rebuiltIndexRowCount) {
        this.rebuiltIndexRowCount = rebuiltIndexRowCount;
    }

    public PhaseResult getBefore() {
        return before;
    }

    public void setBefore(PhaseResult before) {
        this.before = before;
    }

    public PhaseResult getAfter() {
        return after;
    }

    public void setAfter(PhaseResult after) {
        this.after = after;
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public byte[] getStopRow() {
        return stopRow;
    }

    public long getScanMaxTs() {
        return scanMaxTs;
    }

    public IndexToolVerificationResult(long scanMaxTs) {
        this.scanMaxTs = scanMaxTs;
    }

    public IndexToolVerificationResult(byte[] startRow, byte[] stopRow, long scanMaxTs) {
        this.setStartRow(startRow);
        this.setStopRow(stopRow);
        this.scanMaxTs = scanMaxTs;
    }

    public IndexToolVerificationResult(Scan scan) {
        this.setStartRow(scan.getStartRow());
        this.setStopRow(scan.getStopRow());
        this.scanMaxTs = scan.getTimeRange().getMax();
    }

    public byte[] getRegion() {
        return region;
    }

    public void setStartRow(byte[] startRow) {
        this.startRow = startRow;
    }

    public void setStopRow(byte[] stopRow) {
        this.stopRow = stopRow;
    }

    public static class PhaseResult {
        private long validIndexRowCount = 0;
        private long expiredIndexRowCount = 0;
        private long missingIndexRowCount = 0;
        private long invalidIndexRowCount = 0;
        private long beyondMaxLookBackMissingIndexRowCount = 0;
        private long beyondMaxLookBackInvalidIndexRowCount = 0;
        private long indexHasExtraCellsCount = 0;
        private long indexHasMissingCellsCount = 0;
        private long unverifiedIndexRowCount = 0;
        private long oldIndexRowCount = 0;
        private long unknownIndexRowCount = 0;
        private long extraVerifiedIndexRowCount = 0;
        private long extraUnverifiedIndexRowCount = 0;

        public void add(PhaseResult phaseResult) {
            setBeyondMaxLookBackMissingIndexRowCount(getBeyondMaxLookBackMissingIndexRowCount() +
                phaseResult.getBeyondMaxLookBackMissingIndexRowCount());
            setBeyondMaxLookBackInvalidIndexRowCount(getBeyondMaxLookBackInvalidIndexRowCount() +
                phaseResult.getBeyondMaxLookBackInvalidIndexRowCount());
            setValidIndexRowCount(getValidIndexRowCount() + phaseResult.getValidIndexRowCount());
            setExpiredIndexRowCount(getExpiredIndexRowCount() + phaseResult.getExpiredIndexRowCount());
            setMissingIndexRowCount(getMissingIndexRowCount() + phaseResult.getMissingIndexRowCount());
            setInvalidIndexRowCount(getInvalidIndexRowCount() + phaseResult.getInvalidIndexRowCount());
            setIndexHasExtraCellsCount(getIndexHasExtraCellsCount() + phaseResult.getIndexHasExtraCellsCount());
            setIndexHasMissingCellsCount(getIndexHasMissingCellsCount() + phaseResult.getIndexHasMissingCellsCount());
            setUnverifiedIndexRowCount(getUnverifiedIndexRowCount() + phaseResult.getUnverifiedIndexRowCount());
            setUnknownIndexRowCount(getUnknownIndexRowCount() + phaseResult.getUnknownIndexRowCount());
            setOldIndexRowCount(getOldIndexRowCount() + phaseResult.getOldIndexRowCount());
            setExtraVerifiedIndexRowCount(getExtraVerifiedIndexRowCount() +
                phaseResult.getExtraVerifiedIndexRowCount());
            setExtraUnverifiedIndexRowCount(getExtraUnverifiedIndexRowCount() +
                phaseResult.getExtraUnverifiedIndexRowCount());
        }

        public PhaseResult() {
        }

        public PhaseResult(long validIndexRowCount, long expiredIndexRowCount,
            long missingIndexRowCount, long invalidIndexRowCount,
            long beyondMaxLookBackMissingIndexRowCount,
            long beyondMaxLookBackInvalidIndexRowCount,
            long indexHasExtraCellsCount, long indexHasMissingCellsCount,
            long extraVerifiedIndexRowCount, long extraUnverifiedIndexRowCount) {
            this.setValidIndexRowCount(validIndexRowCount);
            this.setExpiredIndexRowCount(expiredIndexRowCount);
            this.setMissingIndexRowCount(missingIndexRowCount);
            this.setInvalidIndexRowCount(invalidIndexRowCount);
            this.setBeyondMaxLookBackInvalidIndexRowCount(beyondMaxLookBackInvalidIndexRowCount);
            this.setBeyondMaxLookBackMissingIndexRowCount(beyondMaxLookBackMissingIndexRowCount);
            this.setIndexHasExtraCellsCount(indexHasExtraCellsCount);
            this.setIndexHasMissingCellsCount(indexHasMissingCellsCount);
            this.setExtraVerifiedIndexRowCount(extraVerifiedIndexRowCount);
            this.setExtraUnverifiedIndexRowCount(extraUnverifiedIndexRowCount);
        }


        public long getTotalCount() {
            return getValidIndexRowCount() + getExpiredIndexRowCount() + getMissingIndexRowCount() + getInvalidIndexRowCount()
                + getBeyondMaxLookBackMissingIndexRowCount() + getBeyondMaxLookBackInvalidIndexRowCount();
        }

        public long getIndexHasExtraCellsCount() {
            return indexHasExtraCellsCount;
        }

        public long getIndexHasMissingCellsCount() {
            return indexHasMissingCellsCount;
        }

        public long getTotalExtraIndexRowsCount() {
            return getExtraVerifiedIndexRowCount() + getExtraUnverifiedIndexRowCount() ;
        }

        @Override
        public String toString() {
            return "PhaseResult{" +

                    "validIndexRowCount=" + validIndexRowCount +
                    ", expiredIndexRowCount=" + expiredIndexRowCount +
                    ", missingIndexRowCount=" + missingIndexRowCount +
                    ", invalidIndexRowCount=" + invalidIndexRowCount +
                    ", beyondMaxLookBackMissingIndexRowCount=" + getBeyondMaxLookBackMissingIndexRowCount() +
                    ", beyondMaxLookBackInvalidIndexRowCount=" + getBeyondMaxLookBackInvalidIndexRowCount() +
                    ", extraCellsOnIndexCount=" + indexHasExtraCellsCount +
                    ", missingCellsOnIndexCount=" + indexHasMissingCellsCount +
                    ", unverifiedIndexRowCount=" + unverifiedIndexRowCount +
                    ", oldIndexRowCount=" + oldIndexRowCount +
                    ", unknownIndexRowCount=" + unknownIndexRowCount +
                    ", extraVerifiedIndexRowCount=" + extraVerifiedIndexRowCount +
                    ", extraUnverifiedIndexRowCount=" + extraUnverifiedIndexRowCount +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (!(o instanceof PhaseResult)) {
                return false;
            }
            PhaseResult pr = (PhaseResult) o;

            return this.expiredIndexRowCount == pr.expiredIndexRowCount
                    && this.validIndexRowCount == pr.validIndexRowCount
                    && this.invalidIndexRowCount == pr.invalidIndexRowCount
                    && this.missingIndexRowCount == pr.missingIndexRowCount
                    && this.beyondMaxLookBackInvalidIndexRowCount == pr.beyondMaxLookBackInvalidIndexRowCount
                    && this.beyondMaxLookBackMissingIndexRowCount== pr.beyondMaxLookBackMissingIndexRowCount
                    && this.indexHasMissingCellsCount == pr.indexHasMissingCellsCount
                    && this.indexHasExtraCellsCount == pr.indexHasExtraCellsCount
                    && this.oldIndexRowCount == pr.oldIndexRowCount
                    && this.unknownIndexRowCount == pr.unknownIndexRowCount
                    && this.extraVerifiedIndexRowCount == pr.extraVerifiedIndexRowCount
                    && this.extraUnverifiedIndexRowCount == pr.extraUnverifiedIndexRowCount;
        }

        @Override
        public int hashCode() {
            long result = 17;
            result = 31 * result + getExpiredIndexRowCount();
            result = 31 * result + getValidIndexRowCount();
            result = 31 * result + getMissingIndexRowCount();
            result = 31 * result + getInvalidIndexRowCount();
            result = 31 * result + getBeyondMaxLookBackMissingIndexRowCount();
            result = 31 * result + getBeyondMaxLookBackInvalidIndexRowCount();
            result = 31 * result + getIndexHasMissingCellsCount();
            result = 31 * result + getIndexHasExtraCellsCount();
            result = 31 * result + getUnverifiedIndexRowCount();
            result = 31 * result + getOldIndexRowCount();
            result = 31 * result + getUnknownIndexRowCount();
            result = 31 * result + getExtraVerifiedIndexRowCount();
            result = 31 * result + getExtraUnverifiedIndexRowCount();
            return (int) result;
        }

        public long getValidIndexRowCount() {
            return validIndexRowCount;
        }

        public void setValidIndexRowCount(long validIndexRowCount) {
            this.validIndexRowCount = validIndexRowCount;
        }

        public long getExpiredIndexRowCount() {
            return expiredIndexRowCount;
        }

        public void setExpiredIndexRowCount(long expiredIndexRowCount) {
            this.expiredIndexRowCount = expiredIndexRowCount;
        }

        public long getMissingIndexRowCount() {
            return missingIndexRowCount;
        }

        public void setMissingIndexRowCount(long missingIndexRowCount) {
            this.missingIndexRowCount = missingIndexRowCount;
        }

        public long getInvalidIndexRowCount() {
            return invalidIndexRowCount;
        }

        public void setInvalidIndexRowCount(long invalidIndexRowCount) {
            this.invalidIndexRowCount = invalidIndexRowCount;
        }

        public long getBeyondMaxLookBackMissingIndexRowCount() {
            return beyondMaxLookBackMissingIndexRowCount;
        }

        public void setBeyondMaxLookBackMissingIndexRowCount(long beyondMaxLookBackMissingIndexRowCount) {
            this.beyondMaxLookBackMissingIndexRowCount = beyondMaxLookBackMissingIndexRowCount;
        }

        public long getBeyondMaxLookBackInvalidIndexRowCount() {
            return beyondMaxLookBackInvalidIndexRowCount;
        }

        public void setBeyondMaxLookBackInvalidIndexRowCount(long beyondMaxLookBackInvalidIndexRowCount) {
            this.beyondMaxLookBackInvalidIndexRowCount = beyondMaxLookBackInvalidIndexRowCount;
        }

        public void setIndexHasMissingCellsCount(long indexHasMissingCellsCount) {
            this.indexHasMissingCellsCount = indexHasMissingCellsCount;
        }

        public void setIndexHasExtraCellsCount(long indexHasExtraCellsCount) {
            this.indexHasExtraCellsCount = indexHasExtraCellsCount;
        }

        public long getUnverifiedIndexRowCount() {
            return unverifiedIndexRowCount;
        }

        public void setUnverifiedIndexRowCount(long unverifiedIndexRowCount) {
            this.unverifiedIndexRowCount = unverifiedIndexRowCount;
        }

        public long getOldIndexRowCount() {
            return oldIndexRowCount;
        }

        public void setOldIndexRowCount(long oldIndexRowCount) {
            this.oldIndexRowCount = oldIndexRowCount;
        }

        public long getUnknownIndexRowCount() {
            return unknownIndexRowCount;
        }

        public void setUnknownIndexRowCount(long unknownIndexRowCount) {
            this.unknownIndexRowCount = unknownIndexRowCount;
        }

        public long getExtraVerifiedIndexRowCount() { return extraVerifiedIndexRowCount; }

        public void setExtraVerifiedIndexRowCount(long extraVerifiedIndexRowCount) {
            this.extraVerifiedIndexRowCount = extraVerifiedIndexRowCount;
        }

        public long getExtraUnverifiedIndexRowCount() { return extraUnverifiedIndexRowCount; }

        public void setExtraUnverifiedIndexRowCount(long extraUnverifiedIndexRowCount) {
            this.extraUnverifiedIndexRowCount = extraUnverifiedIndexRowCount;
        }
    }

    private long scannedDataRowCount = 0;
    private long rebuiltIndexRowCount = 0;
    private byte[] startRow;
    private byte[] stopRow;
    private long scanMaxTs;
    private byte[] region;
    private PhaseResult before = new PhaseResult();
    private PhaseResult after = new PhaseResult();

    @Override
    public String toString() {
        return "VerificationResult{" +
                "scannedDataRowCount=" + getScannedDataRowCount() +
                ", rebuiltIndexRowCount=" + getRebuiltIndexRowCount() +
                ", before=" + getBefore() +
                ", after=" + getAfter() +
                '}';
    }

    public long getScannedDataRowCount() {
        return scannedDataRowCount;
    }

    public long getRebuiltIndexRowCount() {
        return rebuiltIndexRowCount;
    }

    public long getBeforeRebuildValidIndexRowCount() {
        return getBefore().getValidIndexRowCount();
    }

    public long getBeforeRebuildExpiredIndexRowCount() {
        return getBefore().getExpiredIndexRowCount();
    }

    public long getBeforeRebuildInvalidIndexRowCount() {
        return getBefore().getInvalidIndexRowCount();
    }

    public long getBeforeRebuildUnverifiedIndexRowCount() {
        return getBefore().getUnverifiedIndexRowCount();
    }

    public long getBeforeRebuildOldIndexRowCount() {
        return getBefore().getOldIndexRowCount();
    }

    public long getBeforeRebuildUnknownIndexRowCount() {
        return getBefore().getUnknownIndexRowCount();
    }

    public long getBeforeRebuildBeyondMaxLookBackMissingIndexRowCount() {
        return before.getBeyondMaxLookBackMissingIndexRowCount();
    }

    public long getBeforeRebuildBeyondMaxLookBackInvalidIndexRowCount() {
        return before.getBeyondMaxLookBackInvalidIndexRowCount();
    }

    public long getBeforeRebuildMissingIndexRowCount() {
        return getBefore().getMissingIndexRowCount();
    }

    public long getBeforeIndexHasMissingCellsCount() {return getBefore().getIndexHasMissingCellsCount(); }

    public long getBeforeIndexHasExtraCellsCount() {return getBefore().getIndexHasExtraCellsCount(); }

    public long getBeforeRepairExtraVerifiedIndexRowCount() { return getBefore().getExtraVerifiedIndexRowCount(); }

    public long getBeforeRepairExtraUnverifiedIndexRowCount() { return getBefore().getExtraUnverifiedIndexRowCount(); }

    public long getAfterRebuildValidIndexRowCount() {
        return getAfter().getValidIndexRowCount();
    }

    public long getAfterRebuildExpiredIndexRowCount() {
        return getAfter().getExpiredIndexRowCount();
    }

    public long getAfterRebuildInvalidIndexRowCount() {
        return getAfter().getInvalidIndexRowCount();
    }

    public long getAfterRebuildMissingIndexRowCount() {
        return getAfter().getMissingIndexRowCount();
    }

    public long getAfterRebuildBeyondMaxLookBackMissingIndexRowCount() {
        return after.getBeyondMaxLookBackMissingIndexRowCount();
    }

    public long getAfterRebuildBeyondMaxLookBackInvalidIndexRowCount() {
        return after.getBeyondMaxLookBackInvalidIndexRowCount();
    }

    public long getAfterIndexHasMissingCellsCount() { return getAfter().getIndexHasMissingCellsCount(); }

    public long getAfterIndexHasExtraCellsCount() { return getAfter().getIndexHasExtraCellsCount(); }

    public long getAfterRepairExtraVerifiedIndexRowCount() { return getAfter().getExtraVerifiedIndexRowCount(); }

    public long getAfterRepairExtraUnverifiedIndexRowCount() { return getAfter().getExtraUnverifiedIndexRowCount(); }

    private void addScannedDataRowCount(long count) {
        this.setScannedDataRowCount(this.getScannedDataRowCount() + count);
    }

    private void addRebuiltIndexRowCount(long count) {
        this.setRebuiltIndexRowCount(this.getRebuiltIndexRowCount() + count);
    }

    private void addBeforeRebuildValidIndexRowCount(long count) {
        getBefore().setValidIndexRowCount(getBefore().getValidIndexRowCount() + count);
    }

    private void addBeforeRebuildExpiredIndexRowCount(long count) {
        getBefore().setExpiredIndexRowCount(getBefore().getExpiredIndexRowCount() + count);
    }

    private void addBeforeRebuildMissingIndexRowCount(long count) {
        getBefore().setMissingIndexRowCount(getBefore().getMissingIndexRowCount() + count);
    }

    private void addBeforeRebuildInvalidIndexRowCount(long count) {
        getBefore().setInvalidIndexRowCount(getBefore().getInvalidIndexRowCount() + count);
    }

    private void addBeforeRebuildBeyondMaxLookBackMissingIndexRowCount(long count) {
        before.setBeyondMaxLookBackMissingIndexRowCount(before.getBeyondMaxLookBackMissingIndexRowCount() + count);
    }

    private void addBeforeRebuildBeyondMaxLookBackInvalidIndexRowCount(long count) {
        before.setBeyondMaxLookBackInvalidIndexRowCount(before.getBeyondMaxLookBackInvalidIndexRowCount() + count);
    }
    public void addBeforeIndexHasMissingCellsCount(long count) {
        getBefore().setIndexHasMissingCellsCount(getBefore().getIndexHasMissingCellsCount() + count);
    }

    public void addBeforeIndexHasExtraCellsCount(long count) {
        getBefore().setIndexHasExtraCellsCount(getBefore().getIndexHasExtraCellsCount() + count);
    }

    public void addBeforeUnverifiedIndexRowCount(long count) {
        getBefore().setUnverifiedIndexRowCount(getBefore().getUnverifiedIndexRowCount() + count);
    }

    public void addBeforeOldIndexRowCount(long count) {
        getBefore().setOldIndexRowCount(getBefore().getOldIndexRowCount() + count);
    }

    public void addBeforeUnknownIndexRowCount(long count) {
        getBefore().setUnknownIndexRowCount(getBefore().getUnknownIndexRowCount() + count);
    }

    public void addBeforeRepairExtraVerifiedIndexRowCount(long count) {
        getBefore().setExtraVerifiedIndexRowCount(getBefore().getExtraVerifiedIndexRowCount() + count);
    }

    public void addBeforeRepairExtraUnverifiedIndexRowCount(long count) {
        getBefore().setExtraUnverifiedIndexRowCount(getBefore().getExtraUnverifiedIndexRowCount() + count);
    }

    private void addAfterRebuildValidIndexRowCount(long count) {
        getAfter().setValidIndexRowCount(getAfter().getValidIndexRowCount() + count);
    }

    private void addAfterRebuildExpiredIndexRowCount(long count) {
        getAfter().setExpiredIndexRowCount(getAfter().getExpiredIndexRowCount() + count);
    }

    private void addAfterRebuildMissingIndexRowCount(long count) {
        getAfter().setMissingIndexRowCount(getAfter().getMissingIndexRowCount() + count);
    }

    private void addAfterRebuildInvalidIndexRowCount(long count) {
        getAfter().setInvalidIndexRowCount(getAfter().getInvalidIndexRowCount() + count);
    }

    private void addAfterRebuildBeyondMaxLookBackMissingIndexRowCount(long count) {
        after.setBeyondMaxLookBackMissingIndexRowCount(after.getBeyondMaxLookBackMissingIndexRowCount() + count);
    }

    private void addAfterRebuildBeyondMaxLookBackInvalidIndexRowCount(long count) {
        after.setBeyondMaxLookBackInvalidIndexRowCount(after.getBeyondMaxLookBackInvalidIndexRowCount() + count);
    }

    public void addAfterIndexHasMissingCellsCount(long count) {
        getAfter().setIndexHasMissingCellsCount(getAfter().getIndexHasMissingCellsCount() + count);
    }

    public void addAfterIndexHasExtraCellsCount(long count) {
        getAfter().setIndexHasExtraCellsCount(getAfter().getIndexHasExtraCellsCount() + count);
    }

    public void addAfterRepairExtraVerifiedIndexRowCount(long count) {
        getAfter().setExtraVerifiedIndexRowCount(getAfter().getExtraVerifiedIndexRowCount() + count);
    }

    public void addAfterRepairExtraUnverifiedIndexRowCount(long count) {
        getAfter().setExtraUnverifiedIndexRowCount(getAfter().getExtraUnverifiedIndexRowCount() + count);
    }

    private static boolean isAfterRebuildInvalidIndexRowCount(Cell cell) {
        if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES, 0,
                AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES.length) == 0) {
            return true;
        }
        return false;
    }

    private long getValue(Cell cell) {
        return Long.parseLong(Bytes.toString(cell.getValueArray(),
                cell.getValueOffset(), cell.getValueLength()));
    }

    public void update(Cell cell) {
        if (CellUtil
                .matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, SCANNED_DATA_ROW_COUNT_BYTES)) {
            addScannedDataRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, REBUILT_INDEX_ROW_COUNT_BYTES)) {
            addRebuiltIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildValidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildExpiredIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildMissingIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildInvalidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildBeyondMaxLookBackMissingIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRebuildBeyondMaxLookBackInvalidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES)) {
            addBeforeIndexHasExtraCellsCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES)) {
            addBeforeIndexHasMissingCellsCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT_BYTES)) {
            addBeforeUnverifiedIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_OLD_INDEX_ROW_COUNT_BYTES)) {
            addBeforeOldIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT_BYTES)) {
            addBeforeUnknownIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRepairExtraVerifiedIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT_BYTES)) {
            addBeforeRepairExtraUnverifiedIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildValidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildExpiredIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildMissingIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildInvalidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildBeyondMaxLookBackMissingIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildBeyondMaxLookBackInvalidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES)) {
            addAfterIndexHasExtraCellsCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES)) {
            addAfterIndexHasMissingCellsCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES)) {
            addAfterRepairExtraVerifiedIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT_BYTES)) {
            addAfterRepairExtraUnverifiedIndexRowCount(getValue(cell));
        }
    }

    public boolean isVerificationFailed() {
        //we don't want to count max look back failures alone as failing an index rebuild job
        //so we omit them from the below calculation.
        if (getAfter().getInvalidIndexRowCount() + getAfter().getMissingIndexRowCount()
            + getAfter().getExtraVerifiedIndexRowCount() > 0) {
            return true;
        }
        return false;
    }

    public void add(IndexToolVerificationResult verificationResult) {
        setScannedDataRowCount(getScannedDataRowCount() + verificationResult.getScannedDataRowCount());
        setRebuiltIndexRowCount(getRebuiltIndexRowCount() + verificationResult.getRebuiltIndexRowCount());
        getBefore().add(verificationResult.getBefore());
        getAfter().add(verificationResult.getAfter());
    }
}
