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

import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_OLD_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.REBUILT_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.RESULT_TABLE_COLUMN_FAMILY;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.SCANNED_DATA_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES;

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

    public IndexToolVerificationResult(byte[] startRow, byte[] stopRow, long scanMaxTs){
        this.setStartRow(startRow);
        this.setStopRow(stopRow);
        this.scanMaxTs = scanMaxTs;
    }

    public IndexToolVerificationResult(Scan scan){
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
        private long indexHasExtraCellsCount = 0;
        private long indexHasMissingCellsCount = 0;
        private long unverifiedIndexRowCount = 0;
        private long oldIndexRowCount = 0;
        private long unknownIndexRowCount = 0;

        public void add(PhaseResult phaseResult) {
            setValidIndexRowCount(getValidIndexRowCount() + phaseResult.getValidIndexRowCount());
            setExpiredIndexRowCount(getExpiredIndexRowCount() + phaseResult.getExpiredIndexRowCount());
            setMissingIndexRowCount(getMissingIndexRowCount() + phaseResult.getMissingIndexRowCount());
            setInvalidIndexRowCount(getInvalidIndexRowCount() + phaseResult.getInvalidIndexRowCount());
            setIndexHasExtraCellsCount(getIndexHasExtraCellsCount() + phaseResult.getIndexHasExtraCellsCount());
            setIndexHasMissingCellsCount(getIndexHasMissingCellsCount() + phaseResult.getIndexHasMissingCellsCount());
            setUnverifiedIndexRowCount(getUnverifiedIndexRowCount() + phaseResult.getUnverifiedIndexRowCount());
            setUnknownIndexRowCount(getUnknownIndexRowCount() + phaseResult.getUnknownIndexRowCount());
            setOldIndexRowCount(getOldIndexRowCount() + phaseResult.getOldIndexRowCount());
        }

        public PhaseResult(){}

        public PhaseResult(long validIndexRowCount, long expiredIndexRowCount,
                long missingIndexRowCount, long invalidIndexRowCount, long indexHasExtraCellsCount, long indexHasMissingCellsCount) {
            this.setValidIndexRowCount(validIndexRowCount);
            this.setExpiredIndexRowCount(expiredIndexRowCount);
            this.setMissingIndexRowCount(missingIndexRowCount);
            this.setInvalidIndexRowCount(invalidIndexRowCount);
            this.setIndexHasExtraCellsCount(indexHasExtraCellsCount);
            this.setIndexHasMissingCellsCount(indexHasMissingCellsCount);
        }

        public long getTotalCount() {
            return getValidIndexRowCount() + getExpiredIndexRowCount() + getMissingIndexRowCount() + getInvalidIndexRowCount();
        }

        public long getIndexHasExtraCellsCount() {
            return indexHasExtraCellsCount;
        }

        @Override
        public String toString() {
            return "PhaseResult{" +
                    "validIndexRowCount=" + getValidIndexRowCount() +
                    ", expiredIndexRowCount=" + getExpiredIndexRowCount() +
                    ", missingIndexRowCount=" + getMissingIndexRowCount() +
                    ", invalidIndexRowCount=" + getInvalidIndexRowCount() +
                    ", extraCellsOnIndexCount=" + getIndexHasExtraCellsCount() +
                    ", missingCellsOnIndexCount=" + getIndexHasMissingCellsCount() +
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
            return this.getExpiredIndexRowCount() == pr.getExpiredIndexRowCount()
                    && this.getValidIndexRowCount() == pr.getValidIndexRowCount()
                    && this.getInvalidIndexRowCount() == pr.getInvalidIndexRowCount()
                    && this.getMissingIndexRowCount() == pr.getMissingIndexRowCount()
                    && this.getIndexHasMissingCellsCount() == pr.getIndexHasMissingCellsCount()
                    && this.getIndexHasExtraCellsCount() == pr.getIndexHasExtraCellsCount();
        }

        @Override
        public int hashCode() {
            long result = 17;
            result = 31 * result + getExpiredIndexRowCount();
            result = 31 * result + getValidIndexRowCount();
            result = 31 * result + getMissingIndexRowCount();
            result = 31 * result + getInvalidIndexRowCount();
            result = 31 * result + getIndexHasMissingCellsCount();
            result = 31 * result + getIndexHasExtraCellsCount();
            result = 31 * result + getUnverifiedIndexRowCount();
            result = 31 * result + getOldIndexRowCount();
            result = 31 * result + getUnknownIndexRowCount();
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

        public void setIndexHasExtraCellsCount(long indexHasExtraCellsCount) {
            this.indexHasExtraCellsCount = indexHasExtraCellsCount;
        }

        public long getIndexHasMissingCellsCount() {
            return indexHasMissingCellsCount;
        }

        public void setIndexHasMissingCellsCount(long indexHasMissingCellsCount) {
            this.indexHasMissingCellsCount = indexHasMissingCellsCount;
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


    public long getBeforeRebuildMissingIndexRowCount() {
        return getBefore().getMissingIndexRowCount();
    }

    public long getBeforeIndexHasMissingCellsCount() {return before.indexHasMissingCellsCount; }

    public long getBeforeIndexHasExtraCellsCount() {return before.indexHasExtraCellsCount; }

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

    public long getAfterIndexHasMissingCellsCount() { return after.indexHasMissingCellsCount; }

    public long getAfterIndexHasExtraCellsCount() { return after.indexHasExtraCellsCount; }

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

    public void addBeforeIndexHasMissingCellsCount(long count) {
        before.indexHasMissingCellsCount += count;
    }

    public void addBeforeIndexHasExtraCellsCount(long count) {
        before.indexHasExtraCellsCount += count;
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

    public void addAfterIndexHasMissingCellsCount(long count) {
        after.indexHasMissingCellsCount += count;
    }

    public void addAfterIndexHasExtraCellsCount(long count) {
        after.indexHasExtraCellsCount += count;
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
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildValidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildExpiredIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildMissingIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES)) {
            addAfterRebuildInvalidIndexRowCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES)) {
            addAfterIndexHasExtraCellsCount(getValue(cell));
        } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES)) {
            addAfterIndexHasMissingCellsCount(getValue(cell));
        }
    }

    public boolean isVerificationFailed() {
        if (getAfter().getInvalidIndexRowCount() + getAfter().getMissingIndexRowCount() > 0) {
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
