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
package org.apache.phoenix.mapreduce.index;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType;

import java.util.Arrays;
import java.util.Objects;

public class IndexVerificationOutputRow {
    public static final String SCAN_MAX_TIMESTAMP = "ScanMaxTimestamp: ";
    private String dataTableName;
    private String indexTableName;
    private Long scanMaxTimestamp;
    private byte[] dataTableRowKey;
    private byte[] indexTableRowKey;
    private Long dataTableRowTimestamp;
    private Long indexTableRowTimestamp;
    private String errorMessage;
    private byte[] expectedValue;
    private byte[] actualValue;
    private byte[] phaseValue;
    private IndexVerificationErrorType errorType;

    private IndexVerificationOutputRow(String dataTableName, String indexTableName,
                                       byte[] dataTableRowKey, Long scanMaxTimestamp,
                                      byte[] indexTableRowKey,
                                       long dataTableRowTimestamp, long indexTableRowTimestamp,
                                      String errorMessage, byte[] expectedValue, byte[] actualValue,
                                      byte[] phaseValue, IndexVerificationErrorType errorType) {
        this.dataTableName = dataTableName;
        this.indexTableName = indexTableName;
        this.scanMaxTimestamp = scanMaxTimestamp;
        this.dataTableRowKey = dataTableRowKey;
        this.indexTableRowKey = indexTableRowKey;
        this.dataTableRowTimestamp = dataTableRowTimestamp;
        this.indexTableRowTimestamp = indexTableRowTimestamp;
        this.errorMessage = errorMessage;
        this.expectedValue = expectedValue;
        this.actualValue = actualValue;
        this.phaseValue = phaseValue;
        this.errorType = errorType;
    }

    public String getDataTableName() {
        return dataTableName;
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public Long getScanMaxTimestamp() {
        return scanMaxTimestamp;
    }

    public byte[] getIndexTableRowKey() {
        return indexTableRowKey;
    }

    public long getIndexTableRowTimestamp() {
        return indexTableRowTimestamp;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public byte[] getExpectedValue() {
        return expectedValue;
    }

    public byte[] getActualValue() {
        return actualValue;
    }

    public byte[] getPhaseValue() {
        return phaseValue;
    }

    public byte[] getDataTableRowKey() {
        return dataTableRowKey;
    }

    public Long getDataTableRowTimestamp() {
        return dataTableRowTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null ) {
            return false;
        }
        if (!(o instanceof  IndexVerificationOutputRow)) {
            return false;
        }
        IndexVerificationOutputRow otherRow = (IndexVerificationOutputRow) o;

        return Objects.equals(dataTableName, otherRow.getDataTableName()) &&
            Objects.equals(indexTableName, otherRow.getIndexTableName()) &&
            Objects.equals(scanMaxTimestamp, otherRow.getScanMaxTimestamp()) &&
            Arrays.equals(dataTableRowKey, otherRow.getDataTableRowKey()) &&
            Arrays.equals(indexTableRowKey, otherRow.getIndexTableRowKey()) &&
            Objects.equals(dataTableRowTimestamp, otherRow.getDataTableRowTimestamp()) &&
            Objects.equals(indexTableRowTimestamp, otherRow.getIndexTableRowTimestamp()) &&
            Objects.equals(errorMessage, otherRow.getErrorMessage()) &&
            Arrays.equals(expectedValue, otherRow.getExpectedValue()) &&
            Arrays.equals(actualValue, otherRow.getActualValue()) &&
            Arrays.equals(phaseValue, otherRow.getPhaseValue()) &&
            Objects.equals(errorType, otherRow.getErrorType());
    }

    @Override
    public int hashCode(){
        return Objects.hashCode(scanMaxTimestamp) ^ Objects.hashCode(indexTableName) ^
            Arrays.hashCode(dataTableRowKey);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(IndexVerificationOutputRepository.DATA_TABLE_NAME + ": ").append(dataTableName).append(",");
        sb.append(IndexVerificationOutputRepository.INDEX_TABLE_NAME + ": ").append(indexTableName).append(",");
        sb.append(SCAN_MAX_TIMESTAMP).append(": ").append(scanMaxTimestamp).append(",");
        sb.append(IndexVerificationOutputRepository.DATA_TABLE_ROW_KEY + ": ").append(Bytes.toString(dataTableRowKey)).append(",");
        sb.append(IndexVerificationOutputRepository.INDEX_TABLE_ROW_KEY + ": ").append(Bytes.toString(indexTableRowKey)).append(",");
        sb.append(IndexVerificationOutputRepository.DATA_TABLE_TS + ": ").append(dataTableRowTimestamp).append(",");
        sb.append(IndexVerificationOutputRepository.INDEX_TABLE_TS + ": ").append(indexTableRowTimestamp).append(",");
        sb.append(IndexVerificationOutputRepository.ERROR_MESSAGE + ": ").append(errorMessage).append(",");
        sb.append(IndexVerificationOutputRepository.EXPECTED_VALUE + ": ").append(Bytes.toString(expectedValue)).append(",");
        sb.append(IndexVerificationOutputRepository.ACTUAL_VALUE + ": ").append(Bytes.toString(actualValue)).append(
            ",");
        sb.append(IndexVerificationOutputRepository.VERIFICATION_PHASE + ": ").append(Bytes.toString(phaseValue));
        sb.append(IndexVerificationOutputRepository.ERROR_TYPE + ": " ).append(Objects.toString(errorType));
        return sb.toString();
    }

    public IndexVerificationErrorType getErrorType() {
        return errorType;
    }

    public static class IndexVerificationOutputRowBuilder {
        private String dataTableName;
        private String indexTableName;
        private Long scanMaxTimestamp;
        private byte[] dataTableRowKey;
        private byte[] indexTableRowKey;
        private long dataTableRowTimestamp;
        private long indexTableRowTimestamp;
        private String errorMessage;
        private byte[] expectedValue;
        private byte[] actualValue;
        private byte[] phaseValue;
        private IndexVerificationErrorType errorType;

        public IndexVerificationOutputRowBuilder setDataTableName(String dataTableName) {
            this.dataTableName = dataTableName;
            return this;
        }

        public IndexVerificationOutputRowBuilder setIndexTableName(String indexTableName) {
            this.indexTableName = indexTableName;
            return this;
        }

        public IndexVerificationOutputRowBuilder setScanMaxTimestamp(Long scanMaxTimestamp) {
            this.scanMaxTimestamp = scanMaxTimestamp;
            return this;
        }

        public IndexVerificationOutputRowBuilder setIndexTableRowKey(byte[] indexTableRowKey) {
            this.indexTableRowKey = indexTableRowKey;
            return this;
        }

        public IndexVerificationOutputRowBuilder setDataTableRowKey(byte[] dataTableRowKey){
            this.dataTableRowKey = dataTableRowKey;
            return this;
        }

        public IndexVerificationOutputRowBuilder setDataTableRowTimestamp(long dataTableRowTimestamp) {
            this.dataTableRowTimestamp = dataTableRowTimestamp;
            return this;
        }

        public IndexVerificationOutputRowBuilder setIndexTableRowTimestamp(long indexTableRowTimestamp) {
            this.indexTableRowTimestamp = indexTableRowTimestamp;
            return this;
        }

        public IndexVerificationOutputRowBuilder setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public IndexVerificationOutputRowBuilder setExpectedValue(byte[] expectedValue) {
            this.expectedValue = expectedValue;
            return this;
        }

        public IndexVerificationOutputRowBuilder setActualValue(byte[] actualValue) {
            this.actualValue = actualValue;
            return this;
        }

        public IndexVerificationOutputRowBuilder setPhaseValue(byte[] phaseValue) {
            this.phaseValue = phaseValue;
            return this;
        }

        public IndexVerificationOutputRowBuilder setErrorType(IndexVerificationErrorType errorType) {
            this.errorType = errorType;
            return this;
        }

        public IndexVerificationOutputRow build() {
            return new IndexVerificationOutputRow(dataTableName, indexTableName, dataTableRowKey,
                scanMaxTimestamp, indexTableRowKey, dataTableRowTimestamp, indexTableRowTimestamp,
                errorMessage, expectedValue, actualValue, phaseValue, errorType);
        }
    }
}
