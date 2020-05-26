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

import java.util.Arrays;
import java.util.Objects;

public class IndexVerificationOutputRow {
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

    private IndexVerificationOutputRow(String dataTableName, String indexTableName,
                                       byte[] dataTableRowKey, Long scanMaxTimestamp,
                                      byte[] indexTableRowKey,
                                       long dataTableRowTimestamp, long indexTableRowTimestamp,
                                      String errorMessage, byte[] expectedValue, byte[] actualValue,
                                      byte[] phaseValue) {
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
            Arrays.equals(phaseValue, otherRow.getPhaseValue());
    }

    @Override
    public int hashCode(){
        return Objects.hashCode(scanMaxTimestamp) ^ Objects.hashCode(indexTableName) ^
            Arrays.hashCode(dataTableRowKey);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DataTableName: ").append(dataTableName).append(",");
        sb.append("IndexTableName: ").append(indexTableName).append(",");
        sb.append("ScanMaxTimestamp: ").append(scanMaxTimestamp).append(",");
        sb.append("DataTableRowKey: ").append(Bytes.toString(dataTableRowKey)).append(",");
        sb.append("IndexTableRowKey: ").append(Bytes.toString(indexTableRowKey)).append(",");
        sb.append("DataTableRowTimestamp: ").append(dataTableRowTimestamp).append(",");
        sb.append("IndexTableRowTimestamp: ").append(indexTableRowTimestamp).append(",");
        sb.append("ErrorMessage: ").append(errorMessage).append(",");
        sb.append("ExpectedValue: ").append(Bytes.toString(expectedValue)).append(",");
        sb.append("ActualValue: ").append(Bytes.toString(actualValue)).append(",");
        sb.append("PhaseValue: ").append(Bytes.toString(phaseValue));
        return sb.toString();
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

        public IndexVerificationOutputRow build() {
            return new IndexVerificationOutputRow(dataTableName, indexTableName, dataTableRowKey,
                scanMaxTimestamp, indexTableRowKey, dataTableRowTimestamp, indexTableRowTimestamp,
                errorMessage, expectedValue, actualValue, phaseValue);
        }
    }
}
