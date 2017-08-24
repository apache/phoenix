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

import java.util.List;

import org.apache.phoenix.mapreduce.util.IndexColumnNames;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;

/**
 * Get index scrutiny source/target column names, depending on whether the source is the
 * data table or index table
 */
public interface SourceTargetColumnNames {

    List<String> getSourceColNames();

    List<String> getUnqualifiedSourceColNames();

    List<String> getTargetColNames();

    /**
     * @return The target column name with a CAST to the source's data type
     */
    List<String> getCastedTargetColNames();

    List<String> getUnqualifiedTargetColNames();

    List<String> getSourceDynamicCols();

    List<String> getTargetDynamicCols();

    List<String> getTargetPkColNames();

    List<String> getSourcePkColNames();

    String getQualifiedSourceTableName();

    String getQualifiedTargetTableName();

    /**
     * Used when the data table is the source table of a scrutiny
     */
    public static class DataSourceColNames extends IndexColumnNames
            implements SourceTargetColumnNames {
        /**
         * @param pdataTable the data table
         * @param pindexTable the index table for the data table
         */
        public DataSourceColNames(PTable pdataTable, PTable pindexTable) {
            super(pdataTable, pindexTable);
        }

        @Override
        public List<String> getSourceColNames() {
            return getDataColNames();
        }

        @Override
        public List<String> getUnqualifiedSourceColNames() {
            return getUnqualifiedDataColNames();
        }

        @Override
        public List<String> getUnqualifiedTargetColNames() {
            return getUnqualifiedIndexColNames();
        }

        @Override
        public List<String> getTargetColNames() {
            return getIndexColNames();
        }

        @Override
        public List<String> getSourceDynamicCols() {
            return getDynamicDataCols();
        }

        @Override
        public List<String> getTargetDynamicCols() {
            return getDynamicIndexCols();
        }

        @Override
        public List<String> getTargetPkColNames() {
            return getIndexPkColNames();
        }

        @Override
        public List<String> getSourcePkColNames() {
            return getDataPkColNames();
        }

        @Override
        public String getQualifiedSourceTableName() {
            return getQualifiedDataTableName();
        }

        @Override
        public String getQualifiedTargetTableName() {
            return getQualifiedIndexTableName();
        }

        @Override
        public List<String> getCastedTargetColNames() {
            return getCastedColumnNames(getIndexColNames(), dataColSqlTypeNames);
        }

    }

    /**
     * Used when the index table is the source table of a scrutiny
     */
    public static class IndexSourceColNames extends IndexColumnNames
            implements SourceTargetColumnNames {
        /**
         * @param pdataTable the data table
         * @param pindexTable the index table for the data table
         */
        public IndexSourceColNames(PTable pdataTable, PTable pindexTable) {
            super(pdataTable, pindexTable);
        }

        @Override
        public List<String> getSourceColNames() {
            return getIndexColNames();
        }

        @Override
        public List<String> getUnqualifiedSourceColNames() {
            return getUnqualifiedIndexColNames();
        }

        @Override
        public List<String> getUnqualifiedTargetColNames() {
            return getUnqualifiedDataColNames();
        }

        @Override
        public List<String> getTargetColNames() {
            return getDataColNames();
        }

        @Override
        public List<String> getSourceDynamicCols() {
            return getDynamicIndexCols();
        }

        @Override
        public List<String> getTargetDynamicCols() {
            return getDynamicDataCols();
        }

        @Override
        public List<String> getTargetPkColNames() {
            return getDataPkColNames();
        }

        @Override
        public List<String> getSourcePkColNames() {
            return getIndexPkColNames();
        }

        @Override
        public String getQualifiedSourceTableName() {
            return getQualifiedIndexTableName();
        }

        @Override
        public String getQualifiedTargetTableName() {
            return getQualifiedDataTableName();
        }

        @Override
        public List<String> getCastedTargetColNames() {
            return getCastedColumnNames(getDataColNames(), indexColSqlTypeNames);
        }
    }
}
