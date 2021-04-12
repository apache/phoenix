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

import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public interface ColumnMutator {

    enum MutateColumnType {
        ADD_COLUMN, DROP_COLUMN
    }

    /**
     * Validates the column to be added or dropped against all child views
     */
    MetaDataProtocol.MetaDataMutationResult validateWithChildViews(PTable table, List<PTable> childViews,
                                                                   List<Mutation> tableMetadata,
                                                                   byte[] schemaName, byte[] tableName)
            throws IOException, SQLException;

    /**
     * Validates that the column being added or dropped against the table or view itself
     * Adds to  the list of mutations required to add or drop columns
     */
    MetaDataProtocol.MetaDataMutationResult validateAndAddMetadata(PTable table, byte[][] rowKeyMetaData,
                                                                   List<Mutation> tableMetadata, Region region,
                                                                   List<ImmutableBytesPtr> invalidateList,
                                                                   List<Region.RowLock> locks,
                                                                   long clientTimeStamp,
                                                                   long clientVersion,
                                                                   ExtendedCellBuilder extendedCellBuilder,
                                                                   boolean isAddingOrDroppingColumns)
            throws IOException, SQLException;

    /**
     * @return list of pair of table and column being dropped, used to drop any indexes that require the column
     */
    List<Pair<PTable, PColumn>> getTableAndDroppedColumnPairs();

    MutateColumnType getMutateColumnType();
}
