/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PARENT_TENANT_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

public class ViewFinder {

    public List<Result> findParentViews(HTable systemCatalog, byte[] tenantId, byte[] schema, byte[] table) throws IOException {
        List<Result> results =
            new CopyOnWriteArrayList<>(findRelatedViews(systemCatalog, tenantId, schema, table, PTable.LinkType.PARENT_TABLE));
        for (Result viewResult : results) {
            byte[][] rowViewKeyMetaData = new byte[5][];
            getVarChars(viewResult.getRow(), 5, rowViewKeyMetaData);
            byte[] viewtenantId = rowViewKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
            byte[] viewSchema = SchemaUtil.getSchemaNameFromFullName(rowViewKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
            byte[] viewTable = SchemaUtil.getTableNameFromFullName(rowViewKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
            results.addAll(findParentViews(systemCatalog, viewtenantId, viewSchema, viewTable));
        }
        return results;

    }

    public List<Result> findChildViews(HTable systemCatalog, byte[] tenantId, byte[] schema, byte[] table) throws IOException {
        List<Result> results =
            new CopyOnWriteArrayList<>(findRelatedViews(systemCatalog, tenantId, schema, table, PTable.LinkType.CHILD_TABLE));
        for (Result viewResult : results) {
            byte[][] rowViewKeyMetaData = new byte[5][];
            getVarChars(viewResult.getRow(), 5, rowViewKeyMetaData);
            byte[] viewtenantId = rowViewKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
            byte[] viewSchema = SchemaUtil.getSchemaNameFromFullName(rowViewKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
            byte[] viewTable = SchemaUtil.getTableNameFromFullName(rowViewKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
            results.addAll(findChildViews(systemCatalog, viewtenantId, viewSchema, viewTable));
        }
        return results;
    }

    private List<Result> findRelatedViews(HTable systemCatalog, byte[] tenantId, byte[] schema, byte[] table, PTable.LinkType linkType) throws IOException {
        Scan scan = new Scan();
        byte[] startRow = SchemaUtil.getTableKey(tenantId, schema, table);
        byte[] stopRow = ByteUtil.nextKey(startRow);
        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);
        SingleColumnValueFilter linkFilter =
            new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES, CompareFilter.CompareOp.EQUAL, linkType.getSerializedValueAsByteArray());
        linkFilter.setFilterIfMissing(true);
        scan.setFilter(linkFilter);
        scan.addColumn(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES);
        scan.addColumn(TABLE_FAMILY_BYTES, PARENT_TENANT_ID_BYTES);
            boolean allViewsInCurrentRegion = true;
            int numOfChildViews = 0;
            List<Result> results = Lists.newArrayList();
            ResultScanner scanner = systemCatalog.getScanner(scan);
            try {
                for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                    numOfChildViews++;
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                    ResultTuple resultTuple = new ResultTuple(result);
                    resultTuple.getKey(ptr);
                    byte[] key = ptr.copyBytes();
                    //                    if (checkTableKeyInRegion(key, region) != null) {
                    //                        allViewsInCurrentRegion = false;
                    //                    }
                    results.add(result);
                }
                //                if (numOfChildViews > 0 && !allViewsInCurrentRegion) {
                //                    throw new IOException("Table views not in current region");
                //                }
                return results;
            } finally {
                scanner.close();
            }
    }

}
