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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.phoenix.util.SchemaUtil.getVarChars;

public class OrphanCleaner {

    public static void reapOrphans(HTableInterface hTable, byte[] tenantId, byte[] schema, byte[] name) throws IOException {
        List<byte[]> listOBytes = Lists.newArrayList();
        TableViewFinderResult viewFinderResult = new TableViewFinderResult();
        ViewFinder.findAllRelatives(hTable, tenantId, schema, name, PTable.LinkType.CHILD_TABLE, viewFinderResult);
        for (Result aResult : viewFinderResult.getResults()) {
            byte[][] rowViewKeyMetaData = new byte[5][];
            getVarChars(aResult.getRow(), 5, rowViewKeyMetaData);
            byte[] resultTenantId = rowViewKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
            byte[] resultSchema =
                SchemaUtil.getSchemaNameFromFullName(rowViewKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX])
                    .getBytes();
            byte[] resultTable =
                SchemaUtil.getTableNameFromFullName(rowViewKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX])
                    .getBytes();
            byte[] rowKeyInQuestion = SchemaUtil.getTableKey(resultTenantId, resultSchema, resultTable);
            listOBytes.add(rowKeyInQuestion);
        }

        for (int i = listOBytes.size() - 1; i >= 0; i--) {
            traverseUpAndDelete(hTable, listOBytes.get(i));
        }
    }

    static List<Delete> traverseUpAndDelete(HTableInterface hTable, byte[] row) throws IOException {
        byte[][] rowViewKeyMetaData = new byte[3][];
        getVarChars(row, 3, rowViewKeyMetaData);
        byte[] resultTenantId = rowViewKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
        byte[] resultSchema =
            SchemaUtil.getSchemaNameFromFullName(rowViewKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX])
                .getBytes();
        byte[] resultTable =
            SchemaUtil.getTableNameFromFullName(rowViewKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX])
                .getBytes();
        List<Delete> deletesToIssue = Lists.newArrayList();
        byte[] startKey = SchemaUtil.getTableKey(resultTenantId, resultSchema, resultTable);
        Scan scan = new Scan(startKey, ByteUtil.nextKey(startKey));
        scan.setFilter(new KeyOnlyFilter());
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            deletesToIssue.add(new Delete(result.getRow()));
        }
        return deletesToIssue;
    }

}
