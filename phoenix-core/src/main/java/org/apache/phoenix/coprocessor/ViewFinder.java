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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PARENT_TENANT_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

public class ViewFinder {

	// The PHYSICAL_TABLE link from view to the base table overwrites the PARENT_TABLE link (when namespace mapping is disabled)
    static TableViewFinderResult findBaseTable(Table systemCatalog, byte[] tenantId, byte[] schema, byte[] table)
        throws IOException {
        return findRelatedViews(systemCatalog, tenantId, schema, table, PTable.LinkType.PHYSICAL_TABLE,
            HConstants.LATEST_TIMESTAMP);
    }
    
    static TableViewFinderResult findParentViewofIndex(Table systemCatalog, byte[] tenantId, byte[] schema, byte[] table)
            throws IOException {
            return findRelatedViews(systemCatalog, tenantId, schema, table, PTable.LinkType.VIEW_INDEX_PARENT_TABLE,
                HConstants.LATEST_TIMESTAMP);
        }

    public static void findAllRelatives(Table systemTable, byte[] tenantId, byte[] schema, byte[] table,
        PTable.LinkType linkType, TableViewFinderResult result) throws IOException {
        findAllRelatives(systemTable, tenantId, schema, table, linkType, HConstants.LATEST_TIMESTAMP, result);
    }

    static void findAllRelatives(Table systemCatalog, byte[] tenantId, byte[] schema, byte[] table,
        PTable.LinkType linkType, long timestamp, TableViewFinderResult result) throws IOException {
        TableViewFinderResult currentResult =
            findRelatedViews(systemCatalog, tenantId, schema, table, linkType, timestamp);
        result.addResult(currentResult);
        for (TableInfo viewInfo : currentResult.getLinks()) {
            findAllRelatives(systemCatalog, viewInfo.getTenantId(), viewInfo.getSchemaName(), viewInfo.getTableName(), linkType, timestamp, result);
        }
    }

    /**
     * Runs a scan on SYSTEM.CATALOG or SYSTEM.CHILD_LINK to get the related tables/views
     */
    static TableViewFinderResult findRelatedViews(Table systemCatalog, byte[] tenantId, byte[] schema, byte[] table,
        PTable.LinkType linkType, long timestamp) throws IOException {
        if (linkType==PTable.LinkType.INDEX_TABLE || linkType==PTable.LinkType.EXCLUDED_COLUMN) {
            throw new IllegalArgumentException("findAllRelatives does not support link type "+linkType);
        }
        byte[] key = SchemaUtil.getTableKey(tenantId, schema, table);
		Scan scan = MetaDataUtil.newTableRowsScan(key, MetaDataProtocol.MIN_TABLE_TIMESTAMP, timestamp);
        SingleColumnValueFilter linkFilter =
            new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES, CompareFilter.CompareOp.EQUAL,
                linkType.getSerializedValueAsByteArray());
        linkFilter.setFilterIfMissing(true);
        scan.setFilter(linkFilter);
        scan.addColumn(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES);
        if (linkType==PTable.LinkType.PARENT_TABLE)
            scan.addColumn(TABLE_FAMILY_BYTES, PARENT_TENANT_ID_BYTES);
        if (linkType==PTable.LinkType.PHYSICAL_TABLE)
            scan.addColumn(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
        List<TableInfo> tableInfoList = Lists.newArrayList();
        try (ResultScanner scanner = systemCatalog.getScanner(scan))  {
            for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                byte[][] rowKeyMetaData = new byte[5][];
                byte[] viewTenantId = null;
                getVarChars(result.getRow(), 5, rowKeyMetaData);
                if (linkType==PTable.LinkType.PARENT_TABLE) {
                    viewTenantId = result.getValue(TABLE_FAMILY_BYTES, PARENT_TENANT_ID_BYTES);
                } else if (linkType==PTable.LinkType.CHILD_TABLE) {
                    viewTenantId = rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
                } else if (linkType==PTable.LinkType.VIEW_INDEX_PARENT_TABLE) {
                    viewTenantId = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
                } 
                else if (linkType==PTable.LinkType.PHYSICAL_TABLE && result.getValue(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES)!=null) {
                    // do not links from indexes to their physical table
                    continue;
                }
                byte[] viewSchemaName = SchemaUtil.getSchemaNameFromFullName(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
                byte[] viewName = SchemaUtil.getTableNameFromFullName(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
                tableInfoList.add(new TableInfo(viewTenantId, viewSchemaName, viewName));
            }
            return new TableViewFinderResult(tableInfoList);
        } 
    }
    
    /**
     * @return true if the given table has at least one child view
     * @throws IOException 
     */
    public static boolean hasChildViews(Table systemCatalog, byte[] tenantId, byte[] schemaName, byte[] tableName, long timestamp) throws IOException {
        byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
        Scan scan = MetaDataUtil.newTableRowsScan(key, MetaDataProtocol.MIN_TABLE_TIMESTAMP, timestamp);
        SingleColumnValueFilter linkFilter =
                new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES,
                        CompareFilter.CompareOp.EQUAL,
                        LinkType.CHILD_TABLE.getSerializedValueAsByteArray()) {
                    // if we found a row with the CHILD_TABLE link type we are done and can
                    // terminate the scan
                    @Override
                    public boolean filterAllRemaining() throws IOException {
                        return matchedColumn;
                    }
                };
        linkFilter.setFilterIfMissing(true);
        scan.setFilter(linkFilter);
        scan.addColumn(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES);
        try (ResultScanner scanner = systemCatalog.getScanner(scan)) {
            Result result = scanner.next();
            return result!=null; 
        }
    }

}
