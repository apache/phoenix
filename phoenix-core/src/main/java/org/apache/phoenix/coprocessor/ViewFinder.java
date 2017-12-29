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
import static org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE_ARRAY;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

class ViewFinder {

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

    static void findAllRelatives(Table systemTable, byte[] tenantId, byte[] schema, byte[] table,
        PTable.LinkType linkType, TableViewFinderResult result) throws IOException {
        findAllRelatives(systemTable, tenantId, schema, table, linkType, HConstants.LATEST_TIMESTAMP, result);
    }

    static void findAllRelatives(Table systemCatalog, byte[] tenantId, byte[] schema, byte[] table,
        PTable.LinkType linkType, long timestamp, TableViewFinderResult result) throws IOException {
        TableViewFinderResult currentResult =
            findRelatedViews(systemCatalog, tenantId, schema, table, linkType, timestamp);
        result.addResult(currentResult);
        for (TableInfo viewInfo : currentResult.getResults()) {
            findAllRelatives(systemCatalog, viewInfo.getTenantId(), viewInfo.getSchemaName(), viewInfo.getTableName(), linkType, timestamp, result);
        }
    }

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
        ResultScanner scanner = systemCatalog.getScanner(scan);
        List<TableInfo> tableInfoList = Lists.newArrayList();
        try {
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
                viewTenantId = viewTenantId==null ? new byte[]{} : viewTenantId;
                byte[] viewSchemaName = SchemaUtil.getSchemaNameFromFullName(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
                byte[] viewName = SchemaUtil.getTableNameFromFullName(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]).getBytes();
                tableInfoList.add(new TableInfo(result.getRow(), viewTenantId, viewSchemaName, viewName));
            }
            return new TableViewFinderResult(tableInfoList);
        } finally {
            scanner.close();
        }
    }

    static Graph<TableInfo> findOrphans(Table systemCatalog, long timestamp) throws IOException {
        Graph<TableInfo> graph = new Graph<>();
        Scan scan = new Scan();
        scan.setTimeRange(0, timestamp);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        SingleColumnValueFilter childFilter =
            new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES, CompareFilter.CompareOp.EQUAL,
                PTable.LinkType.CHILD_TABLE.getSerializedValueAsByteArray());
        childFilter.setFilterIfMissing(true);
        SingleColumnValueFilter parentFilter =
            new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES, CompareFilter.CompareOp.EQUAL,
                PTable.LinkType.PARENT_TABLE.getSerializedValueAsByteArray());
        parentFilter.setFilterIfMissing(true);
        SingleColumnValueFilter physicalTableFilter =
            new SingleColumnValueFilter(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES, CompareFilter.CompareOp.EQUAL,
                PTable.LinkType.PHYSICAL_TABLE.getSerializedValueAsByteArray());
        physicalTableFilter.setFilterIfMissing(true);
        list.addFilter(childFilter);
        list.addFilter(parentFilter);
        list.addFilter(physicalTableFilter);
        scan.setFilter(list);
        scan.addColumn(TABLE_FAMILY_BYTES, LINK_TYPE_BYTES);
        scan.addColumn(TABLE_FAMILY_BYTES, PARENT_TENANT_ID_BYTES);
        ResultScanner scanner = systemCatalog.getScanner(scan);
        try {
            for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                ResultTuple resultTuple = new ResultTuple(result);
                resultTuple.getKey(ptr);
                byte[][] rowKeyMetaData = new byte[5][];
                getVarChars(result.getRow(), 5, rowKeyMetaData);
                byte[] tenantId = rowKeyMetaData[0];
                byte[] schema = rowKeyMetaData[1];
                byte[] tableName = rowKeyMetaData[2];
                byte[] link = rowKeyMetaData[4];
                graph.addEdge(new TableInfo(tenantId, schema, tableName), new TableInfo(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, link));
            }
            return graph;
        } finally {
            scanner.close();
        }
    }


    @Deprecated
    public static class Graph<T> {
        private LinkedHashMultimap<T, T> map;

        public Graph() {
            map = LinkedHashMultimap.create();
        }

        public void addEdge(T leftNode, T rightNode) {
            Set<T> adjacent = map.get(leftNode);
            if (adjacent == null) {
                adjacent = new LinkedHashSet<T>();
                map.putAll(leftNode, adjacent);
            }
            adjacent.add(rightNode);
        }

        public void addTwoWayVertex(T leftNode, T rightNode) {
            addEdge(leftNode, rightNode);
            addEdge(rightNode, leftNode);
        }

        public boolean isConnected(T leftNode, T rightNode) {
            Set<T> adjacent = map.get(leftNode);
            return adjacent != null && adjacent.contains(rightNode);
        }

        public Set<T> adjacentNodes(T last) {
            return map.get(last);
        }

        public Set<T> findAllOrphans() {
            Set<T> results = Sets.newHashSet();
            for (T t : map.keySet() ) {
                for (T value : map.get(t)) {
                    if (!map.containsKey(value)) {
                        results.add(t);
                    }
                }
            }
            return results;
        }

        @Override
        public String toString() {
            return "Graph{" +
                "map=" + map +
                '}';
        }
    }

}
