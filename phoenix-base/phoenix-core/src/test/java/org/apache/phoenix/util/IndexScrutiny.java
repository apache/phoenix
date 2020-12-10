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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PDataType;

import org.apache.phoenix.thirdparty.com.google.common.base.Objects;

public class IndexScrutiny {

    public static long scrutinizeIndex(Connection conn, String fullTableName, String fullIndexName) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable ptable = pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName));
        int tableColumnOffset = 0;
        List<PColumn> tableColumns = ptable.getColumns();
        List<PColumn> tablePKColumns = ptable.getPKColumns();
        if (ptable.getBucketNum() != null) {
            tableColumnOffset = 1;
            tableColumns = tableColumns.subList(tableColumnOffset, tableColumns.size());
            tablePKColumns = tablePKColumns.subList(tableColumnOffset, tablePKColumns.size());
        }
        PTable pindex = pconn.getTable(new PTableKey(pconn.getTenantId(), fullIndexName));
        List<PColumn> indexColumns = pindex.getColumns();
        int indexColumnOffset = 0;
        if (pindex.getBucketNum() != null) {
            indexColumnOffset = 1;
        }
        if (pindex.getViewIndexId() != null) {
            indexColumnOffset++;
        }
        if (indexColumnOffset > 0) {
            indexColumns = indexColumns.subList(indexColumnOffset, indexColumns.size());
        }
        StringBuilder indexQueryBuf = new StringBuilder("SELECT ");
        for (PColumn dcol : tablePKColumns) {
            indexQueryBuf.append("CAST(\"" + IndexUtil.getIndexColumnName(dcol) + "\" AS " + dcol.getDataType().getSqlTypeName() + ")");
            indexQueryBuf.append(",");
        }
        for (PColumn icol :indexColumns) {
            PColumn dcol = IndexUtil.getDataColumn(ptable, icol.getName().getString());
            if (SchemaUtil.isPKColumn(icol) && !SchemaUtil.isPKColumn(dcol)) {
                indexQueryBuf.append("CAST (\"" + icol.getName().getString() + "\" AS " + dcol.getDataType().getSqlTypeName() + ")");
                indexQueryBuf.append(",");
            }
        }
        for (PColumn icol : indexColumns) {
            if (!SchemaUtil.isPKColumn(icol)) {
                PColumn dcol = IndexUtil.getDataColumn(ptable, icol.getName().getString());
                indexQueryBuf.append("CAST (\"" + icol.getName().getString() + "\" AS " + dcol.getDataType().getSqlTypeName() + ")");
                indexQueryBuf.append(",");
            }
        }
        indexQueryBuf.setLength(indexQueryBuf.length()-1);
        indexQueryBuf.append("\nFROM " + fullIndexName);
        
        StringBuilder tableQueryBuf = new StringBuilder("SELECT ");
        for (PColumn dcol : tablePKColumns) {
            tableQueryBuf.append("\"" + dcol.getName().getString() + "\"");
            tableQueryBuf.append(",");
        }
        for (PColumn icol : indexColumns) {
            PColumn dcol = IndexUtil.getDataColumn(ptable, icol.getName().getString());
            if (SchemaUtil.isPKColumn(icol) && !SchemaUtil.isPKColumn(dcol)) {
                if (dcol.getFamilyName() != null) {
                    tableQueryBuf.append("\"" + dcol.getFamilyName().getString() + "\"");
                    tableQueryBuf.append(".");
                }
                tableQueryBuf.append("\"" + dcol.getName().getString() + "\"");
                tableQueryBuf.append(",");
            }
        }
        for (PColumn icol : indexColumns) {
            if (!SchemaUtil.isPKColumn(icol)) {
                PColumn dcol = IndexUtil.getDataColumn(ptable, icol.getName().getString());
                if (dcol.getFamilyName() != null) {
                    tableQueryBuf.append("\"" + dcol.getFamilyName().getString() + "\"");
                    tableQueryBuf.append(".");
                }
                tableQueryBuf.append("\"" + dcol.getName().getString() + "\"");
                tableQueryBuf.append(",");
            }
        }
        tableQueryBuf.setLength(tableQueryBuf.length()-1);
        tableQueryBuf.append("\nFROM " + fullTableName + "\nWHERE (");
        for (PColumn dcol : tablePKColumns) {
            tableQueryBuf.append("\"" + dcol.getName().getString() + "\"");
            tableQueryBuf.append(",");
        }
        tableQueryBuf.setLength(tableQueryBuf.length()-1);
        tableQueryBuf.append(") = ((");
        for (int i = 0; i < tablePKColumns.size(); i++) {
            tableQueryBuf.append("?");
            tableQueryBuf.append(",");
        }
        tableQueryBuf.setLength(tableQueryBuf.length()-1);
        tableQueryBuf.append("))");
        
        String tableQuery = tableQueryBuf.toString();
        PreparedStatement istmt = conn.prepareStatement(tableQuery);
        
        String indexQuery = indexQueryBuf.toString();
        ResultSet irs = conn.createStatement().executeQuery(indexQuery);
        ResultSetMetaData irsmd = irs.getMetaData();
        long icount = 0;
        while (irs.next()) {
            icount++;
            StringBuilder pkBuf = new StringBuilder("(");
            for (int i = 0; i < tablePKColumns.size(); i++) {
                PColumn dcol = tablePKColumns.get(i);
                int offset = i+1;
                Object pkVal = irs.getObject(offset);
                PDataType pkType = PDataType.fromTypeId(irsmd.getColumnType(offset));
                istmt.setObject(offset, pkVal, dcol.getDataType().getSqlType());
                pkBuf.append(pkType.toStringLiteral(pkVal));
                pkBuf.append(",");
            }
            pkBuf.setLength(pkBuf.length()-1);
            pkBuf.append(")");
            ResultSet drs = istmt.executeQuery();
            ResultSetMetaData drsmd = drs.getMetaData();
            assertTrue("Expected to find PK in data table: " + pkBuf, drs.next());
            for (int i = 0; i < irsmd.getColumnCount(); i++) {
                Object iVal = irs.getObject(i + 1);
                PDataType iType = PDataType.fromTypeId(irsmd.getColumnType(i + 1));
                Object dVal = drs.getObject(i + 1);
                PDataType dType = PDataType.fromTypeId(drsmd.getColumnType(i + 1));
                assertTrue("Expected equality for " + drsmd.getColumnName(i + 1) + ", but " + iType.toStringLiteral(iVal) + "!=" + dType.toStringLiteral(dVal), Objects.equal(iVal, dVal));
            }
        }
        
        long dcount = TestUtil.getRowCount(conn, fullTableName);
        assertEquals("Expected data table row count to match", dcount, icount);
        return dcount;
    }

}
