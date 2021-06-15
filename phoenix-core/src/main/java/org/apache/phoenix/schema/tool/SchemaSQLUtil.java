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
package org.apache.phoenix.schema.tool;

import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;

import java.util.List;
import java.util.Map;

public class SchemaSQLUtil {

    protected static String getCreateTableSQL(CreateTableStatement createStmt) {
        if (createStmt == null) {
            return "";
        }
        StringBuffer sb = new StringBuffer()
                .append("CREATE "+createStmt.getTableType() + " ");
        if (createStmt.ifNotExists()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(createStmt.getTableName()).append("\n")
                .append(getColumnDefListToString(createStmt))
                .append("\nCONSTRAINT "+createStmt.getPrimaryKeyConstraint().getName()+" PRIMARY KEY")
                .append(" ("+createStmt.getPrimaryKeyConstraint().toString()+"))"
                        .replaceAll(",", ",\n"));
        if (createStmt.getTableType().equals(PTableType.VIEW)) {
            sb.append("\nAS SELECT * FROM " + createStmt.getBaseTableName());
            if (createStmt.getWhereClause()!=null) {
                sb.append(" WHERE " +createStmt.getWhereClause());
            }
        }
        appendProperties(sb, createStmt.getProps());
        return sb.toString();
    }

    protected static String getCreateIndexSQL(CreateIndexStatement createStmt) {
        if (createStmt == null) {
            return "";
        }
        StringBuffer sb = new StringBuffer()
                .append("CREATE"
                        + (createStmt.getIndexType().equals(PTable.IndexType.LOCAL) ? " "+createStmt.getIndexType() : "")
                        + " INDEX ");
        if (createStmt.ifNotExists()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(createStmt.getIndexTableName().getTableName()).append("\n")
                .append("ON "+createStmt.getTable().getName())
                .append("("+createStmt.getIndexConstraint().toString()).append(")");
        if (createStmt.getIncludeColumns()!=null && !createStmt.getIncludeColumns().isEmpty()) {
            sb.append("\nINCLUDE ");
            sb.append(getColumnListToString(createStmt.getIncludeColumns()));
        }
        if (createStmt.isAsync()) {
            sb.append(" ASYNC");
        }
        appendProperties(sb, createStmt.getProps());
        return sb.toString();
    }

    private static String getColumnListToString(List<ColumnName> columnNames) {
        StringBuffer sb = new StringBuffer();
        for(ColumnName cName : columnNames) {
            if (sb.length()==0) {
                sb.append("(");
            }
            sb.append(cName.toString()).append(",\n");
        }
        if (sb.length()!=0) {
            sb.deleteCharAt(sb.length()-1).deleteCharAt(sb.length()-1);
            sb.append(")");
        }
        return sb.toString();
    }

    private static String getColumnDefListToString(CreateTableStatement createStatement) {
        List<ColumnDef> colDef = createStatement.getColumnDefs();
        StringBuffer sb = new StringBuffer();
        for(ColumnDef cDef : colDef) {
            String columnString = getColumnInfoString(cDef);
            if (sb.length()==0) {
                sb.append("(");
            } else {
                sb.append(",\n");
            }
            sb.append(columnString);
        }
        return sb.toString();
    }

    private static String getColumnInfoString(ColumnDef cDef) {
        String colName = cDef.getColumnDefName().toString();
        boolean isArrayType = cDef.getDataType().isArrayType();
        String type = cDef.getDataType().getSqlTypeName();
        Integer maxLength = cDef.getMaxLength();
        Integer arrSize = cDef.getArraySize();
        Integer scale = cDef.getScale();
        StringBuilder buf = new StringBuilder(colName);
        buf.append(' ');
        if (isArrayType) {
            String arrayPrefix = type.split("\\s+")[0];
            buf.append(arrayPrefix);
            appendMaxLengthAndScale(buf, maxLength, scale);
            buf.append(' ');
            buf.append("ARRAY");
            if (arrSize != null) {
                buf.append('[');
                buf.append(arrSize);
                buf.append(']');
            }
        } else {
            buf.append(type);
            appendMaxLengthAndScale(buf, maxLength, scale);
        }

        if (!cDef.isNull()) {
            buf.append(' ');
            buf.append("NOT NULL");
        }
        if(cDef.getExpression()!=null) {
            buf.append(" DEFAULT ");
            buf.append(cDef.getExpression());
        }

        return buf.toString();
    }

    private static void appendMaxLengthAndScale(StringBuilder buf, Integer maxLength, Integer scale){
        if (maxLength != null) {
            buf.append('(');
            buf.append(maxLength);
            if (scale != null) {
                buf.append(',');
                buf.append(scale); // has both max length and scale. For ex- decimal(10,2)
            }
            buf.append(')');
        }
    }

    private static void appendProperties(StringBuffer sb,
            ListMultimap<String, Pair<String, Object>> props) {
        if (props != null && !props.isEmpty()) {
            sb.append("\n");
            for (Map.Entry<String, Pair<String, Object>> entry : props.entries()) {
                sb.append(entry.getValue().getFirst()).append("=")
                        .append(entry.getValue().getSecond());
                sb.append(",");
            }
            sb.deleteCharAt(sb.length()-1);
        }
    }
}
