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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;

public class UnionCompiler {
    private static final PName UNION_FAMILY_NAME = PNameFactory.newName("unionFamilyName");
    private static final PName UNION_SCHEMA_NAME = PNameFactory.newName("unionSchemaName");
    private static final PName UNION_TABLE_NAME = PNameFactory.newName("unionTableName");

    public static List<QueryPlan> checkProjectionNumAndTypes(List<QueryPlan> selectPlans) throws SQLException {
        QueryPlan plan = selectPlans.get(0);
        int columnCount = plan.getProjector().getColumnCount();
        List<? extends ColumnProjector> projectors = plan.getProjector().getColumnProjectors();
        List<PDataType> selectTypes = new ArrayList<PDataType>();
        for (ColumnProjector pro : projectors) {
            selectTypes.add(pro.getExpression().getDataType());
        }

        for (int i = 1;  i < selectPlans.size(); i++) {     
            plan = selectPlans.get(i);
            if (columnCount !=plan.getProjector().getColumnCount()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS).setMessage(".").build().buildException();
            }
            List<? extends ColumnProjector> pros =  plan.getProjector().getColumnProjectors();
            for (int j = 0; j < columnCount; j++) {
                PDataType type = pros.get(j).getExpression().getDataType();
                if (!type.isCoercibleTo(selectTypes.get(j))) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SELECT_COLUMN_TYPE_IN_UNIONALL_DIFFS).setMessage(".").build().buildException();
                }
            }
        }
        return selectPlans;
    }

    public static TableRef contructSchemaTable(PhoenixStatement statement, QueryPlan plan, List<AliasedNode> selectNodes) throws SQLException {
        List<PColumn> projectedColumns = new ArrayList<PColumn>();
        for (int i=0; i< plan.getProjector().getColumnCount(); i++) {
            ColumnProjector colProj = plan.getProjector().getColumnProjector(i);
            Expression sourceExpression = colProj.getExpression();
            String name = selectNodes == null ? colProj.getName() : selectNodes.get(i).getAlias();
            PColumnImpl projectedColumn = new PColumnImpl(PNameFactory.newName(name), UNION_FAMILY_NAME,
                    sourceExpression.getDataType(), sourceExpression.getMaxLength(), sourceExpression.getScale(), sourceExpression.isNullable(),
                    i, sourceExpression.getSortOrder(), 500, null, false, sourceExpression.toString());
            projectedColumns.add(projectedColumn);
        }
        Long scn = statement.getConnection().getSCN();
        PTable tempTable = PTableImpl.makePTable(statement.getConnection().getTenantId(), UNION_SCHEMA_NAME, UNION_TABLE_NAME, 
                PTableType.SUBQUERY, null, HConstants.LATEST_TIMESTAMP, scn == null ? HConstants.LATEST_TIMESTAMP : scn, null, null, projectedColumns, null, null, null,
                        true, null, null, null, true, true, true, null, null, null);
        TableRef tableRef = new TableRef(null, tempTable, 0, false);
        return tableRef;
    }
}
