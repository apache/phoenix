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
package org.apache.phoenix.schema;

import java.sql.SQLException;
import java.util.Set;

import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.util.IndexUtil;

public class LocalIndexDataColumnRef extends ColumnRef {
    final private int position;
    final private Set<PColumn> columns;
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();

    public LocalIndexDataColumnRef(StatementContext context, String indexColumnName) throws MetaDataEntityNotFoundException, SQLException {
        super(FromCompiler.getResolver(
            FACTORY.namedTable(null, TableName.create(context.getCurrentTable().getTable()
                    .getSchemaName().getString(), context.getCurrentTable().getTable()
                    .getParentTableName().getString())), context.getConnection()).resolveTable(
            context.getCurrentTable().getTable().getSchemaName().getString(),
            context.getCurrentTable().getTable().getParentTableName().getString()), IndexUtil
                .getDataColumnFamilyName(indexColumnName), IndexUtil
                .getDataColumnName(indexColumnName));
        position = context.getDataColumnPosition(this.getColumn());
        columns = context.getDataColumns();
    }

    protected LocalIndexDataColumnRef(LocalIndexDataColumnRef localIndexDataColumnRef, long timestamp) {
        super(localIndexDataColumnRef, timestamp);
        this.position = localIndexDataColumnRef.position;
        this.columns = localIndexDataColumnRef.columns;
    }

    @Override
    public ColumnRef cloneAtTimestamp(long timestamp) {
        return new LocalIndexDataColumnRef(this, timestamp);
    }

    @Override
    public ColumnExpression newColumnExpression(boolean schemaNameCaseSensitive, boolean colNameCaseSensitive) {
        String displayName = this.getTableRef().getColumnDisplayName(this, schemaNameCaseSensitive, colNameCaseSensitive);
        return new ProjectedColumnExpression(this.getColumn(), columns, position, displayName);
    }
}
