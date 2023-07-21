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

/**
 * Even when a column is not covered by an index table for a given query, we may still want to
 * use index in the query plan and fetch the missing columns from the data table rows on the
 * server side. This class is used to keep track of such data columns.
 */
public class IndexUncoveredDataColumnRef extends ColumnRef {
    final private int position;
    // Despite the final keyword, columns IS mutable, and must not be used for equality/hashCode
    final private Set<PColumn> columns;
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();

    public IndexUncoveredDataColumnRef(StatementContext context, TableRef tRef, String indexColumnName)
            throws MetaDataEntityNotFoundException, SQLException {
        super(FromCompiler.getResolver(
            FACTORY.namedTable(
                null,
                TableName.create(tRef.getTable().getSchemaName().getString(), tRef.getTable()
                        .getParentTableName().getString())), context.getConnection(), false)
                .resolveTable(context.getCurrentTable().getTable().getSchemaName().getString(),
                    tRef.getTable().getParentTableName().getString()),
                IndexUtil.getDataColumnFamilyName(indexColumnName), IndexUtil
                        .getDataColumnName(indexColumnName));
        position = context.getDataColumnPosition(this.getColumn());
        columns = context.getDataColumns();
    }

    protected IndexUncoveredDataColumnRef(IndexUncoveredDataColumnRef indexDataColumnRef, long timestamp) {
        super(indexDataColumnRef, timestamp);
        this.position = indexDataColumnRef.position;
        this.columns = indexDataColumnRef.columns;
    }

    @Override
    public ColumnRef cloneAtTimestamp(long timestamp) {
        return new IndexUncoveredDataColumnRef(this, timestamp);
    }

    @Override
    public ColumnExpression newColumnExpression(boolean schemaNameCaseSensitive, boolean colNameCaseSensitive) {
        String displayName = this.getTableRef().getColumnDisplayName(this, schemaNameCaseSensitive, colNameCaseSensitive);
        return new ProjectedColumnExpression(this.getColumn(), columns, position, displayName);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + position;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        IndexUncoveredDataColumnRef that = (IndexUncoveredDataColumnRef) o;
        return position == that.position;
    }
}
