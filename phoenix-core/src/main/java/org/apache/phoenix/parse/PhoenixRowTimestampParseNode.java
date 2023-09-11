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

package org.apache.phoenix.parse;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.PhoenixRowTimestampFunction;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class PhoenixRowTimestampParseNode extends FunctionParseNode {

    PhoenixRowTimestampParseNode(String name, List<ParseNode> children,
                                 BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    /**
     * Note: Although this ParseNode does not take any children, we are injecting an EMPTY_COLUMN
     * KeyValueColumnExpression so that the EMPTY_COLUMN is evaluated during scan filter processing.
     */
    public FunctionExpression create(List<Expression> children, StatementContext context)
            throws SQLException {

        // PhoenixRowTimestampFunction does not take any parameters.
        if (children.size() != 0) {
            throw new IllegalArgumentException(
                    "PhoenixRowTimestampFunction does not take any parameters"
            );
        }

        // Get the empty column family and qualifier for the context.
        PTable table = context.getCurrentTable().getTable();
        byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(table);
        byte[] emptyColumnName =
                table.getEncodingScheme() == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS
                        ? QueryConstants.EMPTY_COLUMN_BYTES
                        : table.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

        // Create an empty column key value expression.
        // This will cause the empty column key value to be evaluated during scan filter processing.
        Expression emptyColumnExpression = new KeyValueColumnExpression(new PDatum() {
            @Override
            public boolean isNullable() {
                return false;
            }
            @Override
            public PDataType getDataType() {
                return PDate.INSTANCE;
            }
            @Override
            public Integer getMaxLength() {
                return null;
            }
            @Override
            public Integer getScale() {
                return null;
            }
            @Override
            public SortOrder getSortOrder() {
                return SortOrder.getDefault();
            }
        }, emptyColumnFamilyName, emptyColumnName);
        List<Expression> expressionList = Arrays.asList(new Expression[] {emptyColumnExpression});
        context.getScan().setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME, emptyColumnFamilyName);
        context.getScan().setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME, emptyColumnName);
        return new PhoenixRowTimestampFunction(expressionList);
    }
}
