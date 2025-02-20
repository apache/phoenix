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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.DecodeViewIndexIdFunction;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.IndexUtil;

import java.sql.SQLException;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE;

public class DecodeViewIndexIdParseNode extends FunctionParseNode {

    DecodeViewIndexIdParseNode(String name, List<ParseNode> children,
                                 BuiltInFunctionInfo info) {
        super(name, children, info);
        // It takes 2 parameters - VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE.
        if (children.size() != 2) {
            throw new IllegalArgumentException(
                    "DecodeViewIndexIdParseNode should only have "
                            + "VIEW_INDEX_ID and VIEW_INDEX_ID_DATA_TYPE parse nodes."
            );
        }
        if (children.get(0).getClass().isAssignableFrom(ColumnParseNode.class)
                && children.get(1).getClass().isAssignableFrom(ColumnParseNode.class)
                && (!(((ColumnParseNode) children.get(0)).getName().equals(VIEW_INDEX_ID))
                || !(((ColumnParseNode) children.get(1)).getName().equals(VIEW_INDEX_ID_DATA_TYPE)))
        ) {
            throw new IllegalArgumentException(
                    "DecodeViewIndexIdParseNode should only have "
                            + "VIEW_INDEX_ID and VIEW_INDEX_ID_DATA_TYPE parse nodes."
            );
        }

        // CastPastNode is generated during IndexStatement rewriting
        if (children.get(0).getClass().isAssignableFrom(CastParseNode.class)
                && children.get(1).getClass().isAssignableFrom(CastParseNode.class)
                && (!((ColumnParseNode) (((CastParseNode) children.get(0)).getChildren().get(0))).getName().equals(
                IndexUtil.getIndexColumnName(QueryConstants.DEFAULT_COLUMN_FAMILY, VIEW_INDEX_ID))
                || !((ColumnParseNode) (((CastParseNode) children.get(1)).getChildren().get(0))).getName().equals(
                IndexUtil.getIndexColumnName(QueryConstants.DEFAULT_COLUMN_FAMILY, VIEW_INDEX_ID_DATA_TYPE)))
        ) {
            throw new IllegalArgumentException(
                    "DecodeViewIndexIdParseNode should only have "
                            + "VIEW_INDEX_ID and VIEW_INDEX_ID_DATA_TYPE parse nodes."
            );
        }

    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context)
            throws SQLException {
        return new DecodeViewIndexIdFunction(children);
    }

}
