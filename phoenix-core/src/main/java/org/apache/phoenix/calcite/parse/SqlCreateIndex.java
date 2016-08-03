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
package org.apache.phoenix.calcite.parse;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import java.util.List;

/**
 * Parse tree node for SQL {@code CREATE INDEX} command.
 */
public class SqlCreateIndex extends SqlCall {
    public static final SqlOperator OPERATOR = new SqlDdlOperator("CREATE INDEX", SqlKind.CREATE_INDEX);

    public final SqlIdentifier indexName;
    public final SqlLiteral isLocal;
    public final SqlLiteral ifNotExists;
    public final SqlIdentifier dataTableName;
    public final SqlNodeList expressions;
    public final SqlNodeList includeColumns;
    public final SqlLiteral async;
    public final SqlNodeList indexOptions;
    public final SqlNodeList splitKeyList;
    

    /** Creates a CREATE INDEX. */
    public SqlCreateIndex(
            SqlParserPos pos,
            SqlIdentifier indexName,
            SqlLiteral isLocal,
            SqlLiteral ifNotExists,
            SqlIdentifier dataTableName,
            SqlNodeList expressions,
            SqlNodeList includeColumns,
            SqlLiteral async,
            SqlNodeList indexOptions,
            SqlNodeList splitKeyList) {
        super(pos);
        this.indexName = indexName;
        this.isLocal = isLocal;
        this.ifNotExists = ifNotExists;
        this.dataTableName = dataTableName;
        this.expressions = expressions;
        this.includeColumns = includeColumns;
        this.async = async;
        this.indexOptions = indexOptions;
        this.splitKeyList = splitKeyList;
    }

    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableList.of(indexName, isLocal, ifNotExists, dataTableName, expressions, includeColumns, async, indexOptions, splitKeyList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // TODO Auto-generated method stub
    }
}
