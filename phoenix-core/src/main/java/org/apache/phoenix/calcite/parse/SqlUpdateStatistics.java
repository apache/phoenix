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
import org.apache.phoenix.schema.stats.StatisticsCollectionScope;

import java.util.List;

/**
 * Parse tree node for SQL {@code UPDATE STATISTICS} command.
 */
public class SqlUpdateStatistics extends SqlCall {
    public static final SqlOperator OPERATOR = new SqlDdlOperator("UPDATE STATISTICS", SqlKind.OTHER_DDL);

    public final SqlIdentifier tableName;
    public final StatisticsCollectionScope scope;
    public final SqlNodeList options;
    
    private final SqlLiteral scopeLiteral;
    

    /** Creates a UPDATE STATISTICS. */
    public SqlUpdateStatistics(
            SqlParserPos pos,
            SqlIdentifier tableName,
            StatisticsCollectionScope scope,
            SqlNodeList options) {
        super(pos);
        this.tableName = tableName;
        this.scope = scope;
        this.options = options;
        this.scopeLiteral = SqlLiteral.createCharString(scope.name(), SqlParserPos.ZERO);
    }

    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableList.of(tableName, scopeLiteral, options);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // TODO Auto-generated method stub
    }
}
