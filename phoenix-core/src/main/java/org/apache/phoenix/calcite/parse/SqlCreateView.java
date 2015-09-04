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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Parse tree node for SQL {@code CREATE VIEW} command.
 */
public class SqlCreateView extends SqlCall {
    public static final SqlOperator OPERATOR = new SqlDdlOperator("CREATE VIEW", SqlKind.CREATE_VIEW);

    public final SqlIdentifier name;
    public final SqlNode query;
    public final String queryString;

    /** Creates a CREATE VIEW. */
    public SqlCreateView(SqlParserPos pos, SqlIdentifier name, SqlNode query, String queryString) {
        super(pos);
        this.name = name;
        this.query = query;
        this.queryString = queryString;
    }

    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name, query);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE VIEW");
        name.unparse(writer, 0, 0);
        writer.keyword(" ");
        query.unparse(writer, 0, 0);
    }
}
