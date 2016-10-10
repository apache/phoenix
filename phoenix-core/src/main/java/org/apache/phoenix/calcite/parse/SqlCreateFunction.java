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

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

public class SqlCreateFunction extends SqlCall {
    public static final SqlOperator OPERATOR = new SqlDdlOperator("CREATE FUNCTION",
            SqlKind.OTHER_DDL);
    public final SqlIdentifier functionName;
    public final SqlLiteral replace;
    public final SqlLiteral tempFunction;
    public final SqlNodeList functionArguements;
    public final SqlIdentifier returnType;
    public final SqlNode className;
    public final SqlNode jarPath;

    public SqlCreateFunction(SqlParserPos pos, SqlIdentifier functionName, SqlLiteral replace,
            SqlLiteral tempFunction, SqlNodeList functionArguements, SqlIdentifier returnType,
            SqlNode className, SqlNode jarPath) {
        super(pos);
        this.functionName = functionName;
        this.replace = replace;
        this.tempFunction = tempFunction;
        this.functionArguements = functionArguements;
        this.returnType = returnType;
        this.className = className;
        this.jarPath = jarPath;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableList.of(functionName, replace, tempFunction, functionArguements,
            returnType, className, jarPath);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter arg0, int arg1, int arg2) {
        // TODO Auto-generated method stub
    }
}
