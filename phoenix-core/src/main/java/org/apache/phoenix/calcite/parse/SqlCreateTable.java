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
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.PrimaryKeyConstraint;

import java.util.List;

/**
 * Parse tree node for SQL {@code CREATE TABLE} command.
 */
public class SqlCreateTable extends SqlCall {
    public static final SqlOperator OPERATOR = new SqlDdlOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    public final SqlIdentifier tableName;
    public  final SqlLiteral ifNotExists;
    public final SqlNodeList columnDefs;
    public final SqlIdentifier pkConstraint;
    public final SqlNodeList pkConstraintColumnDefs;
    public final SqlNodeList tableOptionsKeyList;
    public final SqlNodeList tableOptionsValueList;
    public final SqlNodeList splitKeyList;
    

    /** Creates a CREATE TABLE. */
    public SqlCreateTable(SqlParserPos pos, SqlIdentifier tableName, SqlLiteral ifNotExists, SqlNodeList columnDefs, SqlIdentifier pkConstraint, SqlNodeList pkConstraintColumnDefs, SqlNodeList tableOptionsKeyList, SqlNodeList tableOptionsValueList, SqlNodeList splitKeyList) {
        super(pos);
        this.tableName = tableName;
        this.ifNotExists = ifNotExists;
        this.columnDefs = columnDefs;
        this.pkConstraint = pkConstraint;
        this.pkConstraintColumnDefs = pkConstraintColumnDefs;
        this.tableOptionsKeyList = tableOptionsKeyList;
        this.tableOptionsValueList = tableOptionsValueList;
        this.splitKeyList = splitKeyList;
    }

    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableList.of(tableName, ifNotExists, columnDefs, pkConstraint, pkConstraintColumnDefs, tableOptionsKeyList, tableOptionsValueList, splitKeyList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE TABLE");
        if(ifNotExists != null) {
        	writer.keyword("IF NOT");
        	ifNotExists.unparse(writer, 0, 0);
        }
        tableName.unparse(writer, 0, 0);
        ((SqlDdlOperator)getOperator()).unparseListClause(writer, columnDefs);
        if(pkConstraint != null) {
        	writer.keyword(",");
        	writer.keyword("CONSTRANT");
        	pkConstraint.unparse(writer, 0, 0);
        	writer.keyword("PRIMARY KEY");
        	((SqlDdlOperator)getOperator()).unparseListClause(writer, pkConstraintColumnDefs);
        }
        if(splitKeyList != null) {
        	writer.keyword("SPLIT ON");
            ((SqlDdlOperator)getOperator()).unparseListClause(writer, splitKeyList);
        }
    }
}
