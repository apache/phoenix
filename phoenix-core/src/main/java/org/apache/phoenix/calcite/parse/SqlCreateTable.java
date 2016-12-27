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
 * Parse tree node for SQL {@code CREATE TABLE} command.
 */
public class SqlCreateTable extends SqlCall {
    public final SqlOperator operator;

    public final SqlIdentifier tableName;
    public final SqlLiteral immutable;
    public final SqlLiteral ifNotExists;
    public final SqlNodeList columnDefs;
    public final SqlIdentifier pkConstraint;
    public final SqlNodeList pkConstraintColumnDefs;
    public final SqlIdentifier baseTableName;
    public final SqlNode whereNode;
    public final SqlNodeList tableOptions;
    public final SqlNodeList splitKeyList;
    

    /** Creates a CREATE TABLE. */
    public SqlCreateTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlLiteral immutable,
            SqlLiteral ifNotExists,
            SqlNodeList columnDefs,
            SqlIdentifier pkConstraint,
            SqlNodeList pkConstraintColumnDefs,
            SqlNodeList tableOptions,
            SqlNodeList splitKeyList) {
        super(pos);
        this.operator = new SqlDdlOperator("CREATE TABLE", SqlKind.CREATE_TABLE);
        this.tableName = tableName;
        this.immutable = immutable;
        this.ifNotExists = ifNotExists;
        this.columnDefs = columnDefs;
        this.pkConstraint = pkConstraint;
        this.pkConstraintColumnDefs = pkConstraintColumnDefs;
        this.baseTableName = null;
        this.whereNode = null;
        this.tableOptions = tableOptions;
        this.splitKeyList = splitKeyList;
    }
    
    /** Creates a CREATE VIEW. */
    public SqlCreateTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlLiteral immutable,
            SqlLiteral ifNotExists,
            SqlNodeList columnDefs,
            SqlIdentifier baseTableName,
            SqlNode whereNode,
            SqlNodeList tableOptions) {
        super(pos);
        this.operator = new SqlDdlOperator("CREATE VIEW", SqlKind.CREATE_VIEW);
        this.tableName = tableName;
        this.immutable = immutable;
        this.ifNotExists = ifNotExists;
        this.columnDefs = columnDefs;
        this.pkConstraint = null;
        this.pkConstraintColumnDefs = SqlNodeList.EMPTY;
        this.baseTableName = baseTableName;
        this.whereNode = whereNode;
        this.tableOptions = tableOptions;
        this.splitKeyList = SqlNodeList.EMPTY;
    }

    public SqlOperator getOperator() {
        return operator;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableList.of(tableName, ifNotExists, columnDefs, pkConstraint, pkConstraintColumnDefs, baseTableName, whereNode, tableOptions, splitKeyList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(operator.getName());
        if(SqlLiteral.value(ifNotExists).equals(Boolean.TRUE)) {
        	writer.keyword("IF NOT EXISTS");
        }
        tableName.unparse(writer, 0, 0);
        ((SqlDdlOperator)getOperator()).unparseListClause(writer, columnDefs);
        if(pkConstraint != null) {
        	writer.keyword("CONSTRANT");
        	pkConstraint.unparse(writer, 0, 0);
        	writer.keyword("PRIMARY KEY");
        	((SqlDdlOperator)getOperator()).unparseListClause(writer, pkConstraintColumnDefs);
        }
        if (baseTableName != null) {
            writer.keyword("AS SELECT * FROM");
            baseTableName.unparse(writer, 0, 0);
            if (whereNode != null) {
                writer.keyword("WHERE");
                whereNode.unparse(writer, 0, 0);
            }
        }
        if (SqlNodeList.isEmptyList(tableOptions)) {
            ((SqlDdlOperator)getOperator()).unparseListClause(writer, tableOptions);            
        }
        if (SqlNodeList.isEmptyList(splitKeyList)) {
        	writer.keyword("SPLIT ON");
            ((SqlDdlOperator)getOperator()).unparseListClause(writer, splitKeyList);
        }
    }
}
