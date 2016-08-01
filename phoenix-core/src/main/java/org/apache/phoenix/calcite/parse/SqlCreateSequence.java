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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

public class SqlCreateSequence extends SqlCall {
    public static final SqlOperator OPERATOR = new SqlDdlOperator("CREATE SEQUENCE", SqlKind.CREATE_SEQUENCE);
    
    public final SqlIdentifier sequenceName;
    public final SqlLiteral ifNotExists;
    public final SqlLiteral startWith;
    public final SqlLiteral incrementBy;
    public final SqlLiteral minValue;
    public final SqlLiteral maxValue;
    public final SqlLiteral cycle;
    public final SqlLiteral cache;

    public SqlCreateSequence(
            SqlParserPos pos,
            SqlIdentifier sequenceName,
            SqlLiteral ifNotExists,
            SqlLiteral startWith,
            SqlLiteral incrementBy,
            SqlLiteral minValue,
            SqlLiteral maxValue,
            SqlLiteral cycle,
            SqlLiteral cache) {
        super(pos);
        this.sequenceName = sequenceName;
        this.ifNotExists = ifNotExists;
        this.startWith = startWith;
        this.incrementBy = incrementBy;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.cycle = cycle;
        this.cache = cache;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(sequenceName, ifNotExists, startWith, incrementBy, minValue, maxValue, cycle, cache);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // TODO Auto-generated method stub
    }

}
