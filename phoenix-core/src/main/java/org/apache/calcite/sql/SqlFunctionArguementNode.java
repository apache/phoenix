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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlFunctionArguementNode extends SqlNode {

    public final SqlDataTypeNode typeNode;
    public final boolean isConstant;
    public final SqlNode defaultValue;
    public final SqlNode minValue;
    public final SqlNode maxValue;

    public SqlFunctionArguementNode(SqlParserPos pos, SqlDataTypeNode typeNode, boolean isConstant,
            SqlNode defaultValue, SqlNode minValue, SqlNode maxValue) {
        super(pos);
        this.typeNode = typeNode;
        this.isConstant = isConstant;
        this.defaultValue = defaultValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public <R> R accept(SqlVisitor<R> arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode arg0, Litmus arg1) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void unparse(SqlWriter arg0, int arg1, int arg2) {
        // TODO Auto-generated method stub

    }

    @Override
    public void validate(SqlValidator arg0, SqlValidatorScope arg1) {
        // TODO Auto-generated method stub

    }

}
