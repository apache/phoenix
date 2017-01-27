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

public class SqlDataTypeNode extends SqlNode {
    public final String typeName;
    public final Integer maxLength;
    public final Integer scale;
    public final boolean isArray;
    public final Integer arraySize;

    public SqlDataTypeNode(
            SqlParserPos pos,
            SqlIdentifier typeName,
            Integer maxLength,
            Integer scale,
            boolean isArray,
            Integer arraySize) {
        super(pos);
        if (typeName.isSimple()) {
            this.typeName = typeName.getSimple();
        } else {
            throw new RuntimeException("Invalid data type name: " + typeName);
        }
        this.maxLength = maxLength;
        this.scale = scale;
        this.isArray = isArray;
        this.arraySize = arraySize;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // TODO Auto-generated method stub
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // TODO Auto-generated method stub
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        // TODO Auto-generated method stub
        return null;
    }

}
