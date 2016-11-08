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
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementor;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementorImpl;
import org.apache.phoenix.execute.RuntimeContext;

public class SqlOptionNode extends SqlNode {
    public final String familyName;
    public final String propertyName;
    public final Object value;

    public SqlOptionNode(SqlParserPos pos, SqlIdentifier key, SqlLiteral literal) {
        super(pos);
        if (key.isSimple()) {
            familyName = "";
            propertyName = key.getSimple();
        } else {
            familyName = key.names.get(0);
            propertyName = key.names.get(1);
        }

        PhoenixRelImplementor implementor =
                new PhoenixRelImplementorImpl(null, RuntimeContext.EMPTY_CONTEXT);
        this.value = CalciteUtils.convertSqlLiteral(literal, implementor);
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

}
