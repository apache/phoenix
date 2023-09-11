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

package org.apache.phoenix.parse;

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.SchemaUtil;

import java.util.Objects;

/**
 * ParseNode implementation for SHOW CREATE TABLE statements.
 */
public class ShowCreateTableStatement extends ShowCreateTable {

    private TableName tableName;

    public ShowCreateTableStatement(TableName tn) {
        tableName = tn;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        Preconditions.checkNotNull(buf);
        buf.append("SHOW CREATE TABLE ");

        buf.append(SchemaUtil
                .getEscapedTableName(tableName.getSchemaName(), tableName.getTableName()));
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        toSQL(null, buf);
        return buf.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ShowCreateTableStatement)) return false;
        ShowCreateTableStatement stmt = (ShowCreateTableStatement) other;
        return Objects.equals(tableName, stmt.getTableName());
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName);
    }
}
