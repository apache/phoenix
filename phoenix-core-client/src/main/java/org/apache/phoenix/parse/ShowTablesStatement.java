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

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.compile.ColumnResolver;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * ParseNode implementation for SHOW TABLES {@code [IN <schema>] }.
 */
public class ShowTablesStatement extends ShowStatement {
    // Schema for table listing. null implies the the db for this connection is used.
    @Nullable
    private String targetSchema;

    // Pattern to be matched while looking up for tables in 'targetSchema'.
    // null implies everything is returned.
    @Nullable
    private String dbPattern;

    public  ShowTablesStatement() {
        this(null, null);
    }

    public ShowTablesStatement(@Nullable String schema, @Nullable String pattern) {
        targetSchema = schema;
        dbPattern = pattern;
    }

    @Nullable
    public String getTargetSchema() {
        return targetSchema;
    }

    @Nullable
    public String getDbPattern() {
        return dbPattern;
    }

    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        Preconditions.checkNotNull(buf);
        buf.append("SHOW TABLES");
        if (targetSchema != null) {
            buf.append(" IN ");
            buf.append(targetSchema);
            buf.append(" ");
        }
        if (dbPattern != null) {
            buf.append(" LIKE ");
            buf.append("'").append(dbPattern).append("'");
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        toSQL(null, buf);
        return buf.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ShowTablesStatement)) return false;
        ShowTablesStatement stmt = (ShowTablesStatement) other;
        return Objects.equals(targetSchema, stmt.getTargetSchema()) && Objects.equals(dbPattern,
            stmt.getDbPattern());
    }

    @Override
    public int hashCode() {
      return Objects.hash(targetSchema, dbPattern);
    }
}
