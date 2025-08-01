/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import java.sql.SQLException;
import org.apache.phoenix.compile.ColumnResolver;

/**
 * Abstract base class for FROM clause data sources
 * @since 0.1
 */
public abstract class TableNode {
  private final String alias;

  TableNode(String alias) {
    this.alias = alias;
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public final String toString() {
    StringBuilder buf = new StringBuilder();
    toSQL(null, buf);
    return buf.toString();
  }

  public abstract <T> T accept(TableNodeVisitor<T> visitor) throws SQLException;

  public abstract void toSQL(ColumnResolver resolver, StringBuilder buf);
}
