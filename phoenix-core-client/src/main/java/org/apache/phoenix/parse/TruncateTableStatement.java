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

import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTableType;

public class TruncateTableStatement extends MutableStatement {
  private final TableName tableName;
  private final PTableType tableType;
  private final boolean preserveSplits;

  public TruncateTableStatement(TruncateTableStatement truncateTableStatement) {
    this.tableName = truncateTableStatement.tableName;
    this.tableType = truncateTableStatement.tableType;
    this.preserveSplits = true;
  }

  protected TruncateTableStatement(TableName tableName, PTableType tableType,
    boolean preserveSplits) {
    this.tableName = tableName;
    this.tableType = PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA.equals(tableName.getSchemaName())
      ? PTableType.SYSTEM
      : tableType;
    this.preserveSplits = preserveSplits;
  }

  public TableName getTableName() {
    return tableName;
  }

  public PTableType getTableType() {
    return tableType;
  }

  public boolean preserveSplits() {
    return preserveSplits;
  }

  @Override
  public int getBindCount() {
    return 0;
  }
}
