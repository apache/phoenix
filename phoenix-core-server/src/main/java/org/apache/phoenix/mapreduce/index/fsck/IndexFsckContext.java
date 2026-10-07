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
package org.apache.phoenix.mapreduce.index.fsck;

import java.sql.Connection;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.index.fsck.RowKeyFormatter.KeyFormat;
import org.apache.phoenix.schema.PTable;

/** Execution context and configuration options for an IndexFsckTool invocation. */
public class IndexFsckContext {
  private final Connection connection;
  private final Configuration configuration;
  private final PTable dataTable;
  private final PTable indexTable;
  private final String tenantId;
  private final Long startTime;
  private final Long endTime;
  private final KeyFormat keyFormat;
  private final boolean confirm;

  public IndexFsckContext(Connection connection, Configuration configuration, PTable dataTable,
    PTable indexTable, String tenantId, Long startTime, Long endTime, KeyFormat keyFormat,
    boolean confirm) {
    this.connection = connection;
    this.configuration = configuration;
    this.dataTable = dataTable;
    this.indexTable = indexTable;
    this.tenantId = tenantId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.keyFormat = keyFormat;
    this.confirm = confirm;
  }

  public Connection getConnection() {
    return connection;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public PTable getDataTable() {
    return dataTable;
  }

  public PTable getIndexTable() {
    return indexTable;
  }

  public String getDataTableName() {
    return dataTable.getName().getString();
  }

  /** Returns the fully qualified index table name. */
  public String getIndexTableName() {
    return indexTable.getName().getString();
  }

  public String getTenantId() {
    return tenantId;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public boolean isConfirm() {
    return confirm;
  }

  public String formatRowKey(byte[] rowKey, PTable table) {
    return RowKeyFormatter.format(rowKey, table, keyFormat);
  }
}
