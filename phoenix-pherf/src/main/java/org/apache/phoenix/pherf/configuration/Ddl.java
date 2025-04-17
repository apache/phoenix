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
package org.apache.phoenix.pherf.configuration;

import javax.xml.bind.annotation.XmlAttribute;

public class Ddl {
  private String statement;
  private String tableName;
  private boolean useGlobalConnection;

  public Ddl() {
  }

  public Ddl(String statement, String tableName) {
    this.statement = statement;
    this.tableName = tableName;
  }

  /**
   * DDL
   */
  @XmlAttribute
  public String getStatement() {
    return statement;
  }

  public void setStatement(String statement) {
    this.statement = statement;
  }

  /**
   * Table name used in the DDL
   */
  @XmlAttribute
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @XmlAttribute
  public boolean isUseGlobalConnection() {
    return useGlobalConnection;
  }

  public void setUseGlobalConnection(boolean useGlobalConnection) {
    this.useGlobalConnection = useGlobalConnection;
  }

  public String toString() {
    if (statement.contains("?")) {
      return statement.replace("?", tableName);
    } else {
      return statement;
    }

  }

}
