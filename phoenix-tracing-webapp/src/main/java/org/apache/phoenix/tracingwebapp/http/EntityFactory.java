/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.tracingwebapp.http;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * EntityFactory is used to get results entities For SQL query
 */
public class EntityFactory {

  private String queryString;
  protected Connection connection;

  public EntityFactory(Connection connection, String queryString) {
    this.queryString = queryString;
    this.connection = connection;
  }

  public List<Map<String, Object>> findMultiple()
      throws SQLException {
    ResultSet rs = null;
    PreparedStatement ps = null;
    try {
      ps = this.connection.prepareStatement(this.queryString);
      rs = ps.executeQuery();
      return getEntitiesFromResultSet(rs);
    } catch (SQLException e) {
      throw (e);
    } finally {
      if (rs != null) {
        rs.close();
      }
      if (ps != null) {
        ps.close();
      }
    }
  }

  protected static List<Map<String, Object>> getEntitiesFromResultSet(
      ResultSet resultSet) throws SQLException {
    ArrayList<Map<String, Object>> entities = new ArrayList<>();
    while (resultSet.next()) {
      entities.add(getEntityFromResultSet(resultSet));
    }
    return entities;
  }

  protected static Map<String, Object> getEntityFromResultSet(ResultSet resultSet)
      throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    Map<String, Object> resultsMap = new HashMap<>();
    for (int i = 1; i <= columnCount; ++i) {
      String columnName = metaData.getColumnName(i).toLowerCase();
      Object object = resultSet.getObject(i);
      resultsMap.put(columnName, object);
    }
    return resultsMap;
  }

}
