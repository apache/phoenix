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
package org.apache.phoenix.util;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.PhoenixConnection;

/**
 * Utility class for HA Group Store testing operations.
 */
public class HAGroupStoreTestUtil {

  /**
   * Upserts an HA group record into the system table for testing purposes.
   * @param haGroupName       the HA group name
   * @param zkUrl             the ZooKeeper URL for the local cluster
   * @param peerZKUrl         the ZooKeeper URL for the peer cluster
   * @param localClusterRole  the role of the local cluster
   * @param peerClusterRole   the role of the peer cluster
   * @param overrideConnZkUrl optional override for the connection ZK URL
   * @throws SQLException if the database operation fails
   */
  public static void upsertHAGroupRecordInSystemTable(String haGroupName, String zkUrl,
    String peerZKUrl, ClusterRoleRecord.ClusterRole localClusterRole,
    ClusterRoleRecord.ClusterRole peerClusterRole, String overrideConnZkUrl) throws SQLException {
    try (
      PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(JDBC_PROTOCOL_ZK
        + JDBC_PROTOCOL_SEPARATOR + (overrideConnZkUrl != null ? overrideConnZkUrl : zkUrl));
      Statement stmt = conn.createStatement()) {
      // Only insert values that are not null
      StringBuilder queryBuilder =
        new StringBuilder("UPSERT INTO " + SYSTEM_HA_GROUP_NAME + " (HA_GROUP_NAME, ");
      if (zkUrl != null) {
        queryBuilder.append("ZK_URL_1, ");
      }
      if (peerZKUrl != null) {
        queryBuilder.append("ZK_URL_2, ");
      }
      if (localClusterRole != null) {
        queryBuilder.append("CLUSTER_ROLE_1, ");
      }
      if (peerClusterRole != null) {
        queryBuilder.append("CLUSTER_ROLE_2, ");
      }
      if (zkUrl != null) {
        queryBuilder.append("CLUSTER_URL_1, ");
      }
      if (peerZKUrl != null) {
        queryBuilder.append("CLUSTER_URL_2, ");
      }
      queryBuilder.append("POLICY, VERSION) ");
      queryBuilder.append("VALUES ('" + haGroupName + "', ");
      if (zkUrl != null) {
        queryBuilder.append("'" + zkUrl + "', ");
      }
      if (peerZKUrl != null) {
        queryBuilder.append("'" + peerZKUrl + "', ");
      }
      if (localClusterRole != null) {
        queryBuilder.append("'" + localClusterRole + "', ");
      }
      if (peerClusterRole != null) {
        queryBuilder.append("'" + peerClusterRole + "', ");
      }
      if (zkUrl != null) {
        queryBuilder.append("'" + zkUrl + "', ");
      }
      if (peerZKUrl != null) {
        queryBuilder.append("'" + peerZKUrl + "', ");
      }
      queryBuilder.append("'FAILOVER', 1)");
      stmt.executeUpdate(queryBuilder.toString());
      conn.commit();
    }
  }

  /**
   * Deletes an HA group record from the system table for testing purposes.
   * @param haGroupName the HA group name to delete
   * @param zkUrl       the ZooKeeper URL to connect to
   * @throws SQLException if the database operation fails
   */
  public static void deleteHAGroupRecordInSystemTable(String haGroupName, String zkUrl)
    throws SQLException {
    // Delete the record from System Table
    try (
      PhoenixConnection conn = (PhoenixConnection) DriverManager
        .getConnection(JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "DELETE FROM " + SYSTEM_HA_GROUP_NAME + " WHERE HA_GROUP_NAME = '" + haGroupName + "'");
      conn.commit();
    }
  }

  /**
   * Deletes all HA group records from the system table for testing purposes.
   * @param zkUrl the ZooKeeper URL to connect to
   * @throws SQLException if the database operation fails
   */
  public static void deleteAllHAGroupRecordsInSystemTable(String zkUrl) throws SQLException {
    // Delete all records from System Table
    try (
      PhoenixConnection conn = (PhoenixConnection) DriverManager
        .getConnection(JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
      Statement stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM " + SYSTEM_HA_GROUP_NAME);
      conn.commit();
    }
  }
}
