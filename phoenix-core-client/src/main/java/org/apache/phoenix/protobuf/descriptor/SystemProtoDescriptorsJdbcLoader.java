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
package org.apache.phoenix.protobuf.descriptor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;

/**
 * {@link ProtoDescriptorRegistry.Loader} implementation that reads descriptor bytes from
 * {@code SYSTEM.PROTO_DESCRIPTORS}.
 */
public final class SystemProtoDescriptorsJdbcLoader implements ProtoDescriptorRegistry.Loader {

  private static final String SELECT_BYTES_SQL =
    "SELECT " + PhoenixDatabaseMetaData.FILE_DESCRIPTOR_SET + " FROM "
      + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME + " WHERE "
      + PhoenixDatabaseMetaData.PROTOBUF_NAME + " = ?";

  private static final int LOAD_TIMEOUT_SECONDS = 10;

  private final Supplier<Connection> connectionSupplier;

  public SystemProtoDescriptorsJdbcLoader(Supplier<Connection> connectionSupplier) {
    if (connectionSupplier == null) {
      throw new IllegalArgumentException("Connection supplier must not be null");
    }
    this.connectionSupplier = connectionSupplier;
  }

  @Override
  public byte[] load(String fullyQualifiedMessageName) throws IOException {
    Connection conn;
    try {
      conn = connectionSupplier.get();
    } catch (RuntimeException e) {
      throw new IOException("Failed to obtain JDBC connection for PROTOBUF descriptor '"
        + fullyQualifiedMessageName + "' load", e);
    }
    if (conn == null) {
      throw new IOException("Connection supplier returned null for PROTOBUF descriptor '"
        + fullyQualifiedMessageName + "' load");
    }
    try {
      try (PreparedStatement ps = conn.prepareStatement(SELECT_BYTES_SQL)) {
        ps.setQueryTimeout(LOAD_TIMEOUT_SECONDS);
        ps.setString(1, fullyQualifiedMessageName);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            return null;
          }
          return rs.getBytes(1);
        }
      } catch (SQLException e) {
        throw new IOException("Failed to load PROTOBUF descriptor '" + fullyQualifiedMessageName
          + "' from " + PhoenixDatabaseMetaData.SYSTEM_PROTO_DESCRIPTORS_NAME, e);
      }
    } finally {
      try {
        conn.close();
      } catch (SQLException ignored) {
      }
    }
  }
}
