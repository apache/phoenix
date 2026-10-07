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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class SystemProtoDescriptorsJdbcLoaderTest {

  @Test(expected = IllegalArgumentException.class)
  public void nullSupplierRejected() {
    new SystemProtoDescriptorsJdbcLoader(null);
  }

  @Test
  public void constructorDoesNotOpenConnection() {
    AtomicInteger calls = new AtomicInteger();
    new SystemProtoDescriptorsJdbcLoader(() -> {
      calls.incrementAndGet();
      return mock(Connection.class);
    });
    assertEquals(0, calls.get());
  }

  @Test
  public void nullConnectionFailsLoad() {
    SystemProtoDescriptorsJdbcLoader loader = new SystemProtoDescriptorsJdbcLoader(() -> null);
    try {
      loader.load("test.Account");
      fail("Expected IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("test.Account"));
    }
  }

  @Test
  public void loadHitReturnsBytesFromResultSet() throws Exception {
    byte[] expected = new byte[] { 0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f };
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getBytes(1)).thenReturn(expected);

    SystemProtoDescriptorsJdbcLoader loader = new SystemProtoDescriptorsJdbcLoader(() -> conn);
    byte[] actual = loader.load("test.Account");

    assertArrayEquals(expected, actual);
    verify(ps).setString(1, "test.Account");
    verify(rs).close();
    verify(ps).close();
    verify(conn).close();
  }

  @Test
  public void loadMissReturnsNullWhenNoRowsFound() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    SystemProtoDescriptorsJdbcLoader loader = new SystemProtoDescriptorsJdbcLoader(() -> conn);
    assertNull(loader.load("test.NotRegistered"));
  }

  @Test
  public void loadNullBytesFromResultSetReturnsNull() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getBytes(1)).thenReturn(null);

    SystemProtoDescriptorsJdbcLoader loader = new SystemProtoDescriptorsJdbcLoader(() -> conn);
    assertNull(loader.load("test.NullCell"));
  }

  @Test
  public void sqlExceptionFromPrepareStatementIsWrappedAsIOException() throws Exception {
    Connection conn = mock(Connection.class);
    SQLException underlying = new SQLException("catalog unreachable");
    doThrow(underlying).when(conn).prepareStatement(anyString());

    SystemProtoDescriptorsJdbcLoader loader = new SystemProtoDescriptorsJdbcLoader(() -> conn);
    try {
      loader.load("test.Account");
      fail("Expected IOException");
    } catch (IOException e) {
      assertTrue("Cause should be the original SQLException", e.getCause() == underlying);
      assertTrue("Error message should reference the descriptor: " + e.getMessage(),
        e.getMessage().contains("test.Account"));
      assertTrue("Error message should reference SYSTEM.PROTO_DESCRIPTORS: " + e.getMessage(),
        e.getMessage().contains("PROTO_DESCRIPTORS"));
    }
  }

  @Test
  public void sqlExceptionFromExecuteQueryIsWrappedAsIOException() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenThrow(new SQLException("scan interrupted"));

    SystemProtoDescriptorsJdbcLoader loader = new SystemProtoDescriptorsJdbcLoader(() -> conn);
    try {
      loader.load("test.Account");
      fail("Expected IOException");
    } catch (IOException e) {
      assertTrue(e.getCause() instanceof SQLException);
    }
  }
}
