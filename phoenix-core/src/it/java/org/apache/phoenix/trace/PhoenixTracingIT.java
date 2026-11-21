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
package org.apache.phoenix.trace;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.opentelemetry.api.trace.Span;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verifies that the tracing hooks are still wired up after the move to OpenTelemetry, and that they
 * are inert when no OpenTelemetry SDK has been installed. Without an SDK every span is the no-op
 * {@code PropagatedSpan}, so there is nothing to assert about exported traces here.
 */
@Category(ParallelStatsDisabledTest.class)
public class PhoenixTracingIT extends ParallelStatsDisabledIT {

  @Test
  public void testTracingIsANoOpWithoutAnSdk() throws Exception {
    assertFalse(PhoenixTracing.isRecording());
    Span span = PhoenixTracing.createSpan("test.span");
    assertFalse(span.isRecording());
    assertFalse(span.getSpanContext().isValid());
    span.end();
  }

  @Test
  public void testTraceOnAndOffAreAccepted() throws Exception {
    try (Connection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      assertNull(pconn.getTraceSpan());

      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("TRACE ON")) {
          // Without an SDK the span context is invalid, so no trace id row is produced.
          rs.next();
        }
        assertNotNull(pconn.getTraceSpan());

        String table = generateUniqueName();
        stmt.execute("CREATE TABLE " + table + " (K VARCHAR PRIMARY KEY, V VARCHAR)");
        stmt.execute("UPSERT INTO " + table + " VALUES ('a', 'b')");
        conn.commit();
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
          assertTrue(rs.next());
        }

        try (ResultSet rs = stmt.executeQuery("TRACE OFF")) {
          rs.next();
        }
        assertNull(pconn.getTraceSpan());
      }
    }
  }

  /**
   * A Scope is bound to the thread that opened it, so Phoenix must not leave one open on a
   * connection. If it did, the thread running the statement would stay pinned to a stale Context
   * after the connection was closed.
   */
  @Test
  public void testTraceOnDoesNotPinTheCurrentContext() throws Exception {
    try (Connection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("TRACE ON")) {
        rs.next();
      }
    }
    assertFalse(Span.current().getSpanContext().isValid());
  }
}
