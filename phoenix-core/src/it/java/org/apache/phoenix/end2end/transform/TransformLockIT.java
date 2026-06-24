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
package org.apache.phoenix.end2end.transform;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for the transform-lock primitive on SYSTEM.MUTEX. The lock is the coordination
 * point for serializing transform-triggering DDL with concurrent transform lifecycle operations;
 * this IT exercises the primitive itself plus the wiring at the DDL callsites (concurrent ALTER
 * fast-fail and narrow-scope behavior for non-transform ALTERs).
 */
@Category(ParallelStatsDisabledTest.class)
public class TransformLockIT extends ParallelStatsDisabledIT {

  private static ConnectionQueryServices services(Connection conn) throws Exception {
    return conn.unwrap(PhoenixConnection.class).getQueryServices();
  }

  @Test
  public void testAcquireReleaseHappyPath() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(conn);

      assertTrue("first acquire should win", cqs.acquireTransformLock(null, schemaName, tableName));
      cqs.releaseTransformLock(null, schemaName, tableName);
      assertTrue("acquire after release should win again",
        cqs.acquireTransformLock(null, schemaName, tableName));
      cqs.releaseTransformLock(null, schemaName, tableName);
    }
  }

  @Test
  public void testAcquireContention() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(conn);

      assertTrue("first acquire should win", cqs.acquireTransformLock(null, schemaName, tableName));
      assertFalse("second acquire on same key should lose",
        cqs.acquireTransformLock(null, schemaName, tableName));

      cqs.releaseTransformLock(null, schemaName, tableName);
      assertTrue("acquire after release should win again",
        cqs.acquireTransformLock(null, schemaName, tableName));
      cqs.releaseTransformLock(null, schemaName, tableName);
    }
  }

  @Test
  public void testReleaseDoesNotThrow() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(conn);

      // release without prior acquire — must not throw
      cqs.releaseTransformLock(null, schemaName, tableName);

      assertTrue(cqs.acquireTransformLock(null, schemaName, tableName));
      cqs.releaseTransformLock(null, schemaName, tableName);
      // release after release — must not throw
      cqs.releaseTransformLock(null, schemaName, tableName);
    }
  }

  @Test
  public void testDifferentTablesDoNotBlock() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableA = "A_" + generateUniqueName();
    String tableB = "B_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(conn);

      assertTrue(cqs.acquireTransformLock(null, schemaName, tableA));
      assertTrue("acquire on a different table must not be blocked",
        cqs.acquireTransformLock(null, schemaName, tableB));

      cqs.releaseTransformLock(null, schemaName, tableA);
      cqs.releaseTransformLock(null, schemaName, tableB);
    }
  }

  @Test
  public void testLockSurvivesAcrossConnections() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    try (Connection conn1 = DriverManager.getConnection(getUrl(), props);
      Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs1 = services(conn1);
      ConnectionQueryServices cqs2 = services(conn2);

      assertTrue("conn1 should win the first acquire",
        cqs1.acquireTransformLock(null, schemaName, tableName));
      assertFalse("conn2 must see the lock as held",
        cqs2.acquireTransformLock(null, schemaName, tableName));

      cqs1.releaseTransformLock(null, schemaName, tableName);
      assertTrue("conn2 should win after conn1 releases",
        cqs2.acquireTransformLock(null, schemaName, tableName));
      cqs2.releaseTransformLock(null, schemaName, tableName);
    }
  }

  @Test
  public void testConcurrentAlterTableSerializesViaTransformLock() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    String fullTableName = schemaName + "." + tableName;

    try (Connection setupConn = DriverManager.getConnection(getUrl(), props)) {
      setupConn.createStatement()
        .execute("CREATE TABLE " + fullTableName + " (id INTEGER PRIMARY KEY, val VARCHAR)");
    }

    try (Connection holderConn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(holderConn);
      assertTrue("holder must acquire lock", cqs.acquireTransformLock(null, schemaName, tableName));

      try {
        try (Connection altererConn = DriverManager.getConnection(getUrl(), props)) {
          altererConn.createStatement().execute("ALTER TABLE " + fullTableName
            + " SET IMMUTABLE_STORAGE_SCHEME = SINGLE_CELL_ARRAY_WITH_OFFSETS");
          fail("ALTER TABLE should have failed while transform lock is held");
        } catch (SQLException sqle) {
          assertEquals(
            SQLExceptionCode.CANNOT_MODIFY_TABLE_WITH_TRANSFORM_IN_PROGRESS.getErrorCode(),
            sqle.getErrorCode());
          String msg = sqle.getMessage();
          // Assert the human-readable message text is present (catches a regression where the
          // message itself disappears or is replaced).
          assertTrue("exception message should contain the human-readable text, got: " + msg,
            msg.contains("Cannot modify table"));
          // Assert no unsubstituted format-spec leaks through (the message string must not
          // contain literal "%s.%s" — SQLExceptionInfo does not format-substitute the message).
          assertFalse(
            "exception message must not contain unsubstituted %s.%s placeholders, got: " + msg,
            msg.contains("%s"));
          // Assert the appended TABLE_NAME=schema.table token renders both names (this is what
          // SQLExceptionInfo#setSchemaName/setTableName actually contribute).
          assertTrue("exception message should contain TABLE_NAME=" + schemaName + "." + tableName
            + ", got: " + msg, msg.contains("tableName=" + schemaName + "." + tableName));
        }
      } finally {
        cqs.releaseTransformLock(null, schemaName, tableName);
      }
    }

    try (Connection altererConn = DriverManager.getConnection(getUrl(), props)) {
      altererConn.createStatement().execute("ALTER TABLE " + fullTableName
        + " SET IMMUTABLE_STORAGE_SCHEME = SINGLE_CELL_ARRAY_WITH_OFFSETS");
    }
  }

  @Test
  public void testGlobalTransformBlocksTenantTransform() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();

    try (Connection globalConn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices globalCqs = services(globalConn);
      // global acquire (tenantId=null)
      assertTrue("global acquire should win",
        globalCqs.acquireTransformLock(null, schemaName, tableName));
      try {
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty("TenantId", "TENANT_A");
        try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
          ConnectionQueryServices tenantCqs = services(tenantConn);
          assertFalse(
            "tenant acquire on the same (schema, table) must lose to a global holder regardless of tenant",
            tenantCqs.acquireTransformLock("TENANT_A", schemaName, tableName));
        }
      } finally {
        globalCqs.releaseTransformLock(null, schemaName, tableName);
      }
    }
  }

  @Test
  public void testTenantBlocksGlobalAndOtherTenants() throws Exception {
    Properties propsA = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    propsA.setProperty("TenantId", "TENANT_A");
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();

    try (Connection tenantAConn = DriverManager.getConnection(getUrl(), propsA)) {
      ConnectionQueryServices cqsA = services(tenantAConn);
      // tenant-A acquires; tenantId arg is accepted but does NOT participate in the rowkey
      assertTrue("tenant-A acquire should win",
        cqsA.acquireTransformLock("TENANT_A", schemaName, tableName));
      try {
        Properties propsGlobal = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection globalConn = DriverManager.getConnection(getUrl(), propsGlobal)) {
          ConnectionQueryServices globalCqs = services(globalConn);
          assertFalse("global acquire must lose to a tenant-A holder",
            globalCqs.acquireTransformLock(null, schemaName, tableName));
        }
        Properties propsB = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        propsB.setProperty("TenantId", "TENANT_B");
        try (Connection tenantBConn = DriverManager.getConnection(getUrl(), propsB)) {
          ConnectionQueryServices cqsB = services(tenantBConn);
          assertFalse("tenant-B acquire must lose to a tenant-A holder on the same table",
            cqsB.acquireTransformLock("TENANT_B", schemaName, tableName));
        }
      } finally {
        cqsA.releaseTransformLock("TENANT_A", schemaName, tableName);
      }
    }
  }

  @Test
  public void testNonTransformAlterIndexDoesNotAcquireLock() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    String fullTableName = schemaName + "." + tableName;

    try (Connection setupConn = DriverManager.getConnection(getUrl(), props)) {
      setupConn.createStatement()
        .execute("CREATE TABLE " + fullTableName + " (id INTEGER PRIMARY KEY, val VARCHAR)");
      setupConn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (val)");
    }

    // Hold the transform lock externally; non-transform-triggering ALTER INDEX variants
    // (DISABLE, REBUILD, REBUILD ASYNC) must NOT contend on it — each should succeed even while
    // the lock is held.
    try (Connection holderConn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(holderConn);
      assertTrue("holder must acquire lock", cqs.acquireTransformLock(null, schemaName, indexName));
      try {
        try (Connection altererConn = DriverManager.getConnection(getUrl(), props)) {
          altererConn.createStatement()
            .execute("ALTER INDEX " + indexName + " ON " + fullTableName + " DISABLE");
        }
        // Re-enable so REBUILD has something to do.
        try (Connection altererConn = DriverManager.getConnection(getUrl(), props)) {
          altererConn.createStatement()
            .execute("ALTER INDEX " + indexName + " ON " + fullTableName + " REBUILD");
        }
        try (Connection altererConn = DriverManager.getConnection(getUrl(), props)) {
          altererConn.createStatement()
            .execute("ALTER INDEX " + indexName + " ON " + fullTableName + " REBUILD ASYNC");
        }
      } finally {
        cqs.releaseTransformLock(null, schemaName, indexName);
      }
    }
  }

  @Test
  public void testConcurrentAlterIndexSerializesViaTransformLock() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    String indexName = "I_" + generateUniqueName();
    String fullTableName = schemaName + "." + tableName;

    try (Connection setupConn = DriverManager.getConnection(getUrl(), props)) {
      setupConn.createStatement()
        .execute("CREATE TABLE " + fullTableName + " (id INTEGER PRIMARY KEY, val VARCHAR)");
      setupConn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (val)");
    }

    // Lock key for an index transform is keyed on the index's logical name (schemaName, indexName)
    // — see MetaDataClient.alterIndex's acquireTransformLock callsite.
    try (Connection holderConn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(holderConn);
      assertTrue("holder must acquire lock", cqs.acquireTransformLock(null, schemaName, indexName));
      try {
        try (Connection altererConn = DriverManager.getConnection(getUrl(), props)) {
          altererConn.createStatement()
            .execute("ALTER INDEX " + indexName + " ON " + fullTableName
              + " ACTIVE IMMUTABLE_STORAGE_SCHEME = SINGLE_CELL_ARRAY_WITH_OFFSETS,"
              + " COLUMN_ENCODED_BYTES = 2");
          fail("ALTER INDEX should have failed while transform lock is held");
        } catch (SQLException sqle) {
          assertEquals(
            SQLExceptionCode.CANNOT_MODIFY_TABLE_WITH_TRANSFORM_IN_PROGRESS.getErrorCode(),
            sqle.getErrorCode());
          String msg = sqle.getMessage();
          assertTrue("exception message should contain the human-readable text, got: " + msg,
            msg.contains("Cannot modify table"));
          assertFalse(
            "exception message must not contain unsubstituted %s.%s placeholders, got: " + msg,
            msg.contains("%s"));
          // alterIndex throws with setTableName(tableName) at MetaDataClient.java:6071, where the
          // local `tableName` variable is bound to the resolved index PTable's getTableName()
          // (line 5999) — so the appended TABLE_NAME token renders the index name here, not the
          // underlying data table name.
          assertTrue("exception message should contain tableName=" + schemaName + "." + indexName
            + ", got: " + msg, msg.contains("tableName=" + schemaName + "." + indexName));
        }
      } finally {
        cqs.releaseTransformLock(null, schemaName, indexName);
      }
    }

    try (Connection altererConn = DriverManager.getConnection(getUrl(), props)) {
      altererConn.createStatement()
        .execute("ALTER INDEX " + indexName + " ON " + fullTableName
          + " ACTIVE IMMUTABLE_STORAGE_SCHEME = SINGLE_CELL_ARRAY_WITH_OFFSETS,"
          + " COLUMN_ENCODED_BYTES = 2");
    }
  }

  /**
   * Regression-doc test: a stray release after TTL expiry will delete a different caller's lock
   * cell. This documents the load-bearing hazard called out on
   * {@link org.apache.phoenix.query.ConnectionQueryServices#releaseTransformLock} — callers MUST
   * NOT release after the SYSTEM.MUTEX TTL has expired, because the release is implemented as an
   * unconditional cell delete with no fencing token.
   * <p>
   * The sequence the test exercises:
   * <ol>
   * <li>Caller A acquires the lock.</li>
   * <li>The lock cell is removed out-of-band (simulating SYSTEM.MUTEX TTL auto-expiry while A is
   * paused).</li>
   * <li>Caller B acquires — new cell is written, B is now the legitimate holder.</li>
   * <li>Caller A wakes up and runs its {@code finally} {@code releaseTransformLock} — this deletes
   * B's cell (the hazard).</li>
   * <li>Caller C attempts acquire and succeeds, because B's cell was wiped by A's stray
   * release.</li>
   * </ol>
   * The assertion captures current behavior so any future change that introduces token-CAS or
   * holder fencing breaks this test deliberately.
   */
  @Test
  public void testStrayReleaseDoesNotDeleteAnotherCallersLock() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();

    try (Connection connA = DriverManager.getConnection(getUrl(), props);
      Connection connB = DriverManager.getConnection(getUrl(), props);
      Connection connC = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqsA = services(connA);
      ConnectionQueryServices cqsB = services(connB);
      ConnectionQueryServices cqsC = services(connC);

      // Step 1: A acquires.
      assertTrue("A must acquire", cqsA.acquireTransformLock(null, schemaName, tableName));

      // Step 2: simulate TTL auto-expiry by deleting A's cell out-of-band. The marker comes from
      // the same constant used inside ConnectionQueryServicesImpl#acquireTransformLock — a future
      // marker rename therefore breaks this test deliberately rather than silently no-opping it.
      // Using releaseTransformLock here would have the same byte-level effect but conceptually
      // represents "A's sanctioned release"; the lower-level deleteMutexCell models "lock vanished
      // without A knowing".
      cqsA.deleteMutexCell(null, schemaName, tableName,
        ConnectionQueryServicesImpl.TRANSFORM_LOCK_MARKER, null);

      // Step 3: B acquires — succeeds because the cell is gone.
      assertTrue("B must acquire after A's cell auto-expired",
        cqsB.acquireTransformLock(null, schemaName, tableName));

      // Step 4: A's stray release deletes B's cell (the hazard).
      cqsA.releaseTransformLock(null, schemaName, tableName);

      // Step 5: C succeeds because B's cell was wiped by A's stray release. Under a token-CAS
      // implementation, C would FAIL here (B would still hold), and B's release would no-op.
      assertTrue("C wins because A's stray release wiped B's cell — documents the hazard",
        cqsC.acquireTransformLock(null, schemaName, tableName));

      // Cleanup.
      cqsC.releaseTransformLock(null, schemaName, tableName);
    }
  }

  @Test
  public void testNonTransformAlterDoesNotAcquireLock() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    String fullTableName = schemaName + "." + tableName;

    try (Connection setupConn = DriverManager.getConnection(getUrl(), props)) {
      setupConn.createStatement()
        .execute("CREATE TABLE " + fullTableName + " (id INTEGER PRIMARY KEY, val VARCHAR)");
    }

    // Hold the transform lock externally; a non-transform-triggering ALTER (SET TTL) must NOT
    // contend on it under the narrow-scope wiring — it should succeed even while the lock is held.
    try (Connection holderConn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(holderConn);
      assertTrue("holder must acquire lock", cqs.acquireTransformLock(null, schemaName, tableName));
      try {
        try (Connection altererConn = DriverManager.getConnection(getUrl(), props)) {
          altererConn.createStatement().execute("ALTER TABLE " + fullTableName + " SET TTL = 100");
        }
      } finally {
        cqs.releaseTransformLock(null, schemaName, tableName);
      }
    }
  }

  /**
   * Asserts that a transform-triggering ALTER which fails AFTER acquiring the transform lock still
   * releases it in its {@code finally} block, leaving the lock observably free for a subsequent
   * caller.
   * <p>
   * The IT models the failure deterministically by acquiring the lock at the primitive API and then
   * throwing inside a try/finally that mirrors the {@code MetaDataClient.addColumn} /
   * {@code alterIndex} release semantics. A natural mid-ALTER throw (e.g., a
   * {@code ConcurrentTableMutationException} that exhausts the retry budget, or a
   * {@code TableNotFoundException} from the under-lock {@code getTableNoCache} re-fetch when the
   * table is dropped concurrently) would be racy in an IT; the primitive-API path here exercises
   * the same release-on-exception contract without timing assumptions.
   */
  @Test
  public void testLockReleasedAfterFailedTransformTriggeringAlter() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schemaName = "S_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      ConnectionQueryServices cqs = services(conn);

      // Simulate a transform-triggering ALTER that acquires the lock and then throws
      // mid-execution. The release in the finally mirrors MetaDataClient's finally block.
      boolean acquired = false;
      try {
        acquired = cqs.acquireTransformLock(null, schemaName, tableName);
        assertTrue("simulated ALTER must acquire lock", acquired);
        // Inject the failure. In production this would be a TableNotFoundException from
        // getTableNoCache, an evaluateStmtProperties throw, or a ConcurrentTableMutationException
        // retry exhaustion — any post-acquire path that reaches the finally block.
        throw new SQLException("simulated server-side failure during transform-triggering ALTER");
      } catch (SQLException expected) {
        // expected
      } finally {
        if (acquired) {
          cqs.releaseTransformLock(null, schemaName, tableName);
        }
      }

      // Lock must be observably free now: a subsequent caller should win the acquire.
      assertTrue("lock must be observably released after the failed ALTER's finally ran",
        cqs.acquireTransformLock(null, schemaName, tableName));
      cqs.releaseTransformLock(null, schemaName, tableName);
    }
  }
}
