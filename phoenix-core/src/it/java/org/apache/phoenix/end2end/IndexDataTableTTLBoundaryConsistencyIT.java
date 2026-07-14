/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Regression coverage for the production defect from the RCA "Why LEFT_ID & LEFT_ID_SPACE are NULL in
 * ID_MAPPING_V2 data table but non NULL in index table": it reproduces the divergence against pre-fix
 * code and verifies the fix keeps the data table and its index consistent.
 * <p>
 * The schema is {@code IDMAPPER.ID_MAPPING_V2}: PK (organization_id, map_name, map_id),
 * MULTI_TENANT, a NON-FOREVER TTL, and a global covered index on (map_name, left_id, right_id)
 * INCLUDE (map_id_space, right_id_space, left_id_space). The id-mapper client refreshes a row's TTL
 * by issuing a partial self-referential {@code UPSERT SELECT} that names only
 * (organization_id, map_name, map_id, map_id_space) - it does NOT name left_id / left_id_space.
 * <p>
 * Mechanism (all confirmed against the code):
 * <ol>
 *   <li>The initial full row lands with every cell at one timestamp {@code T0}.</li>
 *   <li>The partial touch is READ while the row is still alive (at {@code T0 + TTL - 1}, before the
 *       boundary), but its mutation is committed a couple of ticks later, at
 *       {@code T_commit = T0 + TTL + 1}. The touch names neither left_id nor left_id_space, so as
 *       written it carries only map_id_space + the empty column, both landing at {@code T_commit}.</li>
 *   <li>PRE-FIX (the bug): index maintenance in IndexRegionObserver reads the current data row via an
 *       internal scan that is never TTL-masked, so it sees left_id / left_id_space (still physically
 *       present at {@code T0}) as live, carries them forward, and rebuilds the whole index row at the
 *       single fresh {@code T_commit}. The DATA row, meanwhile, still holds those cells only at
 *       {@code T0}: {@code maxTs - minTs = T_commit - T0 > TTL}, so {@code TTLRegionScanner}'s gap
 *       analysis masks them and a data-side SELECT reads NULL. Index returns left_id / left_id_space,
 *       data reads NULL - the production divergence (data NULL, index non-null).</li>
 *   <li>POST-FIX: the internal current-row read is TTL-masked as of exactly {@code batchTimestamp} -
 *       {@code scan.setTimeRange(0, batchTimestamp)} anchors {@code TTLRegionScanner}'s masking clock
 *       at the very timestamp the index is built at (see {@code IndexRegionObserver.getCurrentRowStates}).
 *       So the index is rebuilt from the same logically-expired-or-alive view the data side sees:
 *       left_id / left_id_space are trimmed-or-retained as a UNIT on both sides. Because this touch
 *       commits one tick PAST the boundary, the anchored read drops them on the index side just as the
 *       data-side read masks them, so data and index both read NULL - in agreement. (Had the touch
 *       landed BEFORE the boundary, or in a dense sub-TTL stream, both sides would instead retain the
 *       value - still in agreement.)</li>
 * </ol>
 * This is a REGRESSION test asserting agreement, robust to whichever route the boundary takes: each
 * test is GREEN when the data table and the index agree on left_id / left_id_space for the same
 * logical row (the fixed behavior), and RED when they diverge (the pre-fix bug: data NULL, index
 * non-null). It deliberately does not pin a specific retain-vs-drop value, so it is deterministic
 * regardless of which side of the boundary the single touch lands on.
 * <p>
 * Agreement is asserted at the QUERY layer (both the data-path and index-path SELECTs read through
 * {@code TTLRegionScanner}), which is the user-visible guarantee. It is NOT asserted with a raw HBase
 * scan: under the production-like non-zero max-lookback this test runs with (see {@code doSetup}), the
 * {@code T0} row version lingers physically inside the time-travel window on the data side even after
 * major compaction, so a raw (unmasked) scan would report physical state that no query ever observes.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexDataTableTTLBoundaryConsistencyIT extends BaseTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IndexDataTableTTLBoundaryConsistencyIT.class);

    private static final String SCHEMA_NAME = "IDMAPPER";

    // Unique per test method (assigned in setUpSchema). Hardcoded names would let one method's
    // future-dated cells (the injected clock commits ~TTL ahead of real wall time) survive the next
    // method's DROP on the shared per-class mini-cluster and pollute its physical table.
    private String tableName;
    private String indexName;

    // Short, NON-FOREVER TTL so the injected clock can cross it deterministically. A FOREVER TTL
    // would short-circuit TTLRegionScanner/CompactionScanner and the bug could not reproduce.
    private static final int TTL_SECONDS = 30;
    private static final long TTL_MS = TTL_SECONDS * 1000L;

    private static final String TENANT_ID = "00Dxx0000001gER"; // VARCHAR(15)
    private static final String MAP_NAME = "MY_MAP";
    private static final String MAP_ID = "uuid:abc";

    private static final String MAP_ID_SPACE = "SPACE_1";      // the touched (named) covered column
    private static final String LEFT_ID = "LEFT_A";            // index key column, distinctive value
    private static final String LEFT_ID_SPACE = "LEFTSPACE_1"; // covered column, distinctive value

    private ManualEnvironmentEdge injectEdge;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        // Run under the production-like config the anchored-masking fix recommends: a non-zero
        // max-lookback set just BELOW the TTL. This is deliberately NOT zero. Two reasons:
        //   1. It matches how indexed TTL tables are actually configured, and it closes the narrow
        //      race where a concurrent major compaction could physically collect a pre-existing cell
        //      between the current-row read and the mutation being persisted.
        //   2. It keeps RowContext.setTTL's effective-TTL floor Math.max(ttlInSecs*1000,
        //      maxLookbackInMillis + 1) equal to TTL_MS: with max-lookback below TTL the floor stays
        //      at TTL_MS, so the injected (TTL + 1) gap still exceeds the effective TTL and the
        //      boundary drop path is exercised. Had max-lookback reached or exceeded TTL the floor
        //      would rise, the gap would no longer exceed it, and the scenario would stop reproducing.
        // Note: under a non-zero max-lookback the T0 row version lingers PHYSICALLY inside the
        // lookback (time-travel) window on the data side even after major compaction. That is
        // expected retention, not divergence: no live query sees it, because every read - data or
        // index - is masked by TTLRegionScanner, which trims the (T0, T_commit] gap identically on
        // both sides. Consistency is therefore asserted at the query layer (masked reads), not by a
        // raw physical scan (which bypasses TTLRegionScanner and would report the unmasked physical
        // state that users never observe).
        Map<String, String> props = new HashMap<>();
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(TTL_SECONDS - 1));
        // Speed up compaction scheduling in the mini-cluster.
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void setUpSchema() throws Exception {
        // DDL runs on the real clock; the manual edge is injected per-scenario, after the schema
        // exists, so it only governs data mutation / scan timestamps.
        EnvironmentEdgeManager.reset();
        // Unique physical tables per method: the injected clock commits cells ~TTL into the future,
        // so a shared hardcoded table would let one method's future-dated cells outlive the next
        // method's DROP (run at real wall time) and bridge its TTL gap, corrupting the scenario.
        tableName = SCHEMA_NAME + "." + generateUniqueName();
        indexName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (\n"
                    + " organization_id VARCHAR(15) NOT NULL,\n"
                    + " map_name VARCHAR(80) NOT NULL,\n"
                    + " map_id_space VARCHAR(255),\n"
                    + " map_id VARCHAR(2047) NOT NULL,\n"
                    + " left_id_space VARCHAR(255),\n"
                    + " left_id VARCHAR(2047),\n"
                    + " right_id_space VARCHAR(255),\n"
                    + " right_id VARCHAR(2047),\n"
                    + " CONSTRAINT PK PRIMARY KEY (organization_id, map_name, map_id)\n"
                    + ") TTL=" + TTL_SECONDS + ", MULTI_TENANT=true, REPLICATION_SCOPE=1");
            stmt.execute("CREATE INDEX " + indexName + "\n"
                    + " ON " + tableName + " (map_name, left_id, right_id)\n"
                    + " INCLUDE (map_id_space, right_id_space, left_id_space)");
        }
        injectEdge = new ManualEnvironmentEdge();
    }

    @After
    public void tearDown() {
        EnvironmentEdgeManager.reset();
    }

    /** Full initial row: PK + map_id_space + non-null left_id / left_id_space; right_* NULL. */
    private void insertInitialRow(Connection conn) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + tableName
                + " (organization_id, map_name, map_id, map_id_space, left_id, left_id_space, "
                + "right_id, right_id_space) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)")) {
            ps.setString(1, TENANT_ID);
            ps.setString(2, MAP_NAME);
            ps.setString(3, MAP_ID);
            ps.setString(4, MAP_ID_SPACE);
            ps.setString(5, LEFT_ID);
            ps.setString(6, LEFT_ID_SPACE);
            ps.executeUpdate();
        }
    }

    /**
     * The exact id-mapper "mappings in use" touch: a self-referential UPSERT SELECT naming only
     * PK + map_id_space. Only {@code executeUpdate()} - the inner SELECT runs and the mutation is
     * buffered here; the caller controls when (and at which timestamp) it commits.
     */
    private void touchWithUpsertSelect(Connection conn) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + tableName
                + " (organization_id, map_name, map_id, map_id_space) "
                + "SELECT organization_id, map_name, map_id, map_id_space FROM " + tableName
                + " WHERE organization_id = ? AND map_name = ? AND map_id = ?")) {
            ps.setString(1, TENANT_ID);
            ps.setString(2, MAP_NAME);
            ps.setString(3, MAP_ID);
            int affected = ps.executeUpdate();
            LOGGER.info("UPSERT SELECT touch affected {} row(s)", affected);
            assertEquals("UPSERT SELECT must read the still-alive row and buffer exactly one touch",
                    1, affected);
        }
    }

    /** Plain partial UPSERT touch naming only PK + map_id_space. Buffered only (no commit here). */
    private void touchWithPlainUpsert(Connection conn) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + tableName
                + " (organization_id, map_name, map_id, map_id_space) VALUES (?, ?, ?, ?)")) {
            ps.setString(1, TENANT_ID);
            ps.setString(2, MAP_NAME);
            ps.setString(3, MAP_ID);
            ps.setString(4, MAP_ID_SPACE);
            ps.executeUpdate();
        }
    }

    /** Resolves the physical HBase name of a Phoenix table/index. */
    private byte[] physicalName(Connection conn, String phoenixName, boolean isIndex)
            throws Exception {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable dataTable = pconn.getTable(new PTableKey(null, tableName));
        if (!isIndex) {
            return dataTable.getPhysicalName().getBytes();
        }
        for (PTable index : dataTable.getIndexes()) {
            if (index.getTableName().getString().equals(phoenixName)) {
                return index.getPhysicalName().getBytes();
            }
        }
        fail("Could not resolve physical name for index " + phoenixName);
        return null; // unreachable
    }

    private void flush(byte[] physicalTable) throws IOException {
        getUtility().getAdmin().flush(TableName.valueOf(physicalTable));
    }

    private void majorCompact(byte[] physicalTable) throws Exception {
        TestUtil.majorCompact(getUtility(), TableName.valueOf(physicalTable));
    }

    /**
     * @param useUpsertSelect when true the touch is the production self-referential UPSERT SELECT;
     *                        when false it is a plain partial UPSERT (a control that drives the
     *                        identical data/index (dis)agreement, isolating the cause as the
     *                        partiality of the write, not the SELECT read side).
     */
    private void runScenario(boolean useUpsertSelect) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);

            // Freeze the clock at T0 and write the full initial row; every cell lands at T0.
            long t0 = EnvironmentEdgeManager.currentTimeMillis();
            injectEdge.setValue(t0);
            EnvironmentEdgeManager.injectEdge(injectEdge);

            insertInitialRow(conn);
            conn.commit();

            byte[] dataPhysical = physicalName(conn, tableName, false);
            byte[] indexPhysical = physicalName(conn, indexName, true);

            // Push the T0 cells onto an HFile so the later major compaction merges two files.
            flush(dataPhysical);
            flush(indexPhysical);

            injectEdge.incrementValue(TTL_MS - 1);
            long tRead = injectEdge.currentTime();
            if (useUpsertSelect) {
                touchWithUpsertSelect(conn);
            } else {
                touchWithPlainUpsert(conn);
            }
            // NOTE: no flush / compaction happens between the touch and its commit, so (for the
            // UPSERT SELECT variant) the inner SELECT read cannot be disturbed mid-statement.

            // --- Advance two ticks (past the boundary) and commit: as written, the touch cells
            // (map_id_space + empty) land at T_commit = T0 + TTL + 1, while left_id / left_id_space
            // stay at T0. The gap on the data row is now (TTL + 1) > TTL. PRE-FIX, gap analysis trims
            // that on the DATA side while the index (rebuilt off an unmasked read at T_commit) keeps
            // it -> divergence. POST-FIX, the internal current-row read is masked as of exactly
            // batchTimestamp (scan.setTimeRange(0, batchTimestamp) anchors TTLRegionScanner's clock at
            // the timestamp the index is built at), so it trims left_id on the INDEX side identically
            // to how a data-side read trims it -> both drop it, in agreement. ---
            injectEdge.incrementValue(2);
            long tCommit = injectEdge.currentTime();
            conn.commit();

            LOGGER.info("t0={} tRead={} tCommit={} gap={}ms ttl={}ms", t0, tRead, tCommit,
                    tCommit - t0, TTL_MS);

            // Major-compact both tables to settle each side's physical state before the reads. Note
            // that under the non-zero max-lookback (see doSetup) the T0 cells are NOT physically
            // purged from the data side - they linger inside the time-travel window - but they remain
            // masked from every query by TTLRegionScanner's gap analysis. Consistency is therefore
            // observed at the query layer: PRE-FIX the masked data SELECT reads NULL while the index
            // (rebuilt off an unmasked internal read at T_commit) still returns the value -> RED;
            // POST-FIX the anchored masked internal read drops it on the index side too -> both NULL.
            flush(dataPhysical);
            flush(indexPhysical);
            majorCompact(dataPhysical);
            majorCompact(indexPhysical);

            // ============ ASSERTIONS: data and index must AGREE (regression guard) ============
            // The bug is a DISAGREEMENT between the data table and its index on left_id /
            // left_id_space for the same logical row. This test pins agreement, NOT a specific
            // retain-vs-drop value, so it is deterministic regardless of which side of the TTL
            // boundary the single touch lands on:
            //   - PRE-FIX  -> data reads NULL (T0 cells gap-trimmed) while the index still carries
            //                 the value rebuilt at T_commit  -> NOT equal -> RED.
            //   - POST-FIX -> both drop it (commit is past the boundary) or both keep it (touch
            //                 before the boundary / dense stream) -> equal -> GREEN.

            // (1) Read left_id / left_id_space from the DATA table (data path).
            String dataLeftId;
            String dataLeftIdSpace;
            String dataMapIdSpace;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT left_id, left_id_space, map_id_space FROM " + tableName
                            + " WHERE organization_id = ? AND map_name = ? AND map_id = ?")) {
                ps.setString(1, TENANT_ID);
                ps.setString(2, MAP_NAME);
                ps.setString(3, MAP_ID);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("DATA row must still exist (map_id_space touch keeps the row alive)",
                            rs.next());
                    dataLeftId = rs.getString("left_id");
                    dataLeftIdSpace = rs.getString("left_id_space");
                    dataMapIdSpace = rs.getString("map_id_space");
                }
            }

            // (2) Read left_id / left_id_space for the same row through the INDEX (forced index).
            String indexLeftId;
            String indexLeftIdSpace;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ left_id, left_id_space "
                            + "FROM " + tableName
                            + " WHERE organization_id = ? AND map_name = ?")) {
                ps.setString(1, TENANT_ID);
                ps.setString(2, MAP_NAME);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue("INDEX query must return the row", rs.next());
                    indexLeftId = rs.getString("left_id");
                    indexLeftIdSpace = rs.getString("left_id_space");
                }
            }

            System.out.println("\n===== TTL-boundary consistency ("
                    + (useUpsertSelect ? "UPSERT SELECT" : "plain UPSERT") + ") =====");
            System.out.println("DATA   left_id=" + dataLeftId + " left_id_space=" + dataLeftIdSpace
                    + " map_id_space=" + dataMapIdSpace);
            System.out.println("INDEX  left_id=" + indexLeftId + " left_id_space=" + indexLeftIdSpace);

            // (3) False-negative guard: the touched column must survive on the data side, proving the
            //     row itself did NOT wholesale-expire (which would make a trivial NULL==NULL pass).
            assertEquals("DATA map_id_space must survive at T_commit (row is alive, not expired)",
                    MAP_ID_SPACE, dataMapIdSpace);

            // (4) Query-layer agreement: data and index must read the SAME value for each column,
            //     whether that value is the retained one or NULL. This is the core regression guard,
            //     and it is asserted at the query layer on purpose. Both the data-path SELECT and the
            //     index-path SELECT read through TTLRegionScanner, so both are masked by the SAME gap
            //     analysis - which is exactly the user-visible consistency guarantee the fix makes.
            //     A raw HBase scan is deliberately NOT used here: under the production-like non-zero
            //     max-lookback (see doSetup), the T0 row version lingers PHYSICALLY on the data side
            //     inside the time-travel window even after major compaction, so a raw (unmasked) scan
            //     would report a physical state that diverges from what every query sees - a false
            //     signal, not a divergence. The query-layer check below is the meaningful assertion.
            assertEquals("left_id must agree between DATA and INDEX (data=" + dataLeftId + " index="
                    + indexLeftId + ")", dataLeftId, indexLeftId);
            assertEquals("left_id_space must agree between DATA and INDEX (data=" + dataLeftIdSpace
                    + " index=" + indexLeftIdSpace + ")", dataLeftIdSpace, indexLeftIdSpace);
        }
    }

    /**
     * The production self-referential UPSERT SELECT touch landing across the TTL boundary. GREEN when
     * the data table and the index agree on left_id / left_id_space (fixed); RED on the pre-fix
     * divergence (data NULL, index non-null).
     */
    @Test
    public void testUpsertSelectTouchNearTtlBoundaryKeepsDataAndIndexConsistent() throws Exception {
        runScenario(true);
    }

    /**
     * Control: a plain partial UPSERT exercises the identical path and must keep the data table and
     * the index consistent, isolating the cause as the partiality of the write, not the SELECT read.
     */
    @Test
    public void testPlainPartialUpsertTouchNearTtlBoundaryKeepsDataAndIndexConsistent()
            throws Exception {
        runScenario(false);
    }
}
