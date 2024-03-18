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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.execute.DescVarLengthFastByteComparisons;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.phoenix.query.QueryConstants.CDC_CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// NOTE: To debug the query execution, add the below condition or the equivalent where you need a
// breakpoint.
//      if (<table>.getTableName().getString().equals("N000002") ||
//                 <table>.getTableName().getString().equals("__CDC__N000002")) {
//          "".isEmpty();
//      }
@RunWith(Parameterized.class)
@Category(ParallelStatsDisabledTest.class)
public class CDCQueryIT extends CDCBaseIT {
    // Offset of the first column, depending on whether PHOENIX_ROW_TIMESTAMP() is in the schema
    // or not.
    private final boolean forView;
    private final boolean dataBeforeCDC;
    private final PTable.QualifierEncodingScheme encodingScheme;
    private final boolean multitenant;
    private final Integer indexSaltBuckets;
    private final Integer tableSaltBuckets;
    private final boolean withSchemaName;
    private ManualEnvironmentEdge injectEdge;
    private Gson gson = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();
    private Calendar cal = Calendar.getInstance();

    private class ChangeRow {
        private final String tenantid;
        private final long changeTS;
        private final Map<String, Object> pks;

        public String getTenantID() {
            return tenantid;
        }

        public Map<String, Object> getPreImage() {
            return (Map<String, Object>) change.get(CDC_PRE_IMAGE);
        }

        public Map<String, Object> getChangeImage() {
            return (Map<String, Object>) change.get(CDC_CHANGE_IMAGE);
        }

        public Map<String, Object> getPostImage() {
            return (Map<String, Object>) change.get(CDC_POST_IMAGE);
        }

        private final Map<String, Object> change;

        ChangeRow(String tenantid, long changeTS, Map<String, Object> pks, Map<String, Object> change) {
            this.tenantid = tenantid;
            this.changeTS = changeTS;
            this.pks = pks;
            this.change = change;
        }

        public String toString() {
            return gson.toJson(this);
        }

        public Map<String, Object> getPrimaryKeys() {
            return pks;
        }

        public long getChangeTimestamp() {
            return changeTS;
        }
    }

    public CDCQueryIT(Boolean forView, Boolean dataBeforeCDC,
                      PTable.QualifierEncodingScheme encodingScheme, boolean multitenant,
                      Integer indexSaltBuckets, Integer tableSaltBuckets, boolean withSchemaName) {
        this.forView = forView;
        this.dataBeforeCDC = dataBeforeCDC;
        this.encodingScheme = encodingScheme;
        this.multitenant = multitenant;
        this.indexSaltBuckets = indexSaltBuckets;
        this.tableSaltBuckets = tableSaltBuckets;
        this.withSchemaName = withSchemaName;
    }

    @Parameterized.Parameters(name = "forView={0} dataBeforeCDC={1}, encodingScheme={2}, " +
            "multitenant={3}, indexSaltBuckets={4}, tableSaltBuckets={5} withSchemaName=${6}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.FALSE, Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, null,
                        Boolean.FALSE },
                { Boolean.FALSE, Boolean.TRUE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, null,
                        Boolean.TRUE },
                { Boolean.FALSE, Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.FALSE, 1, 1,
                        Boolean.FALSE },
                // Once PHOENIX-7239, change this to have different salt buckets for data and index.
                { Boolean.FALSE, Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.TRUE, 1, 1,
                        Boolean.TRUE },
                { Boolean.FALSE, Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.FALSE, 4, null,
                        Boolean.FALSE },
                { Boolean.TRUE, Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, null,
                        Boolean.FALSE },
        });
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    private Map<String, Object> addChange(Connection conn, Map preImage,
                                          Set<PTable.CDCChangeScope> changeScopes,
                                          long changeTS, String changeType, String tableName,
                                          Map<String, Object> pks, Map<String, Object> values)
                                          throws SQLException {
        if (conn != null) {
            String sql;
            if (changeType == CDC_DELETE_EVENT_TYPE) {
                String predicates = pks.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).
                        collect(Collectors.joining(", "));
                sql = "DELETE FROM " + tableName + " WHERE " + predicates;
            }
            else {
                String columnList = Stream.concat(pks.keySet().stream(),
                        values.keySet().stream()).collect(Collectors.joining(", "));
                String valueList =
                        Stream.concat(pks.values().stream(), values.values().stream())
                                .map(v -> String.valueOf(v)).collect(Collectors.joining(", "));
                sql = "UPSERT INTO " + tableName + " (" + columnList + ") VALUES (" + valueList + ")";
            }
            cal.setTimeInMillis(changeTS);
            injectEdge.setValue(changeTS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
        Map<String, Object> cdcChange = new HashMap<>();
        cdcChange.put(CDC_EVENT_TYPE, changeType);
        if (changeScopes.contains(PTable.CDCChangeScope.PRE)) {
            cdcChange.put(CDC_PRE_IMAGE, preImage);
        }
        if (changeType == CDC_UPSERT_EVENT_TYPE) {
            Map<String, Object> changeImage = new HashMap<>();
            changeImage.putAll(values);
            if (changeScopes.contains(PTable.CDCChangeScope.CHANGE)) {
                cdcChange.put(CDC_CHANGE_IMAGE, changeImage);
            }
            if (changeScopes.contains(PTable.CDCChangeScope.POST)) {
                Map<String, Object> postImage = new HashMap<>();
                postImage.putAll(preImage);
                postImage.putAll(changeImage);
                cdcChange.put(CDC_POST_IMAGE, postImage);
            }
        }
        return cdcChange;
    }

    // FIXME: Add the following with consecutive upserts on the sake PK (no delete in between):
    //  - with different values
    //  - with a null
    //  - missing columns
    private List<ChangeRow> generateChanges(long startTS, String[] tenantids, String tableName,
                                            String datatableNameForDDL,
                                            Set<PTable.CDCChangeScope> changeScopes,
                                            CommitAdapter committer)
                            throws Exception {
        List<ChangeRow> changes = new ArrayList<>();
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(startTS);
        boolean dropV3Done = false;
        committer.init();
        Map<String, Object> pk1 = new HashMap() {{ put("K", 1); }};
        Map<String, Object> pk2 = new HashMap() {{ put("K", 2); }};
        Map<String, Object> pk3 = new HashMap() {{ put("K", 3); }};
        Map<String, Object> c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12;
        for (String tid: tenantids) {
            try (Connection conn = committer.getConnection(tid)) {
                c1 = addChange(conn, new HashMap(), changeScopes, startTS,
                        CDC_UPSERT_EVENT_TYPE, tableName, pk1, new TreeMap<String, Object>() {{
                            put("V1", 100L);
                            put("V2", 1000L);
                            put("B.VB", 10000L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c1));
                c2 = addChange(conn, new HashMap(), changeScopes, startTS,
                        CDC_UPSERT_EVENT_TYPE, tableName, pk2, new TreeMap<String, Object>() {{
                            put("V1", 200L);
                            put("V2", 2000L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk2, c2));
                committer.commit(conn);

                c3 = addChange(conn, new HashMap(), changeScopes, startTS +=100,
                        CDC_UPSERT_EVENT_TYPE,
                        tableName, pk3, new TreeMap<String, Object>() {{
                            put("V1", 300L);
                            put("V2", null);
                            put("B.VB", null);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk3, c3));
                committer.commit(conn);

                c4 = addChange(conn, (Map) c1.get(CDC_POST_IMAGE),
                        changeScopes, startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 101L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c4));
                committer.commit(conn);
            }
            if (datatableNameForDDL != null && !dropV3Done) {
                try (Connection conn = newConnection()) {
                    conn.createStatement().execute("ALTER TABLE " + datatableNameForDDL +
                            " DROP COLUMN v3");
                }
                injectEdge.incrementValue(100);
                dropV3Done = true;
            }
            try (Connection conn = newConnection(tid)) {
                c5 = addChange(conn, (Map) c4.get(CDC_POST_IMAGE), changeScopes, startTS +=100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                changes.add(new ChangeRow(tid, startTS, pk1, c5));
                committer.commit(conn);

                c6 = addChange(conn, new HashMap(),
                        changeScopes, startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 102L);
                            put("V2", 1002L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c6));
                committer.commit(conn);

                c7 = addChange(conn, (Map) c6.get(CDC_POST_IMAGE), changeScopes, startTS +=100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                changes.add(new ChangeRow(tid, startTS, pk1, c7));
                committer.commit(conn);

                c8 = addChange(conn, (Map) c2.get(CDC_POST_IMAGE),
                        changeScopes, startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk2,
                        new TreeMap<String, Object>() {{
                            put("V1", 201L);
                            put("V2", null);
                            put("B.VB", 20001L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk2, c8));
                committer.commit(conn);

                c9 = addChange(conn, new HashMap(),
                        changeScopes, startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 103L);
                            put("V2", 1003L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c9));
                committer.commit(conn);

                c10 = addChange(conn, (Map) c9.get(CDC_POST_IMAGE), changeScopes, startTS +=100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                changes.add(new ChangeRow(tid, startTS, pk1, c10));
                committer.commit(conn);

                c11 = addChange(conn, new HashMap(),
                        changeScopes, startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 104L);
                            put("V2", 1004L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c11));
                committer.commit(conn);

                c12 = addChange(conn, (Map) c11.get(CDC_POST_IMAGE), changeScopes, startTS +=100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                changes.add(new ChangeRow(tid, startTS, pk1, c12));
                committer.commit(conn);
            }
        }
        committer.reset();
        return changes;
    }

    private void verifyChanges(String tenantId, ResultSet rs, List<ChangeRow> changes,
                               boolean mutableTable) throws Exception {
        for (int i = 0, changenr = 0; i < changes.size(); ++i) {
            ChangeRow change = changes.get(i);
            if (change.getTenantID() != tenantId) {
                continue;
            }
            String changeDesc = "Chang " + (changenr+1) + ": " + change;
            assertTrue(changeDesc, rs.next());
            Map cdcObj = gson.fromJson(rs.getString(3), HashMap.class);
            // This is needed because for immutable tables, CDC can't distinguish a null value from
            //  that of a a missing cell.
            if (!mutableTable && changeDesc != null) {
                _purgeNulls(change.getPreImage());
                _purgeNulls(change.getChangeImage());
                _purgeNulls(change.getPostImage());
            }
            assertEquals(changeDesc, change.getChangeTimestamp(),
                    rs.getDate(1).getTime());
            for (Map.Entry<String, Object> pk: change.getPrimaryKeys().entrySet()) {
                assertEquals(changeDesc, pk.getValue(), rs.getObject(pk.getKey()));
            }
            assertEquals(changeDesc, change.change, cdcObj);
            ++changenr;
        }
        assertFalse(rs.next());
    }

    private Object _ifIntConvertToLong(Object val) {
        return (val instanceof Integer) ? new Long(((Integer) val).intValue()) : val;
    }

    private void _purgeNulls(Map image) {
        if (image == null) {
            return;
        }
        for (Iterator<Map.Entry> it = image.entrySet().iterator(); it.hasNext(); ) {
            if (it.next().getValue() == null) {
                it.remove();
            }
        }
    }

    @Test
    public void testSelectCDC() throws Exception {
        String cdcName, cdc_sql;
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String datatableName = tableName;
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " ("
                    + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
                    + "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, B.vb INTEGER, "
                    + "CONSTRAINT PK PRIMARY KEY " + (multitenant ? "(TENANT_ID, k) " : "(k)")
                    + ")", encodingScheme, multitenant, tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        List<ChangeRow> changes = generateChanges(startTS, tenantids, tableName, null,
                new HashSet<>(Arrays.asList(PTable.CDCChangeScope.CHANGE)), COMMIT_SUCCESS);

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
            // Testing with flushed data adds more coverage.
            getUtility().getAdmin().flush(TableName.valueOf(datatableName));
            getUtility().getAdmin().flush(TableName.valueOf(SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName))));
        }

        //SingleCellIndexIT.dumpTable(tableName);
        //SingleCellIndexIT.dumpTable(CDCUtil.getCDCIndexName(cdcName));

        String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
        try (Connection conn = newConnection(tenantId)) {

            // Existence of CDC shouldn't cause the regular query path to fail.
            String uncovered_sql = "SELECT " + " /*+ INDEX(" + tableName + " " +
                    CDCUtil.getCDCIndexName(cdcName) + ") */ k, v1 FROM " + tableName;
            try (ResultSet rs = conn.createStatement().executeQuery(uncovered_sql)) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals(300, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(201, rs.getInt(2));
                assertFalse(rs.next());
            }

            verifyChanges(tenantId, conn.createStatement().executeQuery(
                    "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + cdcFullName), changes, true);
            verifyChanges(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(CHANGE) */ PHOENIX_ROW_TIMESTAMP(), K," +
                                    "\"CDC JSON\" FROM " + cdcFullName), changes, true);
            changes = generateChanges(startTS, tenantids, tableName, null,
                    new HashSet<PTable.CDCChangeScope>(
                            Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)),
                    COMMIT_NOOP);
            verifyChanges(tenantId, conn.createStatement().executeQuery(
                    "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName),
                    changes, true);
            changes = generateChanges(startTS, tenantids, tableName, null,
                    new HashSet<>(), COMMIT_NOOP);
            verifyChanges(tenantId, conn.createStatement().executeQuery("SELECT * FROM " + cdcFullName),
                    changes, true);

            HashMap<String, int[]> testQueries = new HashMap<String, int[]>() {{
                put("SELECT 'dummy', k, \"CDC JSON\" FROM " + cdcFullName,
                        new int[]{1, 2, 3, 1, 1, 1, 1, 2, 1, 1, 1, 1});
                put("SELECT PHOENIX_ROW_TIMESTAMP(), k, \"CDC JSON\" FROM " + cdcFullName +
                        " ORDER BY k ASC", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3});
                put("SELECT PHOENIX_ROW_TIMESTAMP(), k, \"CDC JSON\" FROM " + cdcFullName +
                        " ORDER BY k DESC", new int[]{3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1});
                put("SELECT PHOENIX_ROW_TIMESTAMP(), k, \"CDC JSON\" FROM " + cdcFullName +
                        " ORDER BY PHOENIX_ROW_TIMESTAMP() DESC",
                        new int[]{1, 1, 1, 1, 2, 1, 1, 1, 1, 3, 2, 1});
            }};
            Map dummyChange = new HashMap() {{
                put(CDC_EVENT_TYPE, "dummy");
            }};
            for (Map.Entry<String, int[]> testQuery : testQueries.entrySet()) {
                try (ResultSet rs = conn.createStatement().executeQuery(testQuery.getKey())) {
                    for (int i = 0; i < testQuery.getValue().length; ++i) {
                        int k = testQuery.getValue()[i];
                        assertEquals(true, rs.next());
                        assertEquals("Index: " + i + " for query: " + testQuery.getKey(),
                                k, rs.getInt(2));
                        Map change = gson.fromJson(rs.getString(3), HashMap.class);
                        change.put(CDC_EVENT_TYPE, "dummy");
                        // Verify that we are getting nothing but the event type as we specified
                        // no change scopes.
                        assertEquals(dummyChange, change);
                    }
                    assertEquals(false, rs.next());
                }
            }
        }
    }

    private List<ChangeRow> generateChangesImmutableTable(long startTS, String[] tenantids,
                                                          String tableName,
                                                          Set<PTable.CDCChangeScope> changeScopes,
                                                          CommitAdapter committer)
            throws Exception {
        List<ChangeRow> changes = new ArrayList<>();
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(startTS);
        committer.init();
        Map<String, Object> pk1 = new HashMap() {{ put("K", 1); }};
        Map<String, Object> pk2 = new HashMap() {{ put("K", 2); }};
        Map<String, Object> pk3 = new HashMap() {{ put("K", 3); }};
        Map<String, Object> c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12;
        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                c1 = addChange(conn, new HashMap(), changeScopes, startTS,
                        CDC_UPSERT_EVENT_TYPE, tableName, pk1, new TreeMap<String, Object>() {{
                            put("V1", 100L);
                            put("V2", 1000L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c1));
                c2 = addChange(conn, new HashMap(), changeScopes, startTS += 100,
                        CDC_UPSERT_EVENT_TYPE, tableName, pk2, new TreeMap<String, Object>() {{
                            put("V1", 200L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk2, c2));
                c3 = addChange(conn, new HashMap(), changeScopes, startTS += 100,
                        CDC_UPSERT_EVENT_TYPE,
                        tableName, pk3, new TreeMap<String, Object>() {{
                            put("V1", 300L);
                            put("V2", null);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk3, c3));
                c4 = addChange(conn, (Map) c1.get(CDC_POST_IMAGE), changeScopes, startTS += 100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c4));
                c5 = addChange(conn, new HashMap(),
                        changeScopes, startTS += 100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 102L);
                            put("V2", 1002L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c5));
                c6 = addChange(conn, (Map) c5.get(CDC_POST_IMAGE), changeScopes, startTS += 100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c6));
                c7 = addChange(conn, new HashMap(),
                        changeScopes, startTS += 100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 103L);
                            put("V2", 1003L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c7));
                c8 = addChange(conn, (Map) c7.get(CDC_POST_IMAGE), changeScopes, startTS += 100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c8));
                c9 = addChange(conn, new HashMap(),
                        changeScopes, startTS += 100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 104L);
                            put("V2", 1004L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c9));
                c10 = addChange(conn, (Map) c9.get(CDC_POST_IMAGE), changeScopes, startTS += 100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c10));
            }
        }
        committer.reset();
        return changes;
    }

    private void _testSelectCDCImmutable(PTable.ImmutableStorageScheme immutableStorageScheme)
            throws Exception {
        String cdcName, cdc_sql;
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String datatableName = tableName;
        try (Connection conn = newConnection()) {
           createTable(conn, "CREATE TABLE  " + tableName + " (" +
                            (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                            "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                            (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, true, immutableStorageScheme);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        List<ChangeRow> changes = generateChangesImmutableTable(startTS, tenantids, tableName,
                new HashSet<PTable.CDCChangeScope>(
                        Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)),
                COMMIT_SUCCESS);

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
            // Testing with flushed data adds more coverage.
            getUtility().getAdmin().flush(TableName.valueOf(datatableName));
            getUtility().getAdmin().flush(TableName.valueOf(SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName))));
        }

        String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
        try (Connection conn = newConnection(tenantId)) {
            // For debug: uncomment to see the exact results logged to console.
            //try (Statement stmt = conn.createStatement()) {
            //    try (ResultSet rs = stmt.executeQuery(
            //            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ PHOENIX_ROW_TIMESTAMP(), K," +
            //                    "\"CDC JSON\" FROM " + cdcFullName)) {
            //        while (rs.next()) {
            //            System.out.println("----- " + rs.getString(1) + " " +
            //                    rs.getInt(2) + " " + rs.getString(3));
            //        }
            //    }
            //}
            verifyChanges(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName),
                            changes, false);
            changes = generateChangesImmutableTable(startTS, tenantids, tableName,
                    new HashSet<>(Arrays.asList(PTable.CDCChangeScope.CHANGE)), COMMIT_NOOP);
            verifyChanges(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + cdcFullName), changes,
                    false);
            verifyChanges(tenantId, conn.createStatement().executeQuery("SELECT /*+ CDC_INCLUDE(CHANGE) */ " +
                    "PHOENIX_ROW_TIMESTAMP(), K, \"CDC JSON\" FROM " + cdcFullName),
                    changes, false);
        }
    }

    @Test
    public void testSelectCDCImmutableOneCellPerColumn() throws Exception {
        _testSelectCDCImmutable(PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
    }

    @Test
    public void testSelectCDCImmutableSingleCell() throws Exception {
        _testSelectCDCImmutable(PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS);
    }

    @Test
    public void testSelectTimeRangeQueries() throws Exception {
        String cdcName, cdc_sql;
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, v1 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
        }

        EnvironmentEdgeManager.injectEdge(injectEdge);

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        Timestamp ts1 = new Timestamp(System.currentTimeMillis());
        cal.setTimeInMillis(ts1.getTime());
        injectEdge.setValue(ts1.getTime());

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 100)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (2, 200)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 200);
        Timestamp ts2 = new Timestamp(cal.getTime().getTime());
        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (3, 300)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 200 + 100 * tenantids.length);
        Timestamp ts3 = new Timestamp(cal.getTime().getTime());
        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k = 2");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 200 + 100 * tenantids.length);
        Timestamp ts4 = new Timestamp(cal.getTime().getTime());
        EnvironmentEdgeManager.reset();

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
        }

        //SingleCellIndexIT.dumpTable(CDCUtil.getCDCIndexName(cdcName));

        String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
        try (Connection conn = newConnection(tenantId)) {
            String sel_sql =
                    "SELECT to_char(phoenix_row_timestamp()), k, \"CDC JSON\" FROM " + cdcFullName +
                            " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?";
            Object[] testDataSets = new Object[] {
                    new Object[] {ts1, ts2, new int[] {1, 2}},
                    new Object[] {ts2, ts3, new int[] {1, 3}},
                    new Object[] {ts3, ts4, new int[] {1, 2}},
                    new Object[] {ts1, ts4, new int[] {1, 2, 1, 3, 1, 2}},
            };
            PreparedStatement stmt = conn.prepareStatement(sel_sql);
            // For debug: uncomment to see the exact results logged to console.
            //System.out.println("----- ts1: " + ts1 + " ts2: " + ts2 + " ts3: " + ts3 + " ts4: " +
            //        ts4);
            //for (int i = 0; i < testDataSets.length; ++i) {
            //    Object[] testData = (Object[]) testDataSets[i];
            //    stmt.setTimestamp(1, (Timestamp) testData[0]);
            //    stmt.setTimestamp(2, (Timestamp) testData[1]);
            //    try (ResultSet rs = stmt.executeQuery()) {
            //        System.out.println("----- Test data set: " + i);
            //        while (rs.next()) {
            //            System.out.println("----- " + rs.getString(1) + " " +
            //                    rs.getInt(2) + " "  + rs.getString(3));
            //        }
            //    }
            //}
            for (int i = 0; i < testDataSets.length; ++i) {
                Object[] testData = (Object[]) testDataSets[i];
                stmt.setTimestamp(1, (Timestamp) testData[0]);
                stmt.setTimestamp(2, (Timestamp) testData[1]);
                try (ResultSet rs = stmt.executeQuery()) {
                    for (int j = 0; j < ((int[]) testData[2]).length; ++j) {
                        int k = ((int[]) testData[2])[j];
                        assertEquals(" Index: " + j + " Test data set: " + i,
                                true, rs.next());
                        assertEquals(" Index: " + j + " Test data set: " + i,
                                k, rs.getInt(2));
                    }
                    assertEquals("Test data set: " + i, false, rs.next());
                }
            }

            PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT * FROM " + cdcFullName + " WHERE PHOENIX_ROW_TIMESTAMP() > ?");
            pstmt.setTimestamp(1, ts4);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertEquals(false, rs.next());
            }
        }
    }

    @Test
    public void testSelectCDCWithDDL() throws Exception {
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String datatableName = tableName;
        String cdcName, cdc_sql;
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, v0 INTEGER, v1 INTEGER, v1v2 INTEGER, v2 INTEGER, B.vb INTEGER, " +
                    "v3 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }

            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
            conn.createStatement().execute("ALTER TABLE " + datatableName + " DROP COLUMN v0");
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        List<ChangeRow> changes = generateChanges(startTS, tenantids, tableName, datatableName,
                new HashSet<>(Arrays.asList(PTable.CDCChangeScope.CHANGE)), COMMIT_SUCCESS);

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
            // Testing with flushed data adds more coverage.
            getUtility().getAdmin().flush(TableName.valueOf(datatableName));
            getUtility().getAdmin().flush(TableName.valueOf(SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName))));
        }

        try (Connection conn = newConnection(tenantId)) {
            verifyChanges(tenantId, conn.createStatement().executeQuery(
                    "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + SchemaUtil.getTableName(
                            schemaName, cdcName)),
                    changes, true);
        }
    }

    private void assertCDCBinaryAndDateColumn(ResultSet rs,
                                              List<byte []> byteColumnValues,
                                              List<Date> dateColumnValues,
                                              Timestamp timestamp) throws Exception {
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));

        Map<String, Object> row1 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        Map<String, Object> postImage = new HashMap<>();
        postImage.put("A_BINARY",
                Base64.getEncoder().encodeToString(byteColumnValues.get(0)));
        postImage.put("D", dateColumnValues.get(0).toString());
        postImage.put("T", timestamp.toString());
        row1.put(CDC_POST_IMAGE, postImage);
        Map<String, Object> changeImage = new HashMap<>();
        changeImage.put("A_BINARY",
                Base64.getEncoder().encodeToString(byteColumnValues.get(0)));
        changeImage.put("D", dateColumnValues.get(0).toString());
        changeImage.put("T", timestamp.toString());
        row1.put(CDC_CHANGE_IMAGE, changeImage);
        row1.put(CDC_PRE_IMAGE, new HashMap<String, String>() {{
        }});
        assertEquals(row1, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        HashMap<String, Object> row2Json = gson.fromJson(rs.getString(3),
                HashMap.class);
        String row2BinaryColStr = (String) ((Map)((Map)row2Json.get(CDC_CHANGE_IMAGE))).get("A_BINARY");
        byte[] row2BinaryCol = Base64.getDecoder().decode(row2BinaryColStr);

        assertEquals(0, DescVarLengthFastByteComparisons.compareTo(byteColumnValues.get(1),
                0, byteColumnValues.get(1).length, row2BinaryCol, 0, row2BinaryCol.length));
    }

    @Test
    public void testCDCBinaryAndDateColumn() throws Exception {
        List<byte []> byteColumnValues = new ArrayList<>();
        byteColumnValues.add( new byte[] {0,0,0,0,0,0,0,0,0,1});
        byteColumnValues.add(new byte[] {0,0,0,0,0,0,0,0,0,2});
        List<Date> dateColumnValues = new ArrayList<>();
        dateColumnValues.add(Date.valueOf("2024-02-01"));
        dateColumnValues.add(Date.valueOf("2024-01-31"));
        Timestamp timestampColumnValue = Timestamp.valueOf("2024-01-31 12:12:14");
        String cdcName, cdc_sql;
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, a_binary binary(10), d Date, t TIMESTAMP, " +
                    "CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
        }

        String tenantId = multitenant ? "1000" : null;
        try (Connection conn = newConnection(tenantId)) {
            String upsertQuery = "UPSERT INTO " + tableName + " (k, a_binary, d, t) VALUES (?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsertQuery);
            stmt.setInt(1, 1);
            stmt.setBytes(2, byteColumnValues.get(0));
            stmt.setDate(3, dateColumnValues.get(0));
            stmt.setTimestamp(4, timestampColumnValue);
            stmt.execute();
            conn.commit();
            stmt.setInt(1, 2);
            stmt.setBytes(2, byteColumnValues.get(1));
            stmt.setDate(3, dateColumnValues.get(1));
            stmt.setTimestamp(4, timestampColumnValue);
            stmt.execute();
            conn.commit();
        }

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets, null);
            }
        }

        try (Connection conn = newConnection(tenantId)) {
            assertCDCBinaryAndDateColumn(conn.createStatement().executeQuery
                    ("SELECT /*+ CDC_INCLUDE(PRE, POST, CHANGE) */ * FROM " +
                    SchemaUtil.getTableName(schemaName, cdcName)),
                    byteColumnValues, dateColumnValues, timestampColumnValue);
        }
    }

    @Test
    public void testSelectCDCFailDataTableUpdate() throws Exception {
        if (dataBeforeCDC == true) {
            // In this case, index will not exist at the time of upsert, so we can't simulate the
            // index failure.
            return;
        }
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String cdcName, cdc_sql;
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                            (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                            "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, B.vb INTEGER, " +
                            "CONSTRAINT PK PRIMARY KEY " +
                            (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, indexSaltBuckets, null);
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        generateChanges(startTS, tenantids, tableName, null, new HashSet<>(),
                COMMIT_FAILURE_EXPECTED);

        try (Connection conn = newConnection(tenantId)) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " +
                    SchemaUtil.getTableName(schemaName, cdcName));
            assertEquals(false, rs.next());
        }
    }

    private abstract class CommitAdapter {
        abstract void commit(Connection conn) throws SQLException;

        void init() {
            EnvironmentEdgeManager.injectEdge(injectEdge);
        }

        public void reset() {
            EnvironmentEdgeManager.reset();
        }

        public Connection getConnection(String tid) throws SQLException {
            return newConnection(tid);
        }
    }

    private final CommitAdapter COMMIT_SUCCESS = new CommitAdapter() {
        @Override
        public void commit(Connection conn) throws SQLException {
            conn.commit();
        }
    };

    private final CommitAdapter COMMIT_NOOP = new CommitAdapter() {
        void init() {
            // Do nothing.
        }

        public void reset() {
            // Do nothing.
        }

        @Override
        public void commit(Connection conn) throws SQLException {
            // Do nothing.
        }

        public Connection getConnection(String tid) throws SQLException {
            return null;
        }
    };

    private final CommitAdapter COMMIT_FAILURE_EXPECTED = new CommitAdapter() {
        @Override
        public void commit(Connection conn) throws SQLException {
            try {
                conn.commit();
                // It is config issue commit didn't fail.
                fail("Commit expected to fail");
            } catch (SQLException e) {
                // this is expected
            }
        }

        @Override
        void init() {
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
        }

        @Override
        public void reset() {
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
        }
    };
}
