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
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import static org.apache.phoenix.util.MetaDataUtil.getViewIndexPhysicalName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CDCBaseIT extends ParallelStatsDisabledIT {
    static final HashSet<PTable.CDCChangeScope> CHANGE_IMG =
            new HashSet<>(Arrays.asList(PTable.CDCChangeScope.CHANGE));
    static final HashSet<PTable.CDCChangeScope> PRE_POST_IMG = new HashSet<>(
            Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST));

    protected ManualEnvironmentEdge injectEdge;
    protected Gson gson = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();
    protected Calendar cal = Calendar.getInstance();

    protected void createTable(Connection conn, String table_sql)
            throws Exception {
        createTable(conn, table_sql, null, false, null, false, null);
    }

    protected void createTable(Connection conn, String table_sql,
                               PTable.QualifierEncodingScheme encodingScheme)
            throws Exception {
        createTable(conn, table_sql, encodingScheme, false, null, false, null);
    }

    protected void createTable(Connection conn, String table_sql,
                               PTable.QualifierEncodingScheme encodingScheme, boolean multitenant)
            throws Exception {
        createTable(conn, table_sql, encodingScheme, multitenant, null, false, null);
    }

    protected void createTable(Connection conn, String table_sql,
                               PTable.QualifierEncodingScheme encodingScheme, boolean multitenant,
                               Integer nSaltBuckets, boolean immutable, PTable.ImmutableStorageScheme immutableStorageScheme)
            throws Exception {
        createTable(conn, table_sql, encodingScheme, multitenant, nSaltBuckets, null, immutable, immutableStorageScheme);
    }

    protected void createTable(Connection conn, String table_sql,
                               PTable.QualifierEncodingScheme encodingScheme, boolean multitenant,
                               Integer nSaltBuckets, PTable.IndexType indexType, boolean immutable,
                               PTable.ImmutableStorageScheme immutableStorageScheme)
            throws Exception {
        createTable(conn, table_sql, new HashMap<String, Object>() {{
            put(TableProperty.COLUMN_ENCODED_BYTES.getPropertyName(), encodingScheme != null ?
                    new Byte(encodingScheme.getSerializedMetadataValue()) : null);
            put(TableProperty.MULTI_TENANT.getPropertyName(), multitenant);
            put(TableProperty.SALT_BUCKETS.getPropertyName(), nSaltBuckets);
            put(TableProperty.INDEX_TYPE.getPropertyName(), indexType);
            put(TableProperty.IMMUTABLE_ROWS.getPropertyName(), immutable);
            put(TableProperty.IMMUTABLE_STORAGE_SCHEME.getPropertyName(), immutableStorageScheme != null ?
                    immutableStorageScheme.name() : null);
        }});
    }

    protected void createTable(Connection conn, String table_sql,
                               Map<String,Object> tableProps) throws Exception {
        List<String> props = new ArrayList<>();
        Byte encodingScheme = (Byte) TableProperty.COLUMN_ENCODED_BYTES.getValue(tableProps);
        if (encodingScheme != null && encodingScheme !=
                QueryServicesOptions.DEFAULT_COLUMN_ENCODED_BYTES) {
            props.add(TableProperty.COLUMN_ENCODED_BYTES.getPropertyName() + "=" + encodingScheme);
        }
        Boolean multitenant = (Boolean) TableProperty.MULTI_TENANT.getValue(tableProps);
        if (multitenant != null && multitenant) {
            props.add(TableProperty.MULTI_TENANT.getPropertyName() + "=" + multitenant);
        }
        Integer nSaltBuckets = (Integer) TableProperty.SALT_BUCKETS.getValue(tableProps);
        if (nSaltBuckets != null) {
            props.add(TableProperty.SALT_BUCKETS.getPropertyName() + "=" + nSaltBuckets);
        }
        PTable.IndexType indexType = (PTable.IndexType) TableProperty.INDEX_TYPE.getValue(
                tableProps);
        if (indexType != null && indexType == PTable.IndexType.LOCAL) {
            props.add(TableProperty.INDEX_TYPE.getPropertyName() + "=" +
                    (indexType == PTable.IndexType.LOCAL ? "l" : "g"));
        }
        if (nSaltBuckets != null) {
            props.add(TableProperty.INDEX_TYPE.getPropertyName() + "=" + indexType);
        }
        Boolean immutableTable = (Boolean) TableProperty.IMMUTABLE_ROWS.getValue(tableProps);
        if (immutableTable) {
            props.add(TableProperty.IMMUTABLE_ROWS.getPropertyName() + "=true");
        }
        PTable.ImmutableStorageScheme immutableStorageScheme =
                (PTable.ImmutableStorageScheme) TableProperty
                        .IMMUTABLE_STORAGE_SCHEME.getValue(tableProps);
        if (immutableStorageScheme != null) {
            props.add(TableProperty.IMMUTABLE_STORAGE_SCHEME.getPropertyName() + "="
                    + immutableStorageScheme.name());
        }
        table_sql += " " + String.join(", ", props);
        conn.createStatement().execute(table_sql);
    }

    protected void createCDCAndWait(Connection conn, String tableName, String cdcName,
                                    String cdc_sql) throws Exception {
        createCDCAndWait(conn, tableName, cdcName, cdc_sql, null, null, null);
    }

    protected void createCDCAndWait(Connection conn, String tableName, String cdcName,
                                  String cdc_sql, PTable.IndexType indexType) throws Exception{
        createCDCAndWait(conn, tableName, cdcName, cdc_sql, null, null, indexType);
    }

    protected void createCDCAndWait(Connection conn, String tableName, String cdcName,
                                    String cdc_sql, PTable.QualifierEncodingScheme encodingScheme,
                                    Integer nSaltBuckets) throws Exception {
        createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, nSaltBuckets, null);
    }

    protected void createCDCAndWait(Connection conn, String tableName, String cdcName,
                                    String cdc_sql, PTable.QualifierEncodingScheme encodingScheme,
                                    Integer nSaltBuckets, PTable.IndexType indexType) throws Exception {
        // For CDC, multitenancy gets derived automatically via the parent table.
        createTable(conn, cdc_sql, encodingScheme, false, nSaltBuckets, indexType, false, null);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(tableName);
        tableName = SchemaUtil.getTableNameFromFullName(tableName);
        IndexToolIT.runIndexTool(false, schemaName, tableName,
                "\""+CDCUtil.getCDCIndexName(cdcName)+"\"");
        String indexFullName = SchemaUtil.getTableName(schemaName,
                CDCUtil.getCDCIndexName(cdcName));
        TestUtil.waitForIndexState(conn, indexFullName, PIndexState.ACTIVE);
    }

    protected void assertCDCState(Connection conn, String cdcName, String expInclude,
                                  int idxType) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT cdc_include FROM " +
                "system.catalog WHERE table_name = '" + cdcName +
                "' AND column_name IS NULL and column_family IS NULL")) {
            assertEquals(true, rs.next());
            assertEquals(expInclude, rs.getString(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT index_type FROM " +
                "system.catalog WHERE table_name = '" + CDCUtil.getCDCIndexName(cdcName) +
                "' AND column_name IS NULL and column_family IS NULL")) {
            assertEquals(true, rs.next());
            assertEquals(idxType, rs.getInt(1));
        }
    }

    protected void assertPTable(String cdcName, Set<PTable.CDCChangeScope> expIncludeScopes,
                                String tableName, String datatableName)
            throws SQLException {
        Properties props = new Properties();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(tableName);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
        PTable cdcTable = PhoenixRuntime.getTable(conn, cdcFullName);
        assertEquals(expIncludeScopes, cdcTable.getCDCIncludeScopes());
        assertEquals(expIncludeScopes, TableProperty.INCLUDE.getPTableValue(cdcTable));
        assertNull(cdcTable.getIndexState()); // Index state should be null for CDC.
        assertNull(cdcTable.getIndexType()); // This is not an index.
        assertEquals(tableName, cdcTable.getParentName().getString());
        String indexFullName = SchemaUtil.getTableName(schemaName,
                CDCUtil.getCDCIndexName(cdcName));
        assertEquals(cdcTable.getPhysicalName().getString(), tableName == datatableName ?
                indexFullName : getViewIndexPhysicalName(datatableName));
    }

    protected void assertSaltBuckets(Connection conn, String tableName, Integer nbuckets)
            throws SQLException {
        PTable table = PhoenixRuntime.getTable(conn, tableName);
        assertSaltBuckets(table, nbuckets);
    }

    protected void assertSaltBuckets(PTable table, Integer nbuckets) {
        if (nbuckets == null || nbuckets == 0) {
            assertNull(table.getBucketNum());
        } else {
            assertEquals(nbuckets, table.getBucketNum());
        }
    }

    protected void assertNoResults(Connection conn, String cdcName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from " + cdcName);
            assertFalse(rs.next());
        }
    }

    protected Connection newConnection() throws SQLException {
        return newConnection(null);
    }

    protected Connection newConnection(String tenantId) throws SQLException {
        Properties props = new Properties();
        // Uncomment these only while debugging.
        //props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        //props.put("hbase.client.scanner.timeout.period", "6000000");
        //props.put("phoenix.query.timeoutMs", "6000000");
        //props.put("zookeeper.session.timeout", "6000000");
        //props.put("hbase.rpc.timeout", "6000000");
        if (tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        return DriverManager.getConnection(getUrl(), props);
    }

    private Map<String, Object> addChange(Connection conn, Map preImage,
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
        cdcChange.put(CDC_PRE_IMAGE, preImage);
        if (changeType == CDC_UPSERT_EVENT_TYPE) {
            Map<String, Object> changeImage = new HashMap<>();
            changeImage.putAll(values);
            cdcChange.put(CDC_CHANGE_IMAGE, changeImage);
            Map<String, Object> postImage = new HashMap<>();
            postImage.putAll(preImage);
            postImage.putAll(changeImage);
            cdcChange.put(CDC_POST_IMAGE, postImage);
        }
        return cdcChange;
    }

    // FIXME: Add the following with consecutive upserts on the sake PK (no delete in between):
    //  - with different values
    //  - with a null
    //  - missing columns
    protected List<ChangeRow> generateChanges(long startTS, String[] tenantids, String tableName,
                                              String datatableNameForDDL, CommitAdapter committer)
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
                c1 = addChange(conn, new HashMap(), startTS,
                        CDC_UPSERT_EVENT_TYPE, tableName, pk1, new TreeMap<String, Object>() {{
                            put("V1", 100L);
                            put("V2", 1000L);
                            put("B.VB", 10000L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c1));
                c2 = addChange(conn, new HashMap(), startTS,
                        CDC_UPSERT_EVENT_TYPE, tableName, pk2, new TreeMap<String, Object>() {{
                            put("V1", 200L);
                            put("V2", 2000L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk2, c2));
                committer.commit(conn);

                c3 = addChange(conn, new HashMap(), startTS +=100,
                        CDC_UPSERT_EVENT_TYPE,
                        tableName, pk3, new TreeMap<String, Object>() {{
                            put("V1", 300L);
                            put("V2", null);
                            put("B.VB", null);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk3, c3));
                committer.commit(conn);

                c4 = addChange(conn, (Map) c1.get(CDC_POST_IMAGE),
                        startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
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
                dropV3Done = true;
            }
            try (Connection conn = newConnection(tid)) {
                c5 = addChange(conn, (Map) c4.get(CDC_POST_IMAGE), startTS +=100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                changes.add(new ChangeRow(tid, startTS, pk1, c5));
                committer.commit(conn);

                c6 = addChange(conn, new HashMap(),
                        startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 102L);
                            put("V2", 1002L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c6));
                committer.commit(conn);

                c7 = addChange(conn, (Map) c6.get(CDC_POST_IMAGE), startTS +=100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                changes.add(new ChangeRow(tid, startTS, pk1, c7));
                committer.commit(conn);

                c8 = addChange(conn, (Map) c2.get(CDC_POST_IMAGE),
                        startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk2,
                        new TreeMap<String, Object>() {{
                            put("V1", 201L);
                            put("V2", null);
                            put("B.VB", 20001L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk2, c8));
                committer.commit(conn);

                c9 = addChange(conn, new HashMap(),
                        startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 103L);
                            put("V2", 1003L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c9));
                committer.commit(conn);

                c10 = addChange(conn, (Map) c9.get(CDC_POST_IMAGE), startTS +=100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                changes.add(new ChangeRow(tid, startTS, pk1, c10));
                committer.commit(conn);

                c11 = addChange(conn, new HashMap(),
                        startTS +=100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 104L);
                            put("V2", 1004L);
                        }});
                changes.add(new ChangeRow(tid, startTS, pk1, c11));
                committer.commit(conn);

                c12 = addChange(conn, (Map) c11.get(CDC_POST_IMAGE), startTS +=100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                changes.add(new ChangeRow(tid, startTS, pk1, c12));
                committer.commit(conn);
            }
        }
        committer.reset();
        return changes;
    }

    private void _copyScopeIfRelevant(Set<PTable.CDCChangeScope> changeScopes,
                                      PTable.CDCChangeScope changeScope,
                                      Map<String, Object> change, Map<String, Object> expChange,
                                      String scopeKeyName) {
        if (changeScopes.contains(changeScope) && change.containsKey(scopeKeyName)) {
            expChange.put(scopeKeyName, change.get(scopeKeyName));
        }
    }

    protected void verifyChanges(String tenantId, ResultSet rs, List<ChangeRow> changes,
                                 Set<PTable.CDCChangeScope> changeScopes,
                                 boolean mutableTable) throws Exception {
        for (int i = 0, changenr = 0; i < changes.size(); ++i) {
            ChangeRow changeRow = changes.get(i);
            if (changeRow.getTenantID() != tenantId) {
                continue;
            }
            Map<String, Object> expChange = new HashMap<>();
            Map<String, Object> change = changeRow.change;
            expChange.put(CDC_EVENT_TYPE, change.get(CDC_EVENT_TYPE));
            _copyScopeIfRelevant(changeScopes, PTable.CDCChangeScope.PRE, change, expChange,
                    CDC_PRE_IMAGE);
            _copyScopeIfRelevant(changeScopes, PTable.CDCChangeScope.POST, change, expChange,
                    CDC_POST_IMAGE);
            _copyScopeIfRelevant(changeScopes, PTable.CDCChangeScope.CHANGE, change, expChange,
                    CDC_CHANGE_IMAGE);
            String changeDesc = "Change " + (changenr+1) + ": " + changeRow;
            assertTrue(changeDesc, rs.next());
            Map cdcObj = gson.fromJson(rs.getString(3), HashMap.class);
            // This is needed because for immutable tables, CDC can't distinguish a null value from
            //  that of a a missing cell.
            if (!mutableTable && changeDesc != null) {
                _purgeNulls(changeRow.getPreImage());
                _purgeNulls(changeRow.getChangeImage());
                _purgeNulls(changeRow.getPostImage());
            }
            assertEquals(changeDesc, changeRow.getChangeTimestamp(),
                    rs.getDate(1).getTime());
            for (Map.Entry<String, Object> pk: changeRow.getPrimaryKeys().entrySet()) {
                assertEquals(changeDesc, pk.getValue(), rs.getObject(pk.getKey()));
            }
            assertEquals(changeDesc, expChange, cdcObj);
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

    protected List<ChangeRow> generateChangesImmutableTable(long startTS, String[] tenantids,
                                                            String tableName, CommitAdapter committer)
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
                c1 = addChange(conn, new HashMap(), startTS,
                        CDC_UPSERT_EVENT_TYPE, tableName, pk1, new TreeMap<String, Object>() {{
                            put("V1", 100L);
                            put("V2", 1000L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c1));
                c2 = addChange(conn, new HashMap(), startTS += 100,
                        CDC_UPSERT_EVENT_TYPE, tableName, pk2, new TreeMap<String, Object>() {{
                            put("V1", 200L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk2, c2));
                c3 = addChange(conn, new HashMap(), startTS += 100,
                        CDC_UPSERT_EVENT_TYPE,
                        tableName, pk3, new TreeMap<String, Object>() {{
                            put("V1", 300L);
                            put("V2", null);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk3, c3));
                c4 = addChange(conn, (Map) c1.get(CDC_POST_IMAGE), startTS += 100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c4));
                c5 = addChange(conn, new HashMap(),
                        startTS += 100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 102L);
                            put("V2", 1002L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c5));
                c6 = addChange(conn, (Map) c5.get(CDC_POST_IMAGE), startTS += 100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c6));
                c7 = addChange(conn, new HashMap(),
                        startTS += 100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 103L);
                            put("V2", 1003L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c7));
                c8 = addChange(conn, (Map) c7.get(CDC_POST_IMAGE), startTS += 100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c8));
                c9 = addChange(conn, new HashMap(),
                        startTS += 100, CDC_UPSERT_EVENT_TYPE, tableName, pk1,
                        new TreeMap<String, Object>() {{
                            put("V1", 104L);
                            put("V2", 1004L);
                        }});
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c9));
                c10 = addChange(conn, (Map) c9.get(CDC_POST_IMAGE), startTS += 100,
                        CDC_DELETE_EVENT_TYPE, tableName, pk1, null);
                committer.commit(conn);
                changes.add(new ChangeRow(tid, startTS, pk1, c10));
            }
        }
        committer.reset();
        return changes;
    }

    protected class ChangeRow {
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

    protected abstract class CommitAdapter {
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

    protected final CommitAdapter COMMIT_SUCCESS = new CommitAdapter() {
        @Override
        public void commit(Connection conn) throws SQLException {
            conn.commit();
        }
    };

    protected final CommitAdapter COMMIT_FAILURE_EXPECTED = new CommitAdapter() {
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
