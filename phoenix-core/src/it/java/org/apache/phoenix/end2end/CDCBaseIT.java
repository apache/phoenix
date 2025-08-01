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
package org.apache.phoenix.end2end;

import static java.util.stream.Collectors.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;
import static org.apache.phoenix.query.QueryConstants.CDC_CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_JSON_COL_NAME;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_FOREVER;
import static org.apache.phoenix.util.CDCUtil.CDC_STREAM_NAME_FORMAT;
import static org.apache.phoenix.util.MetaDataUtil.getViewIndexPhysicalName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.types.PBinaryBase;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDCBaseIT extends ParallelStatsDisabledIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(CDCBaseIT.class);
  protected static final ObjectMapper mapper = new ObjectMapper();
  static {
    SimpleModule module = new SimpleModule("ChangeRow", new Version(1, 0, 0, null, null, null));
    PhoenixArraySerializer phoenixArraySerializer = new PhoenixArraySerializer(PhoenixArray.class);
    module.addSerializer(PhoenixArray.class, phoenixArraySerializer);
    mapper.registerModule(module);
    mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
  }

  static final HashSet<PTable.CDCChangeScope> CHANGE_IMG =
    new HashSet<>(Arrays.asList(PTable.CDCChangeScope.CHANGE));
  static final HashSet<PTable.CDCChangeScope> PRE_POST_IMG =
    new HashSet<>(Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST));
  static final HashSet<PTable.CDCChangeScope> ALL_IMG = new HashSet<>(Arrays
    .asList(PTable.CDCChangeScope.CHANGE, PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST));

  protected ManualEnvironmentEdge injectEdge;
  protected Calendar cal = Calendar.getInstance();

  protected static RegionCoprocessorEnvironment taskRegionEnvironment;

  protected void createTable(Connection conn, String table_sql) throws Exception {
    createTable(conn, table_sql, null, false, null, false, null);
  }

  protected void createTable(Connection conn, String table_sql,
    PTable.QualifierEncodingScheme encodingScheme) throws Exception {
    createTable(conn, table_sql, encodingScheme, false, null, false, null);
  }

  protected void createTable(Connection conn, String table_sql,
    PTable.QualifierEncodingScheme encodingScheme, boolean multitenant) throws Exception {
    createTable(conn, table_sql, encodingScheme, multitenant, null, false, null);
  }

  protected void createTable(Connection conn, String table_sql,
    PTable.QualifierEncodingScheme encodingScheme, boolean multitenant, Integer nSaltBuckets,
    boolean immutable, PTable.ImmutableStorageScheme immutableStorageScheme) throws Exception {
    createTable(conn, table_sql, new HashMap<String, Object>() {
      {
        put(TableProperty.COLUMN_ENCODED_BYTES.getPropertyName(),
          encodingScheme != null ? new Byte(encodingScheme.getSerializedMetadataValue()) : null);
        put(TableProperty.MULTI_TENANT.getPropertyName(), multitenant);
        put(TableProperty.SALT_BUCKETS.getPropertyName(), nSaltBuckets);
        put(TableProperty.IMMUTABLE_ROWS.getPropertyName(), immutable);
        put(TableProperty.IMMUTABLE_STORAGE_SCHEME.getPropertyName(),
          immutableStorageScheme != null ? immutableStorageScheme.name() : null);
      }
    });
  }

  protected void createTable(Connection conn, String table_sql, Map<String, Object> tableProps)
    throws Exception {
    List<String> props = new ArrayList<>();
    Byte encodingScheme = (Byte) TableProperty.COLUMN_ENCODED_BYTES.getValue(tableProps);
    if (
      encodingScheme != null && encodingScheme != QueryServicesOptions.DEFAULT_COLUMN_ENCODED_BYTES
    ) {
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
    Boolean immutableTable = (Boolean) TableProperty.IMMUTABLE_ROWS.getValue(tableProps);
    if (immutableTable) {
      props.add(TableProperty.IMMUTABLE_ROWS.getPropertyName() + "=true");
    }
    PTable.ImmutableStorageScheme immutableStorageScheme =
      (PTable.ImmutableStorageScheme) TableProperty.IMMUTABLE_STORAGE_SCHEME.getValue(tableProps);
    if (immutableStorageScheme != null) {
      props.add(TableProperty.IMMUTABLE_STORAGE_SCHEME.getPropertyName() + "="
        + immutableStorageScheme.name());
    }
    table_sql += " " + String.join(", ", props);
    LOGGER.debug("Creating table with SQL: " + table_sql);
    conn.createStatement().execute(table_sql);
  }

  protected void createCDC(Connection conn, String cdc_sql) throws Exception {
    createCDC(conn, cdc_sql, null);
  }

  protected void createCDC(Connection conn, String cdc_sql,
    PTable.QualifierEncodingScheme encodingScheme) throws Exception {
    // For CDC, multitenancy gets derived automatically via the parent table.
    createTable(conn, cdc_sql, encodingScheme, false, null, false, null);
  }

  protected void dropCDC(Connection conn, String cdcName, String tableName) throws SQLException {
    conn.createStatement().execute("DROP CDC " + cdcName + " ON " + tableName);
  }

  protected void assertCDCState(Connection conn, String cdcName, String expInclude, int idxType)
    throws SQLException {
    try (ResultSet rs = conn.createStatement()
      .executeQuery("SELECT cdc_include FROM " + "system.catalog WHERE table_name = '" + cdcName
        + "' AND column_name IS NULL and column_family IS NULL")) {
      assertEquals(true, rs.next());
      assertEquals(expInclude, rs.getString(1));
    }
    try (ResultSet rs = conn.createStatement()
      .executeQuery("SELECT index_type FROM " + "system.catalog WHERE table_name = '"
        + CDCUtil.getCDCIndexName(cdcName)
        + "' AND column_name IS NULL and column_family IS NULL")) {
      assertEquals(true, rs.next());
      assertEquals(idxType, rs.getInt(1));
    }
  }

  protected void assertPTable(String cdcName, Set<PTable.CDCChangeScope> expIncludeScopes,
    String tableName, String datatableName) throws SQLException {
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
    String indexFullName = SchemaUtil.getTableName(schemaName, CDCUtil.getCDCIndexName(cdcName));
    assertEquals(cdcTable.getPhysicalName().getString(),
      tableName == datatableName ? indexFullName : getViewIndexPhysicalName(datatableName));
    PTable cdcIndexTable = PhoenixRuntime.getTable(conn, indexFullName);
    assertEquals(cdcIndexTable.getTTLExpression(), TTL_EXPRESSION_FOREVER);
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
    return newConnection(tenantId, new Properties());
  }

  protected Connection newConnection(String tenantId, Properties props) throws SQLException {
    // Uncomment these only while debugging.
    // props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
    // props.put("hbase.client.scanner.timeout.period", "6000000");
    // props.put("phoenix.query.timeoutMs", "6000000");
    // props.put("zookeeper.session.timeout", "6000000");
    // props.put("hbase.rpc.timeout", "6000000");
    if (tenantId != null) {
      props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    }
    return DriverManager.getConnection(getUrl(), props);
  }

  private ChangeRow addChange(Connection conn, String tableName, ChangeRow changeRow)
    throws SQLException {
    Map<String, Object> pks = changeRow.pks;
    Map<String, Object> values = changeRow.change;
    long changeTS = changeRow.changeTS;
    if (conn != null) {
      String sql;
      if (changeRow.getChangeType() == CDC_DELETE_EVENT_TYPE) {
        String predicates =
          pks.entrySet().stream().map(e -> e.getKey() + " = ?").collect(joining(" AND "));
        sql = "DELETE FROM " + tableName + " WHERE " + predicates;
      } else {
        String columnList =
          Stream.concat(pks.keySet().stream(), values.keySet().stream()).collect(joining(", "));
        String bindSql =
          Stream.generate(() -> "?").limit(pks.size() + values.size()).collect(joining(", "));
        sql = "UPSERT INTO " + tableName + " (" + columnList + ") VALUES (" + bindSql + ")";
      }
      cal.setTimeInMillis(changeTS);
      injectEdge.setValue(changeTS);
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        int bindCnt = 1;
        for (Object val : pks.values()) {
          stmt.setObject(bindCnt, val);
          ++bindCnt;
        }
        if (changeRow.getChangeType() != CDC_DELETE_EVENT_TYPE) {
          for (Object val : values.values()) {
            stmt.setObject(bindCnt, val);
            ++bindCnt;
          }
        }
        stmt.executeUpdate();
      }
    }
    return changeRow;
  }

  protected List<Set<ChangeRow>> generateMutations(String tenantId, long startTS,
    Map<String, String> pkColumns, Map<String, String> dataColumns, int nRows, int nBatches) {
    Random rand = new Random();
    // Generate unique rows
    List<Map<String, Object>> rows = new ArrayList<>(nRows);
    Set<Map<String, Object>> rowSet = new HashSet<>(nRows);
    for (int i = 0; i < nRows; ++i) {
      Map<String, Object> row = generateSampleData(rand, pkColumns, false);
      if (rowSet.contains(row)) {
        --i;
        continue;
      }
      rows.add(row);
      rowSet.add(row);
    }

    // Generate the batches. At each batch, determine the row participation and type of
    // operation and the data columns for upserts.
    List<Set<ChangeRow>> batches = new ArrayList<>(nBatches);
    Set<Map<String, Object>> mutatedRows = new HashSet<>(nRows);
    long batchTS = startTS;
    boolean gotDelete = false;
    for (int i = 0; i < nBatches; ++i) {
      Set<ChangeRow> batch = new TreeSet<>();
      for (int j = 0; j < nRows; ++j) {
        if (rand.nextInt(nRows) % 2 == 0) {
          boolean isDelete;
          if (i > nBatches / 2 && !gotDelete) {
            // Force a delete if there was none so far.
            isDelete = true;
          } else {
            isDelete = mutatedRows.contains(rows.get(j)) && rand.nextInt(5) == 0;
          }
          ChangeRow changeRow;
          if (isDelete) {
            changeRow = new ChangeRow(tenantId, batchTS, rows.get(j), null);
            gotDelete = true;
          } else {
            changeRow = new ChangeRow(tenantId, batchTS, rows.get(j),
              generateSampleData(rand, dataColumns, true));
          }
          batch.add(changeRow);
          mutatedRows.add(rows.get(j));
        }
      }
      batches.add(batch);
      batchTS += 100;
    }

    // For debug: uncomment to see the mutations generated.
    LOGGER.debug("----- DUMP Mutations -----");
    int bnr = 1, mnr = 0;
    for (Set<ChangeRow> batch : batches) {
      for (ChangeRow change : batch) {
        LOGGER.debug("Mutation: " + (++mnr) + " in batch: " + bnr + " " + " tenantId:"
          + change.tenantId + " changeTS: " + change.changeTS + " pks: " + change.pks + " change: "
          + change.change);
      }
      ++bnr;
    }
    LOGGER.debug("----------");
    return batches;
  }

  private Map<String, Object> generateSampleData(Random rand, Map<String, String> columns,
    boolean nullOK) {
    Map<String, Object> row = new HashMap<>();
    for (Map.Entry<String, String> pkCol : columns.entrySet()) {
      if (nullOK && rand.nextInt(5) == 0) {
        row.put(pkCol.getKey(), null);
      } else {
        PDataType dt = PDataType.fromSqlTypeName(pkCol.getValue());
        Object val;
        if (dt instanceof PChar || dt instanceof PVarchar) {
          val = dt.getSampleValue(5);
          val = dt instanceof PChar ? ((String) val).trim() : val;
          // Make sure it is at least of length 1.
          val = ((String) val).length() == 0 ? "a" : val;
        } else {
          val = dt instanceof PBinaryBase ? dt.getSampleValue(5) : dt.getSampleValue();
        }
        row.put(pkCol.getKey(), val);
      }
    }
    return row;
  }

  protected void applyMutations(CommitAdapter committer, String schemaName, String tableName,
    String datatableName, String tid, List<Set<ChangeRow>> batches, String cdcName)
    throws Exception {
    EnvironmentEdgeManager.injectEdge(injectEdge);
    try (Connection conn = committer.getConnection(tid)) {
      for (Set<ChangeRow> batch : batches) {
        for (ChangeRow changeRow : batch) {
          addChange(conn, tableName, changeRow);
        }
        committer.commit(conn);
      }
    }
    committer.reset();

    // For debug: uncomment to see the exact HBase cells.
    dumpCells(schemaName, tableName, datatableName, cdcName);
  }

  protected void dumpCells(String schemaName, String tableName, String datatableName,
    String cdcName) throws Exception {
    LOGGER.debug("----- DUMP data table: " + datatableName + " -----");
    SingleCellIndexIT.dumpTable(datatableName);
    String indexName = CDCUtil.getCDCIndexName(cdcName);
    String indexTableName = SchemaUtil.getTableName(schemaName,
      tableName == datatableName ? indexName : getViewIndexPhysicalName(datatableName));
    LOGGER.debug("----- DUMP index table: " + indexTableName + " -----");
    try {
      SingleCellIndexIT.dumpTable(indexTableName);
    } catch (TableNotFoundException e) {
      // Ignore, this would happen if CDC is not yet created. This use case is going to go
      // away soon anyway.
    }
    LOGGER.debug("----------");
  }

  protected void dumpCDCResults(Connection conn, String cdcName, Map<String, String> pkColumns,
    String cdcQuery) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(cdcQuery)) {
        LOGGER.debug("----- DUMP CDC: " + cdcName + " -----");
        for (int i = 0; rs.next(); ++i) {
          LOGGER.debug("CDC row: " + (i + 1) + " timestamp=" + rs.getDate(1).getTime() + " "
            + collectColumns(pkColumns, rs) + ", " + CDC_JSON_COL_NAME + "="
            + rs.getString(pkColumns.size() + 2));
        }
        LOGGER.debug("----------");
      }
    }
  }

  private static String collectColumns(Map<String, String> pkColumns, ResultSet rs) {
    return pkColumns.keySet().stream().map(k -> {
      try {
        return k + "=" + rs.getObject(k);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.joining(", "));
  }

  protected void createTable(Connection conn, String tableName, Map<String, String> pkColumns,
    Map<String, String> dataColumns, boolean multitenant,
    PTable.QualifierEncodingScheme encodingScheme, Integer tableSaltBuckets, boolean immutable,
    PTable.ImmutableStorageScheme immutableStorageScheme) throws Exception {
    List<String> pkConstraintCols = new ArrayList<>();
    if (multitenant) {
      pkConstraintCols.add("TENANT_ID");
    }
    pkConstraintCols.addAll(pkColumns.keySet());
    String pkConstraintSql =
      "CONSTRAINT PK PRIMARY KEY (" + String.join(", ", pkConstraintCols) + ")";
    String pkColSql = pkColumns.entrySet().stream()
      .map(e -> e.getKey() + " " + e.getValue() + " NOT NULL").collect(joining(", "));
    String dataColSql = dataColumns.entrySet().stream().map(e -> e.getKey() + " " + (
    // Some types need a size.
    e.getValue().equals("CHAR") || e.getValue().equals("BINARY")
      ? e.getValue() + "(5)"
      : e.getValue())).collect(joining(", "));
    String tableSql =
      "CREATE TABLE " + tableName + " (" + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
        + pkColSql + ", " + dataColSql + ", " + pkConstraintSql + ")";
    createTable(conn, tableSql, encodingScheme, multitenant, tableSaltBuckets, immutable,
      immutableStorageScheme);
  }

  // FIXME: Add the following with consecutive upserts on the sake PK (no delete in between):
  // - with different values
  // - with a null
  // - missing columns
  protected List<ChangeRow> generateChanges(long startTS, String[] tenantids, String tableName,
    String datatableNameForDDL, CommitAdapter committer) throws Exception {
    return generateChanges(startTS, tenantids, tableName, datatableNameForDDL, committer, "v3", 0);
  }

  protected List<ChangeRow> generateChanges(long startTS, String[] tenantids, String tableName,
    String datatableNameForDDL, CommitAdapter committer, String columnToDrop, int startKey)
    throws Exception {
    List<ChangeRow> changes = new ArrayList<>();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTS);
    boolean dropColumnDone = false;
    committer.init();
    Map<String, Object> rowid1 = new HashMap() {
      {
        put("K", startKey + 1);
      }
    };
    Map<String, Object> rowid2 = new HashMap() {
      {
        put("K", startKey + 2);
      }
    };
    Map<String, Object> rowid3 = new HashMap() {
      {
        put("K", startKey + 3);
      }
    };
    for (String tid : tenantids) {
      try (Connection conn = committer.getConnection(tid)) {
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 100L);
              put("V2", 1000L);
              put("B.VB", 10000L);
            }
          })));
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS, rowid2, new TreeMap<String, Object>() {
            {
              put("V1", 200L);
              put("V2", 2000L);
            }
          })));
        committer.commit(conn);
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid3, new TreeMap<String, Object>() {
            {
              put("V1", 300L);
              put("V2", null);
              put("B.VB", null);
            }
          })));
        committer.commit(conn);

        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 101L);
            }
          })));
        committer.commit(conn);
      }
      if (datatableNameForDDL != null && !dropColumnDone && columnToDrop != null) {
        try (Connection conn = newConnection()) {
          conn.createStatement().execute("ALTER TABLE " + datatableNameForDDL + " DROP COLUMN v3");
        }
        dropColumnDone = true;
      }
      try (Connection conn = newConnection(tid)) {
        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);

        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);

        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 102L);
              put("V2", 1002L);
            }
          })));
        committer.commit(conn);

        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);

        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid2, new TreeMap<String, Object>() {
            {
              put("V1", 201L);
              put("V2", null);
              put("B.VB", 20001L);
            }
          })));
        committer.commit(conn);

        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 103L);
              put("V2", 1003L);
            }
          })));
        committer.commit(conn);

        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);

        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 104L);
              put("V2", 1004L);
            }
          })));
        committer.commit(conn);
        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);
      }
    }
    committer.reset();
    // For debug logging, uncomment this code to see the list of changes.
    for (int i = 0; i < changes.size(); ++i) {
      LOGGER.debug("----- generated change: " + i + " tenantId:" + changes.get(i).tenantId
        + " changeTS: " + changes.get(i).changeTS + " pks: " + changes.get(i).pks + " change: "
        + changes.get(i).change);
    }
    return changes;
  }

  protected void verifyChangesViaSCN(String tenantId, Connection conn, String cdcFullName,
    Map<String, String> pkColumns, String dataTableName, Map<String, String> dataColumns,
    List<ChangeRow> changes, long startTS, long endTS) throws Exception {
    List<ChangeRow> filteredChanges = new ArrayList<>();
    for (ChangeRow change : changes) {
      if (change.changeTS >= startTS && change.changeTS <= endTS) {
        filteredChanges.add(change);
      }
    }
    String cdcSql = "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROm " + cdcFullName + " WHERE "
      + " PHOENIX_ROW_TIMESTAMP() >= CAST(CAST(" + startTS + " AS BIGINT) AS TIMESTAMP) "
      + "AND PHOENIX_ROW_TIMESTAMP() <= CAST(CAST(" + endTS + " AS BIGINT) AS TIMESTAMP)";
    dumpCDCResults(conn, cdcFullName, new TreeMap<String, String>() {
      {
        put("K1", "INTEGER");
      }
    }, cdcSql);
    try (ResultSet rs = conn.createStatement().executeQuery(cdcSql)) {
      verifyChangesViaSCN(tenantId, rs, dataTableName, dataColumns, filteredChanges, CHANGE_IMG);
    }
  }

  protected void verifyChangesViaSCN(String tenantId, ResultSet rs, String dataTableName,
    Map<String, String> dataColumns, List<ChangeRow> changes,
    Set<PTable.CDCChangeScope> changeScopes) throws Exception {
    // CDC guarantees that the set of changes on a given row is delivered in the order of
    // their change timestamps. That is why we need to convert the list of changes to
    // a collection of per row list of changes
    Map<Map<String, Object>, List<ChangeRow>> changeMap = new HashMap();
    Set<Map<String, Object>> deletedRows = new HashSet<>();
    for (ChangeRow changeRow : changes) {
      if (tenantId != null && changeRow.getTenantID() != tenantId) {
        continue;
      }
      if (changeRow.getChangeType() == CDC_DELETE_EVENT_TYPE) {
        // Consecutive delete operations don't appear as separate events.
        if (deletedRows.contains(changeRow.pks)) {
          continue;
        }
        deletedRows.add(changeRow.pks);
      } else {
        deletedRows.remove(changeRow.pks);
      }
      List<ChangeRow> rowVersionList = changeMap.get(changeRow.pks);
      if (rowVersionList == null) {
        rowVersionList = new ArrayList<>();
        changeMap.put(changeRow.pks, rowVersionList);
      }
      rowVersionList.add(changeRow);
    }

    while (rs.next()) {
      Map<String, Object> pks = new HashMap<>();
      for (Map.Entry<String, Object> pkCol : changes.get(0).pks.entrySet()) {
        pks.put(pkCol.getKey(), rs.getObject(pkCol.getKey()));
      }
      ChangeRow changeRow = changeMap.get(pks).remove(0);
      String changeDesc = "Change: " + changeRow;
      for (Map.Entry<String, Object> pkCol : changeRow.pks.entrySet()) {
        if (!pkCol.getValue().equals(rs.getObject(pkCol.getKey()))) {
          assertEquals(changeDesc, pkCol.getValue(), rs.getObject(pkCol.getKey()));
        }
      }
      Map<String, Object> cdcObj =
        mapper.reader(HashMap.class).readValue(rs.getString(changeRow.pks.size() + 2));
      if (!changeRow.getChangeType().equals(cdcObj.get(CDC_EVENT_TYPE))) {
        assertEquals(changeDesc, changeRow.getChangeType(), cdcObj.get(CDC_EVENT_TYPE));
      }
      if (
        cdcObj.containsKey(CDC_PRE_IMAGE) && !((Map) cdcObj.get(CDC_PRE_IMAGE)).isEmpty()
          && changeScopes.contains(PTable.CDCChangeScope.PRE)
      ) {
        Map<String, Object> preImage = getRowImage(changeDesc, tenantId, dataTableName, dataColumns,
          changeRow, changeRow.changeTS);
        assertEquals(changeDesc, preImage,
          fillInNulls((Map<String, Object>) cdcObj.get(CDC_PRE_IMAGE), dataColumns.keySet()));
      }
      if (changeScopes.contains(PTable.CDCChangeScope.CHANGE)) {
        assertEquals(changeDesc,
          encodeValues(fillInNulls(changeRow.change, dataColumns.keySet()), dataColumns),
          fillInNulls((Map<String, Object>) cdcObj.get(CDC_CHANGE_IMAGE), dataColumns.keySet()));
      }
      if (
        changeRow.getChangeType() != CDC_DELETE_EVENT_TYPE
          && changeScopes.contains(PTable.CDCChangeScope.POST)
      ) {
        Map<String, Object> postImage = getRowImage(changeDesc, tenantId, dataTableName,
          dataColumns, changeRow, changeRow.changeTS + 1);
        assertEquals(changeDesc, postImage,
          fillInNulls((Map<String, Object>) cdcObj.get(CDC_POST_IMAGE), dataColumns.keySet()));
      }
    }

    // Make sure that all the expected changes are returned by CDC
    for (List<ChangeRow> rowVersionList : changeMap.values()) {
      assertTrue(rowVersionList.isEmpty());
    }
  }

  protected Map<String, Object> getRowImage(String changeDesc, String tenantId,
    String dataTableName, Map<String, String> dataColumns, ChangeRow changeRow, long scnTimestamp)
    throws Exception {
    Map<String, Object> image = new HashMap<>();
    Properties props = new Properties();
    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scnTimestamp));
    Map<String, String> projections =
      dataColumns.keySet().stream().collect(toMap(s -> s, s -> s.replaceFirst(".*\\.", "")));
    String projection = projections.values().stream().collect(joining(", "));
    String predicates =
      changeRow.pks.entrySet().stream().map(e -> e.getKey() + " = ?").collect(joining(" AND "));
    try (Connection conn = newConnection(tenantId, props)) {
      PreparedStatement stmt = conn.prepareStatement(
        "SELECT " + projection + " FROM " + dataTableName + " WHERE " + predicates);
      int bindCnt = 1;
      for (Object val : changeRow.pks.values()) {
        stmt.setObject(bindCnt, val);
        ++bindCnt;
      }
      // Create projection without namespace.
      ResultSet rs = stmt.executeQuery();
      assertTrue(changeDesc, rs.next());
      for (String colName : projections.keySet()) {
        PDataType dt = PDataType.fromSqlTypeName(dataColumns.get(colName));
        image.put(colName, getJsonEncodedValue(rs.getObject(projections.get(colName)), dt));
      }
    }
    return image;
  }

  private Object getJsonEncodedValue(Object val, PDataType dt) {
    // Our JSON parser uses Long and Double types.
    if (val instanceof Byte || val instanceof Short || val instanceof Integer) {
      val = ((Number) val).longValue();
    } else if (val instanceof Float) {
      val = ((Number) val).doubleValue();
    } else {
      val = CDCUtil.getColumnEncodedValue(val, dt);
    }
    return val;
  }

  private Map<String, Object> fillInNulls(Map<String, Object> image, Collection<String> dataCols) {
    if (image != null) {
      image = new HashMap<>(image);
      for (String colName : dataCols) {
        if (!image.containsKey(colName)) {
          image.put(colName, null);
        }
      }
    }
    return image;
  }

  private Map<String, Object> encodeValues(Map<String, Object> image,
    Map<String, String> dataColumns) {
    if (image != null) {
      image = new HashMap<>(image);
      for (Map.Entry<String, String> col : dataColumns.entrySet()) {
        if (image.containsKey(col.getKey())) {
          image.put(col.getKey(), getJsonEncodedValue(image.get(col.getKey()),
            PDataType.fromSqlTypeName(col.getValue())));
        }
      }
    }
    return image;
  }

  protected List<ChangeRow> generateChangesImmutableTable(long startTS, String[] tenantids,
    String schemaName, String tableName, String datatableName, CommitAdapter committer,
    String cdcName) throws Exception {
    List<ChangeRow> changes = new ArrayList<>();
    EnvironmentEdgeManager.injectEdge(injectEdge);
    injectEdge.setValue(startTS);
    committer.init();
    Map<String, Object> rowid1 = new HashMap() {
      {
        put("K", 1);
      }
    };
    Map<String, Object> rowid2 = new HashMap() {
      {
        put("K", 2);
      }
    };
    Map<String, Object> rowid3 = new HashMap() {
      {
        put("K", 3);
      }
    };
    for (String tid : tenantids) {
      try (Connection conn = newConnection(tid)) {
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 100L);
              put("V2", 1000L);
            }
          })));
        committer.commit(conn);
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid2, new TreeMap<String, Object>() {
            {
              put("V1", 200L);
            }
          })));
        committer.commit(conn);
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid3, new TreeMap<String, Object>() {
            {
              put("V1", 300L);
              put("V2", null);
            }
          })));
        committer.commit(conn);
        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 102L);
              put("V2", 1002L);
            }
          })));
        committer.commit(conn);
        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 103L);
              put("V2", 1003L);
            }
          })));
        committer.commit(conn);
        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);
        changes.add(addChange(conn, tableName,
          new ChangeRow(tid, startTS += 100, rowid1, new TreeMap<String, Object>() {
            {
              put("V1", 104L);
              put("V2", 1004L);
            }
          })));
        committer.commit(conn);
        changes.add(addChange(conn, tableName, new ChangeRow(tid, startTS += 100, rowid1, null)));
        committer.commit(conn);
      }
    }
    committer.reset();
    // For debug logging, uncomment this code to see the list of changes.
    dumpCells(schemaName, tableName, datatableName, cdcName);
    for (int i = 0; i < changes.size(); ++i) {
      LOGGER.debug("----- generated change: " + i + " tenantId:" + changes.get(i).tenantId
        + " changeTS: " + changes.get(i).changeTS + " pks: " + changes.get(i).pks + " change: "
        + changes.get(i).change);
    }
    return changes;
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
      setterVisibility = JsonAutoDetect.Visibility.NONE,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE,
      creatorVisibility = JsonAutoDetect.Visibility.NONE)
  protected class ChangeRow implements Comparable<ChangeRow> {
    @JsonProperty
    protected final String tenantId;
    @JsonProperty
    protected final long changeTS;
    @JsonProperty
    protected final Map<String, Object> pks;
    @JsonProperty
    protected final Map<String, Object> change;

    public String getTenantID() {
      return tenantId;
    }

    public String getChangeType() {
      return change == null ? CDC_DELETE_EVENT_TYPE : CDC_UPSERT_EVENT_TYPE;
    }

    public long getTimestamp() {
      return changeTS;
    }

    ChangeRow(String tenantid, long changeTS, Map<String, Object> pks, Map<String, Object> change) {
      this.tenantId = tenantid;
      this.changeTS = changeTS;
      this.pks = pks;
      this.change = change;
    }

    public String toString() {
      try {
        return mapper.writerFor(ChangeRow.class).writeValueAsString(this);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    // This implementation only comapres the rows by PK only as it is simply meant to have a
    // consistent order in the same batch.
    @Override
    public int compareTo(ChangeRow o) {
      // Quick check to make sure they both have the same PK.
      if (
        pks.size() != o.pks.size() || !pks.keySet().stream().allMatch(k -> o.pks.containsKey(k))
      ) {
        throw new RuntimeException("Incompatible row for comparison: " + pks + " vs " + o.pks);
      }

      int res;
      for (String col : pks.keySet()) {
        Object val1 = pks.get(col);
        Object val2 = o.pks.get(col);
        if (val1 instanceof byte[]) {
          res = Bytes.compareTo((byte[]) val1, (byte[]) val2);
        } else {
          res = ((Comparable) val1).compareTo(val2);
        }
        if (res != 0) {
          return res;
        }
      }
      return 0;
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

  public static class PhoenixArraySerializer extends StdSerializer<PhoenixArray> {
    protected PhoenixArraySerializer(Class<PhoenixArray> t) {
      super(t);
    }

    @Override
    public void serialize(PhoenixArray value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
      gen.writeStartObject();
      gen.writeStringField("elements", value.toString());
      gen.writeEndObject();
    }
  }

  /**
   * Gets the stream name for a CDC stream.
   * @param conn      The connection to use
   * @param tableName The name of the table
   * @param cdcName   The name of the CDC stream
   * @return The full stream name
   * @throws SQLException if an error occurs
   */
  public String getStreamName(Connection conn, String tableName, String cdcName)
    throws SQLException {
    long creationTS = CDCUtil
      .getCDCCreationTimestamp(conn.unwrap(PhoenixConnection.class).getTableNoCache(tableName));
    return String.format(CDC_STREAM_NAME_FORMAT, tableName, cdcName, creationTS,
      CDCUtil.getCDCCreationUTCDateTime(creationTS));
  }

  /**
   * Gets the status of a CDC stream.
   * @param conn       The connection to use.
   * @param tableName  The name of the table.
   * @param streamName The name of the stream.
   * @return The stream status.
   * @throws Exception if an error occurs.
   */
  public String getStreamStatus(Connection conn, String tableName, String streamName)
    throws Exception {
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT STREAM_STATUS FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
        + " WHERE TABLE_NAME='" + tableName + "' AND STREAM_NAME='" + streamName + "'");
    assertTrue(rs.next());
    return rs.getString(1);
  }

  /**
   * Creates a table and enables CDC on it. This method is shared between CDCStreamIT and
   * CDCStream2IT.
   * @param conn                  The connection to use.
   * @param tableName             The name of the table to create.
   * @param useTaskRegionObserver Whether to use TaskRegionObserver.SelfHealingTask to enable the
   *                              stream.
   * @throws Exception if an error occurs.
   */
  public void createTableAndEnableCDC(Connection conn, String tableName,
    boolean useTaskRegionObserver) throws Exception {
    String cdcName = generateUniqueName();
    String cdcSql = "CREATE CDC " + cdcName + " ON " + tableName;
    conn.createStatement().execute(
      "CREATE TABLE  " + tableName + " (k VARCHAR PRIMARY KEY," + " v1 INTEGER," + " v2 VARCHAR)");
    createCDC(conn, cdcSql, null);
    String streamName = getStreamName(conn, tableName, cdcName);
    if (useTaskRegionObserver) {
      TaskRegionObserver.SelfHealingTask task = new TaskRegionObserver.SelfHealingTask(
        taskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
      task.run();
    } else {
      while (
        !CDCUtil.CdcStreamStatus.ENABLED.toString()
          .equals(getStreamStatus(conn, tableName, streamName))
      ) {
        Thread.sleep(1000);
      }
    }
  }

  /**
   * Partition Metadata class.
   */
  public static class PartitionMetadata {
    public String partitionId;
    public String parentPartitionId;
    public Long startTime;
    public Long endTime;
    public byte[] startKey;
    public byte[] endKey;
    public Long parentStartTime;

    public PartitionMetadata(ResultSet rs) throws Exception {
      partitionId = rs.getString(3);
      parentPartitionId = rs.getString(4);
      startTime = rs.getLong(5);
      endTime = rs.getLong(6);
      startKey = rs.getBytes(7);
      endKey = rs.getBytes(8);
      parentStartTime = rs.getLong(9);
    }
  }

}
