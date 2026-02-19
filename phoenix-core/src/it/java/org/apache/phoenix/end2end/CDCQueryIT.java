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

import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_OLD_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.filter.DistinctPrefixFilter;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.RowKeyOrderedAggregateResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

// NOTE: To debug the query execution, add the below condition or the equivalent where you need a
// breakpoint.
//      if (<table>.getTableName().getString().equals("N000002") ||
//                 <table>.getTableName().getString().equals("__CDC__N000002")) {
//          "".isEmpty();
//      }
@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class CDCQueryIT extends CDCBaseIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(CDCQueryIT.class);
  private static final int MAX_LOOKBACK_AGE = 10; // seconds

  // Offset of the first column, depending on whether PHOENIX_ROW_TIMESTAMP() is in the schema
  // or not.
  private final boolean forView;
  private final PTable.QualifierEncodingScheme encodingScheme;
  private final boolean multitenant;
  private final Integer tableSaltBuckets;
  private final boolean withSchemaName;
  private final boolean caseSensitiveNames;

  public CDCQueryIT(Boolean forView, PTable.QualifierEncodingScheme encodingScheme,
    boolean multitenant, Integer tableSaltBuckets, boolean withSchemaName,
    boolean caseSensitiveNames) {
    this.forView = forView;
    this.encodingScheme = encodingScheme;
    this.multitenant = multitenant;
    this.tableSaltBuckets = tableSaltBuckets;
    this.withSchemaName = withSchemaName;
    this.caseSensitiveNames = caseSensitiveNames;
  }

  @Parameterized.Parameters(name = "forView={0}, encodingScheme={1}, "
    + "multitenant={2}, tableSaltBuckets={3}, withSchemaName={4}, caseSensitiveNames={5}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      { Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, Boolean.FALSE, Boolean.FALSE },
      { Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, Boolean.TRUE, Boolean.FALSE },
      { Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, Boolean.FALSE, Boolean.TRUE },
      { Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, Boolean.TRUE, Boolean.TRUE },
      { Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.FALSE, 4, Boolean.FALSE, Boolean.FALSE },
      { Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.TRUE, 2, Boolean.TRUE, Boolean.FALSE },
      { Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.FALSE, null, Boolean.FALSE, Boolean.FALSE },
      { Boolean.TRUE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, Boolean.FALSE, Boolean.FALSE },
      { Boolean.TRUE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, Boolean.FALSE, Boolean.TRUE }, });
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(MAX_LOOKBACK_AGE));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Before
  public void beforeTest() {
    EnvironmentEdgeManager.reset();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
  }

  private void cdcIndexShouldNotBeUsedForDataTableQueries(Connection conn, String dataTableName,
    String cdcName) throws Exception {
    ResultSet rs = conn.createStatement().executeQuery(
      "EXPLAIN SELECT * FROM " + dataTableName + " WHERE PHOENIX_ROW_TIMESTAMP() < CURRENT_TIME()");
    String explainPlan = QueryUtil.getExplainPlan(rs);
    assertFalse(explainPlan.contains(cdcName));
  }

  private boolean isDistinctPrefixFilterIncludedInFilterList(FilterList filterList) {
    for (Filter filter : filterList.getFilters()) {
      if (filter instanceof DistinctPrefixFilter) {
        return true;
      } else if (filter instanceof FilterList) {
        return isDistinctPrefixFilterIncludedInFilterList((FilterList) filter);
      }
    }
    return false;
  }

  private boolean isDistinctPrefixFilterIncluded(Scan scan) {
    Filter filter = scan.getFilter();
    if (filter != null && filter instanceof DistinctPrefixFilter) {
      return true;
    } else if (filter instanceof FilterList) {
      return isDistinctPrefixFilterIncludedInFilterList((FilterList) filter);
    }
    return false;
  }

  private void checkIndexPartitionIdCount(Connection conn, String tableName, String cdcName)
    throws Exception {
    // The number of partitions will be the number of non-empty salt buckets on the data table
    int saltBuckets = getNonEmptySaltBucketCount(conn, tableName);
    // Verify that we can use retrieve partition ids
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT PARTITION_ID() FROM " + cdcName + " ORDER BY PARTITION_ID()");
    String[] partitionId = new String[saltBuckets];
    int[] countPerPartition = new int[saltBuckets];
    int partitionIndex = 0;
    assertTrue(rs.next());
    partitionId[partitionIndex] = rs.getString(1);
    countPerPartition[partitionIndex]++;
    LOGGER.info("PARTITION_ID[" + partitionIndex + "] = " + partitionId[partitionIndex]);
    while (rs.next()) {
      if (!partitionId[partitionIndex].equals(rs.getString(1))) {
        partitionIndex++;
        partitionId[partitionIndex] = rs.getString(1);
        LOGGER.info("PARTITION_ID[" + partitionIndex + "] = " + partitionId[partitionIndex]);
      }
      countPerPartition[partitionIndex]++;
    }
    // Verify that the number of partitions equals to the number of table regions. In this case,
    // it equals to the number of salt buckets
    assertEquals(saltBuckets, partitionIndex + 1);

    rs = conn.createStatement().executeQuery("SELECT DISTINCT PARTITION_ID() FROM " + cdcName);
    assertTrue(rs.next());
    partitionIndex = 0;
    partitionId[partitionIndex] = rs.getString(1);
    int rowCount = 1;
    while (rs.next()) {
      if (!partitionId[partitionIndex].equals(rs.getString(1))) {
        partitionIndex++;
        partitionId[partitionIndex] = rs.getString(1);
        LOGGER.info("PARTITION_ID[" + partitionIndex + "] = " + partitionId[partitionIndex]);
      }
      rowCount++;
    }
    // Verify that the number of partitions equals to the number of table regions. In this case,
    // it equals to the number of salt buckets
    assertEquals(saltBuckets, partitionIndex + 1);
    // Verified that we only got distinct partition ids
    assertEquals(saltBuckets, rowCount);
    // Verify that DistinctPrefixFilter is used to efficiently retrieve partition ids
    assertTrue(isDistinctPrefixFilterIncluded(((PhoenixResultSet) rs).getContext().getScan()));

    // Verify that we can access data table mutations by partition id
    PreparedStatement statement = conn.prepareStatement(getCDCQuery(cdcName, partitionId));
    statement.setTimestamp(1, new Timestamp(1000));
    statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
    rs = statement.executeQuery();
    rowCount = 0;
    while (rs.next()) {
      rowCount++;
      String id = rs.getString(1);
      int count = rs.getInt(2);
      boolean found = false;
      for (int i = 0; i < saltBuckets; i++) {
        if (partitionId[i].equals(id) && count == countPerPartition[i]) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
    // Verify that partition id based queries are row key prefix queries
    ResultIterator resultIterator = ((PhoenixResultSet) rs).getUnderlyingIterator();
    assertTrue(resultIterator instanceof RowKeyOrderedAggregateResultIterator);
    assertEquals(saltBuckets, rowCount);
  }

  private static String getCDCQuery(String cdcName, String[] partitionId) {
    StringBuilder query = new StringBuilder("SELECT PARTITION_ID(), Count(*) from ");
    query.append(cdcName);
    query.append(" WHERE PARTITION_ID() IN (");
    for (int i = 0; i < partitionId.length - 1; i++) {
      query.append("'");
      query.append(partitionId[i]);
      query.append("',");
    }
    query.append("'");
    query.append(partitionId[partitionId.length - 1]);
    query.append("')");
    query.append(" AND PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() < ?");
    query.append(" Group By PARTITION_ID()");
    return query.toString();
  }

  private static String addPartitionInList(Connection conn, String cdcName, String query)
    throws SQLException {
    ResultSet rs =
      conn.createStatement().executeQuery("SELECT DISTINCT PARTITION_ID() FROM " + cdcName);
    List<String> partitionIds = new ArrayList<>();
    while (rs.next()) {
      partitionIds.add(rs.getString(1));
    }
    StringBuilder builder = new StringBuilder(query);
    builder.append(" WHERE PARTITION_ID() IN (");
    boolean initialized = false;
    for (String partitionId : partitionIds) {
      if (!initialized) {
        builder.append("'");
        initialized = true;
      } else {
        builder.append(",'");
      }
      builder.append(partitionId);
      builder.append("'");
    }
    builder.append(")");
    return builder.toString();
  }

  private static PreparedStatement getCDCQueryPreparedStatement(Connection conn, String cdcName,
    String query, long minTimestamp, long maxTimestamp) throws SQLException {
    StringBuilder builder = new StringBuilder(addPartitionInList(conn, cdcName, query));
    builder.append(" AND PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() < ?");
    PreparedStatement statement = conn.prepareStatement(builder.toString());
    statement.setTimestamp(1, new Timestamp(minTimestamp));
    statement.setTimestamp(2, new Timestamp(maxTimestamp));
    return statement;
  }

  @Test
  public void testSelectCDC() throws Exception {
    String cdcName, cdc_sql;
    String schemaName = getSchemaName();
    String tableName = getTableOrViewName(schemaName);
    String datatableName = tableName;
    try (Connection conn = newConnection()) {
      createTable(conn,
        "CREATE TABLE  " + tableName + " (" + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
          + "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, B.vb INTEGER, "
          + "CONSTRAINT PK PRIMARY KEY " + (multitenant ? "(TENANT_ID, k) " : "(k)") + ")",
        encodingScheme, multitenant, tableSaltBuckets, false, null);
      if (forView) {
        String viewName = getTableOrViewName(schemaName);
        createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
          encodingScheme);
        tableName = viewName;
      }
      cdcName = getCDCName();
      cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
      createCDC(conn, cdc_sql, encodingScheme);
    }

    String tenantId = multitenant ? "1000" : null;
    String[] tenantids = { tenantId };
    if (multitenant) {
      tenantids = new String[] { tenantId, "2000" };
    }

    long startTS = System.currentTimeMillis();
    List<ChangeRow> changes = generateChanges(startTS, tenantids, tableName, null, COMMIT_SUCCESS);
    long currentTime = System.currentTimeMillis();
    long endTS = changes.get(changes.size() - 1).getTimestamp() + 1;
    if (endTS > currentTime) {
      Thread.sleep(endTS - currentTime);
    }

    String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
    try (Connection conn = newConnection(tenantId)) {
      // For debug: uncomment to see the exact results logged to console.
      dumpCDCResults(conn, cdcName, new TreeMap<String, String>() {
        {
          put("K", "INTEGER");
        }
      }, addPartitionInList(conn, cdcFullName,
        "SELECT /*+ CDC_INCLUDE(PRE, POST) */ PHOENIX_ROW_TIMESTAMP(), K," + "\"CDC JSON\" FROM "
          + cdcFullName));

      // Existence of an CDC index hint shouldn't cause the regular query path to fail.
      // Run the same query with a CDC index hit and without it and make sure we get the same
      // result from both
      String uncovered_sql = "SELECT " + " /*+ INDEX(" + tableName + " "
        + CDCUtil.getCDCIndexName(cdcName) + ") */ k, v1 FROM " + tableName;
      try (ResultSet rs = conn.createStatement().executeQuery(uncovered_sql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(201, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(300, rs.getInt(2));
        assertFalse(rs.next());
      }
      uncovered_sql = "SELECT " + "  k, v1 FROM " + tableName;
      try (ResultSet rs = conn.createStatement().executeQuery(uncovered_sql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(201, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(300, rs.getInt(2));
        assertFalse(rs.next());
      }

      Map<String, String> dataColumns = new TreeMap<String, String>() {
        {
          put("V1", "INTEGER");
          put("V2", "INTEGER");
          put("B.VB", "INTEGER");
        }
      };
      verifyChangesViaSCN(tenantId,
        getCDCQueryPreparedStatement(conn, cdcFullName,
          "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + cdcFullName, startTS, endTS).executeQuery(),
        datatableName, dataColumns, changes, CHANGE_IMG);
      verifyChangesViaSCN(tenantId,
        getCDCQueryPreparedStatement(conn, cdcFullName,
          "SELECT /*+ CDC_INCLUDE(CHANGE) */ PHOENIX_ROW_TIMESTAMP(), K," + "\"CDC JSON\" FROM "
            + cdcFullName,
          startTS, endTS).executeQuery(),
        datatableName, dataColumns, changes, CHANGE_IMG);
      verifyChangesViaSCN(tenantId,
        getCDCQueryPreparedStatement(conn, cdcFullName,
          "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName, startTS, endTS)
            .executeQuery(),
        datatableName, dataColumns, changes, PRE_POST_IMG);
      verifyChangesViaSCN(tenantId, getCDCQueryPreparedStatement(conn, cdcFullName,
        "SELECT * FROM " + cdcFullName, startTS, endTS).executeQuery(), datatableName, dataColumns,
        changes, new HashSet<>());

      HashMap<String, int[]> testQueries = new HashMap<String, int[]>() {
        {
          put(
            "SELECT 'dummy', k, \"CDC JSON\" FROM " + cdcFullName
              + " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC, K ASC",
            new int[] { 1, 2, 3, 1, 1, 1, 1, 2, 1, 1, 1, 1 });
          put("SELECT PHOENIX_ROW_TIMESTAMP(), k, \"CDC JSON\" FROM " + cdcFullName
            + " ORDER BY k ASC", new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3 });
          put("SELECT PHOENIX_ROW_TIMESTAMP(), k, \"CDC JSON\" FROM " + cdcFullName
            + " ORDER BY k DESC", new int[] { 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1 });
        }
      };
      Map<String, String> dummyChange = new HashMap() {
        {
          put(CDC_EVENT_TYPE, "dummy");
        }
      };

      for (Map.Entry<String, int[]> testQuery : testQueries.entrySet()) {
        try (ResultSet rs = conn.createStatement().executeQuery(testQuery.getKey())) {
          for (int i = 0; i < testQuery.getValue().length; ++i) {
            int k = testQuery.getValue()[i];
            assertEquals(true, rs.next());
            assertEquals("Index: " + i + " for query: " + testQuery.getKey(), k, rs.getInt(2));
            Map<String, Object> change = mapper.reader(HashMap.class).readValue(rs.getString(3));
            change.put(CDC_EVENT_TYPE, "dummy");
            // Verify that we are getting nothing but the event type as we specified
            // no change scopes.
            assertEquals(dummyChange, change);
          }
          assertEquals(false, rs.next());
        }
      }
      cdcIndexShouldNotBeUsedForDataTableQueries(conn, tableName, cdcName);
    }
  }

  @Test
  public void testSelectGeneric() throws Exception {
    String cdcName, cdc_sql;
    String schemaName = getSchemaName();
    String tableName = getTableOrViewName(schemaName);
    String datatableName = tableName;
    Map<String, String> pkColumns = new TreeMap<String, String>() {
      {
        put("K1", "INTEGER");
        put("K2", "INTEGER");
      }
    };
    Map<String, String> dataColumns = new TreeMap<String, String>() {
      {
        put("V1", "INTEGER");
        put("V2", "VARCHAR");
        put("V3", "CHAR");
        put("V4", "DOUBLE");
        put("V5", "DATE");
        put("V6", "TIME");
        put("V7", "TIMESTAMP");
        put("V8", "VARBINARY");
        put("V9", "BINARY");
        put("V10", "VARCHAR ARRAY");
        put("V11", "JSON");
      }
    };
    try (Connection conn = newConnection()) {
      createTable(conn, tableName, pkColumns, dataColumns, multitenant, encodingScheme,
        tableSaltBuckets, false, null);
      if (forView) {
        String viewName = caseSensitiveNames
          ? SchemaUtil.getTableName(schemaName,
            SchemaUtil.getEscapedArgument(generateUniqueName().toLowerCase()))
          : SchemaUtil.getTableName(schemaName, generateUniqueName());
        createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
          encodingScheme);
        tableName = viewName;
      }
      cdcName = getCDCName();
      cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName + " INCLUDE (change)";
      createCDC(conn, cdc_sql, encodingScheme);
    }

    String tenantId = multitenant ? "1000" : null;
    String[] tenantids = { tenantId };
    if (multitenant) {
      tenantids = new String[] { tenantId, "2000" };
    }

    long startTS = System.currentTimeMillis();
    Map<String, List<Set<ChangeRow>>> allBatches = new HashMap<>(tenantids.length);
    for (String tid : tenantids) {
      allBatches.put(tid, generateMutations(tenantId, startTS, pkColumns, dataColumns, 20, 5));
      applyMutations(COMMIT_SUCCESS, schemaName, tableName, datatableName, tid, allBatches.get(tid),
        cdcName);
    }

    String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
    try (Connection conn = newConnection(tenantId)) {
      // For debug: uncomment to see the exact results logged to console.
      dumpCDCResults(conn, cdcName, pkColumns, addPartitionInList(conn, cdcFullName,
        "SELECT /*+ CDC_INCLUDE(PRE, CHANGE) */ * FROM " + cdcFullName));

      List<ChangeRow> changes = new ArrayList<>();
      for (Set<ChangeRow> batch : allBatches.get(tenantId)) {
        changes.addAll(batch);
      }
      long currentTime = System.currentTimeMillis();
      long nextTime = changes.get(changes.size() - 1).getTimestamp() + 1;
      if (nextTime > currentTime) {
        Thread.sleep(nextTime - currentTime);
      }
      verifyChangesViaSCN(tenantId,
        conn.createStatement()
          .executeQuery(addPartitionInList(conn, cdcFullName, "SELECT * FROM " + cdcFullName)),
        datatableName, dataColumns, changes, CHANGE_IMG);
      verifyChangesViaSCN(tenantId,
        conn.createStatement()
          .executeQuery(addPartitionInList(conn, cdcFullName,
            "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + cdcFullName)),
        datatableName, dataColumns, changes, CHANGE_IMG);
      verifyChangesViaSCN(tenantId,
        conn.createStatement()
          .executeQuery(addPartitionInList(conn, cdcFullName,
            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName)),
        datatableName, dataColumns, changes, PRE_POST_IMG);
      verifyChangesViaSCN(tenantId,
        conn.createStatement()
          .executeQuery(addPartitionInList(conn, cdcFullName,
            "SELECT /*+ CDC_INCLUDE(CHANGE, PRE, POST) */ * FROM " + cdcFullName)),
        datatableName, dataColumns, changes, ALL_IMG);
      cdcIndexShouldNotBeUsedForDataTableQueries(conn, tableName, cdcName);
      checkIndexPartitionIdCount(conn, tableName, cdcFullName);
    }
  }

  private int getNonEmptySaltBucketCount(Connection conn, String tableName) throws SQLException {
    if (tableSaltBuckets == null) {
      return 1;
    }
    Set<Integer> nonEmptySaltBuckets = Sets.newHashSet();
    TableName hTableName = TableName.valueOf(tableName);
    ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
    Table table = cqs.getTable(hTableName.getName());
    Scan scan = new Scan();
    scan.setRaw(true);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Result result;
      while ((result = scanner.next()) != null) {
        byte[] row = result.getRow();
        Integer bucketId = ((int) row[0]);
        nonEmptySaltBuckets.add(bucketId);
      }
    } catch (Exception e) {
      throw new SQLException(e);
    }
    return nonEmptySaltBuckets.size();
  }

  private void _testSelectCDCImmutable(PTable.ImmutableStorageScheme immutableStorageScheme)
    throws Exception {
    String cdcName, cdc_sql;
    String schemaName = getSchemaName();
    String tableName = getTableOrViewName(schemaName);
    String datatableName = tableName;
    try (Connection conn = newConnection()) {
      createTable(conn,
        "CREATE TABLE  " + tableName + " (" + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
          + "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, CONSTRAINT PK PRIMARY KEY "
          + (multitenant ? "(TENANT_ID, k) " : "(k)") + ")",
        encodingScheme, multitenant, tableSaltBuckets, true, immutableStorageScheme);
      if (forView) {
        String viewName = getTableOrViewName(schemaName);

        createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
          encodingScheme);
        tableName = viewName;
      }
      cdcName = getCDCName();
      cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;

      createCDC(conn, cdc_sql, encodingScheme);
    }

    String tenantId = multitenant ? "1000" : null;
    String[] tenantids = { tenantId };
    if (multitenant) {
      tenantids = new String[] { tenantId, "2000" };
    }

    long startTS = System.currentTimeMillis();
    List<ChangeRow> changes = generateChangesImmutableTable(startTS, tenantids, schemaName,
      tableName, datatableName, COMMIT_SUCCESS, cdcName);

    String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
    Map<String, String> dataColumns = new TreeMap<String, String>() {
      {
        put("V1", "INTEGER");
        put("V2", "INTEGER");
      }
    };

    try (Connection conn = newConnection(tenantId)) {
      // For debug: uncomment to see the exact results logged to console.
      dumpCDCResults(conn, cdcName, new TreeMap<String, String>() {
        {
          put("K", "INTEGER");
        }
      }, "SELECT /*+ CDC_INCLUDE(PRE, POST) */ PHOENIX_ROW_TIMESTAMP(), K," + "\"CDC JSON\" FROM "
        + cdcFullName);
      verifyChangesViaSCN(tenantId,
        conn.createStatement()
          .executeQuery("SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName
            + " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC"),
        datatableName, dataColumns, changes, PRE_POST_IMG);
      verifyChangesViaSCN(tenantId,
        conn.createStatement()
          .executeQuery("SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + cdcFullName
            + " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC"),
        datatableName, dataColumns, changes, CHANGE_IMG);
      verifyChangesViaSCN(tenantId,
        conn.createStatement().executeQuery(
          "SELECT /*+ CDC_INCLUDE(CHANGE) */ " + "PHOENIX_ROW_TIMESTAMP(), K, \"CDC JSON\" FROM "
            + cdcFullName + " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC"),
        datatableName, dataColumns, changes, CHANGE_IMG);
      cdcIndexShouldNotBeUsedForDataTableQueries(conn, tableName, cdcName);
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
  public void testSelectWithTimeRange() throws Exception {
    String cdcName, cdc_sql;
    String schemaName = getSchemaName();
    String tableName = getTableOrViewName(schemaName);
    String datatableName = tableName;
    Map<String, String> pkColumns = new TreeMap<String, String>() {
      {
        put("K1", "INTEGER");
      }
    };
    Map<String, String> dataColumns = new TreeMap<String, String>() {
      {
        put("V1", "INTEGER");
      }
    };
    try (Connection conn = newConnection()) {
      createTable(conn, tableName, pkColumns, dataColumns, multitenant, encodingScheme,
        tableSaltBuckets, false, null);
      if (forView) {
        String viewName = getTableOrViewName(schemaName);
        createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
          encodingScheme);
        tableName = viewName;
      }
      cdcName = getCDCName();
      cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName + " INCLUDE (change)";
      createCDC(conn, cdc_sql, encodingScheme);
      cdcIndexShouldNotBeUsedForDataTableQueries(conn, tableName, cdcName);
    }

    String tenantId = multitenant ? "1000" : null;
    String[] tenantids = { tenantId };
    if (multitenant) {
      tenantids = new String[] { tenantId, "2000" };
    }

    long startTS = System.currentTimeMillis();
    Map<String, List<Set<ChangeRow>>> allBatches = new HashMap<>(tenantids.length);
    for (String tid : tenantids) {
      allBatches.put(tid, generateMutations(tenantId, startTS, pkColumns, dataColumns, 20, 5));
      applyMutations(COMMIT_SUCCESS, schemaName, tableName, datatableName, tid, allBatches.get(tid),
        cdcName);
    }

    String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
    try (Connection conn = newConnection(tenantId)) {
      // For debug: uncomment to see the exact results logged to console.
      dumpCDCResults(conn, cdcName, pkColumns,
        "SELECT /*+ CDC_INCLUDE(PRE, CHANGE) */ * FROM " + cdcFullName);

      List<ChangeRow> changes = new ArrayList<>();
      for (Set<ChangeRow> batch : allBatches.get(tenantId)) {
        changes.addAll(batch);
      }
      List<Long> uniqueTimestamps = new ArrayList<>();
      Integer lastDeletionTSpos = null;
      for (ChangeRow change : changes) {
        if (
          uniqueTimestamps.size() == 0
            || uniqueTimestamps.get(uniqueTimestamps.size() - 1) != change.changeTS
        ) {
          uniqueTimestamps.add(change.changeTS);
        }
        if (change.change == null) {
          lastDeletionTSpos = uniqueTimestamps.size() - 1;
        }
      }
      Random rand = new Random();
      int randMinTSpos = rand.nextInt(lastDeletionTSpos - 1);
      int randMaxTSpos =
        randMinTSpos + 1 + rand.nextInt(uniqueTimestamps.size() - (randMinTSpos + 1));
      verifyChangesViaSCN(tenantId, conn, cdcFullName, pkColumns, datatableName, dataColumns,
        changes, 0, System.currentTimeMillis());
      verifyChangesViaSCN(tenantId, conn, cdcFullName, pkColumns, datatableName, dataColumns,
        changes, randMinTSpos, randMaxTSpos);
      verifyChangesViaSCN(tenantId, conn, cdcFullName, pkColumns, datatableName, dataColumns,
        changes, randMinTSpos, lastDeletionTSpos);
      verifyChangesViaSCN(tenantId, conn, cdcFullName, pkColumns, datatableName, dataColumns,
        changes, lastDeletionTSpos, randMaxTSpos);
    }
  }

  @Test
  public void testSelectCDCWithDDL() throws Exception {
    String schemaName = getSchemaName();
    String tableName = getTableOrViewName(schemaName);
    String datatableName = tableName;
    String cdcName, cdc_sql;
    try (Connection conn = newConnection()) {
      createTable(conn,
        "CREATE TABLE  " + tableName + " (" + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
          + "k INTEGER NOT NULL, v0 INTEGER, v1 INTEGER, v1v2 INTEGER, v2 INTEGER, B.vb INTEGER, "
          + "v3 INTEGER, CONSTRAINT PK PRIMARY KEY " + (multitenant ? "(TENANT_ID, k) " : "(k)")
          + ")",
        encodingScheme, multitenant, tableSaltBuckets, false, null);
      if (forView) {
        String viewName = getTableOrViewName(schemaName);
        createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
          encodingScheme);
        tableName = viewName;
      }

      cdcName = getCDCName();
      cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
      createCDC(conn, cdc_sql, encodingScheme);
      conn.createStatement().execute("ALTER TABLE " + datatableName + " DROP COLUMN v0");
    }

    String tenantId = multitenant ? "1000" : null;
    String[] tenantids = { tenantId };
    if (multitenant) {
      tenantids = new String[] { tenantId, "2000" };
    }

    long startTS = System.currentTimeMillis();
    List<ChangeRow> changes =
      generateChanges(startTS, tenantids, tableName, datatableName, COMMIT_SUCCESS);

    Map<String, String> dataColumns = new TreeMap<String, String>() {
      {
        put("V0", "INTEGER");
        put("V1", "INTEGER");
        put("V1V2", "INTEGER");
        put("V2", "INTEGER");
        put("B.VB", "INTEGER");
        put("V3", "INTEGER");
      }
    };
    try (Connection conn = newConnection(tenantId)) {
      verifyChangesViaSCN(tenantId,
        conn.createStatement().executeQuery("SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM "
          + SchemaUtil.getTableName(schemaName, cdcName) + " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC"),
        datatableName, dataColumns, changes, CHANGE_IMG);
      cdcIndexShouldNotBeUsedForDataTableQueries(conn, tableName, cdcName);
    }
  }

  @Test
  public void testSelectCDCFailDataTableUpdate() throws Exception {
    String schemaName = getSchemaName();
    String tableName = getTableOrViewName(schemaName);
    String cdcName, cdc_sql;
    try (Connection conn = newConnection()) {
      createTable(conn,
        "CREATE TABLE  " + tableName + " (" + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
          + "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, B.vb INTEGER, "
          + "CONSTRAINT PK PRIMARY KEY " + (multitenant ? "(TENANT_ID, k) " : "(k)") + ")",
        encodingScheme, multitenant, tableSaltBuckets, false, null);
      if (forView) {
        String viewName = getTableOrViewName(schemaName);
        createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
          encodingScheme);
        tableName = viewName;
      }
      cdcName = getCDCName();
      cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
      createCDC(conn, cdc_sql, encodingScheme);
      cdcIndexShouldNotBeUsedForDataTableQueries(conn, tableName, cdcName);
    }

    String tenantId = multitenant ? "1000" : null;
    String[] tenantids = { tenantId };
    if (multitenant) {
      tenantids = new String[] { tenantId, "2000" };
    }

    long startTS = System.currentTimeMillis();
    generateChanges(startTS, tenantids, tableName, null, COMMIT_FAILURE_EXPECTED);

    try (Connection conn = newConnection(tenantId)) {
      ResultSet rs = conn.createStatement()
        .executeQuery("SELECT * FROM " + SchemaUtil.getTableName(schemaName, cdcName));
      assertEquals(false, rs.next());

    }
  }

  @Test
  public void testCDCIndexBuildAndVerification() throws Exception {
    String schemaName = withSchemaName ? generateUniqueName() : null;
    String tableName = generateUniqueName();
    String tableFullName = SchemaUtil.getTableName(schemaName, tableName);
    String cdcName, cdc_sql;
    try (Connection conn = newConnection()) {
      // Create a table and add some rows
      createTable(conn,
        "CREATE TABLE  " + tableFullName + " ("
          + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
          + "k INTEGER NOT NULL, v1 INTEGER, v1v2 INTEGER, v2 INTEGER, B.vb INTEGER, "
          + "v3 INTEGER, CONSTRAINT PK PRIMARY KEY " + (multitenant ? "(TENANT_ID, k) " : "(k)")
          + ")",
        encodingScheme, multitenant, tableSaltBuckets, false, null);
      if (forView) {
        String viewName = generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        createTable(conn, "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + tableFullName,
          encodingScheme);
        tableName = viewName;
        tableFullName = viewFullName;
      }

      String tenantId = multitenant ? "1000" : null;
      String[] tenantids = { tenantId };
      if (multitenant) {
        tenantids = new String[] { tenantId, "2000" };
      }

      long startTS = System.currentTimeMillis();
      List<ChangeRow> changes =
        generateChanges(startTS, tenantids, tableFullName, tableFullName, COMMIT_SUCCESS, null, 0);
      // Make sure the timestamp of the mutations are not in the future
      long currentTime = System.currentTimeMillis();
      long nextTime = changes.get(changes.size() - 1).getTimestamp() + 1;
      if (nextTime > currentTime) {
        Thread.sleep(nextTime - currentTime);
      }
      // Create a CDC table
      cdcName = generateUniqueName();
      cdc_sql = "CREATE CDC " + cdcName + " ON " + tableFullName;
      createCDC(conn, cdc_sql, encodingScheme);
      // Check CDC index is active but empty
      String indexTableFullName =
        SchemaUtil.getTableName(schemaName, CDCUtil.getCDCIndexName(cdcName));
      PTable indexTable = ((PhoenixConnection) conn).getTableNoCache(indexTableFullName);
      assertEquals(indexTable.getIndexState(), PIndexState.ACTIVE);
      TestUtil.assertRawRowCount(conn, TableName.valueOf(indexTable.getPhysicalName().getString()),
        0);
      // Rebuild the index and verify that it is still empty
      IndexToolIT.runIndexTool(false, schemaName, tableName, CDCUtil.getCDCIndexName(cdcName));
      TestUtil.assertRawRowCount(conn, TableName.valueOf(indexTable.getPhysicalName().getString()),
        0);
      // Add more rows
      startTS = System.currentTimeMillis();
      changes =
        generateChanges(startTS, tenantids, tableFullName, tableFullName, COMMIT_SUCCESS, null, 1);
      currentTime = System.currentTimeMillis();
      // Advance time by the max lookback age. This will cause all rows to expire
      nextTime = changes.get(changes.size() - 1).getTimestamp() + 1;
      if (nextTime > currentTime) {
        Thread.sleep(nextTime - currentTime);
      }
      // Verify CDC index verification pass
      IndexTool indexTool = IndexToolIT.runIndexTool(false, schemaName, tableName,
        CDCUtil.getCDCIndexName(cdcName), null, 0, IndexTool.IndexVerifyType.ONLY);
      assertEquals(0,
        indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
      assertEquals(0, indexTool.getJob().getCounters()
        .findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
      assertEquals(0, indexTool.getJob().getCounters()
        .findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
      assertEquals(0, indexTool.getJob().getCounters()
        .findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
      assertEquals(0, indexTool.getJob().getCounters()
        .findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
      assertEquals(0, indexTool.getJob().getCounters()
        .findCounter(BEFORE_REBUILD_OLD_INDEX_ROW_COUNT).getValue());
      assertEquals(0, indexTool.getJob().getCounters()
        .findCounter(BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT).getValue());

    }
  }

  @Test
  public void testCDCIndexTTLEqualsToMaxLookbackAge() throws Exception {
    if (forView) {
      // Except for views
      return;
    }
    String schemaName = withSchemaName ? generateUniqueName() : null;
    String tableName = generateUniqueName();
    String tableFullName = SchemaUtil.getTableName(schemaName, tableName);
    String cdcName, cdc_sql;
    try (Connection conn = newConnection()) {
      // Create a table
      createTable(conn,
        "CREATE TABLE  " + tableFullName + " ("
          + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
          + "k INTEGER NOT NULL, v1 INTEGER, v1v2 INTEGER, v2 INTEGER, B.vb INTEGER, "
          + "v3 INTEGER, CONSTRAINT PK PRIMARY KEY " + (multitenant ? "(TENANT_ID, k) " : "(k)")
          + ")",
        encodingScheme, multitenant, tableSaltBuckets, false, null);
      if (forView) {
        String viewName = generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        createTable(conn, "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + tableFullName,
          encodingScheme);
        tableName = viewName;
        tableFullName = viewFullName;
      }

      String tenantId = multitenant ? "1000" : null;
      String[] tenantids = { tenantId };
      if (multitenant) {
        tenantids = new String[] { tenantId, "2000" };
      }

      // Create a CDC table
      cdcName = generateUniqueName();
      cdc_sql = "CREATE CDC " + cdcName + " ON " + tableFullName;
      createCDC(conn, cdc_sql, encodingScheme);
      // Add rows
      long startTS = System.currentTimeMillis();
      List<ChangeRow> changes =
        generateChanges(startTS, tenantids, tableFullName, tableFullName, COMMIT_SUCCESS, null, 0);
      String indexTableFullName =
        SchemaUtil.getTableName(schemaName, CDCUtil.getCDCIndexName(cdcName));
      PTable indexTable = ((PhoenixConnection) conn).getTableNoCache(indexTableFullName);
      String indexTablePhysicalName = indexTable.getPhysicalName().toString();
      int expectedRawRowCount =
        TestUtil.getRawRowCount(conn, TableName.valueOf(indexTablePhysicalName));
      long currentTime = System.currentTimeMillis();
      // Advance time by the max lookback age. This will cause all rows to expire
      long nextTime = changes.get(changes.size() - 1).getTimestamp() + MAX_LOOKBACK_AGE * 1000 + 1;
      if (nextTime > currentTime) {
        Thread.sleep(nextTime - currentTime);
      }
      // Major compact the CDC index. This will remove all expired rows
      TestUtil.doMajorCompaction(conn, indexTablePhysicalName);
      // Check CDC index is empty
      TestUtil.assertRawRowCount(conn, TableName.valueOf(indexTablePhysicalName), 0);
      // Rebuild the index and verify that it is still empty
      IndexToolIT.runIndexTool(false, schemaName, tableName, CDCUtil.getCDCIndexName(cdcName));
      TestUtil.assertRawRowCount(conn, TableName.valueOf(indexTablePhysicalName), 0);
      // This time we test we only keep the row versions within the max lookback window
      startTS = System.currentTimeMillis();
      // Add the first set of rows
      changes =
        generateChanges(startTS, tenantids, tableFullName, tableFullName, COMMIT_SUCCESS, null, 0);
      // Advance time by the max lookback age. This will cause the first set of rows to expire
      startTS = changes.get(changes.size() - 1).getTimestamp() + MAX_LOOKBACK_AGE * 1000 + 1;
      // Add another set of changes
      changes =
        generateChanges(startTS, tenantids, tableFullName, tableFullName, COMMIT_SUCCESS, null, 10);
      nextTime = changes.get(changes.size() - 1).getTimestamp() + 1;
      // Major compact the CDC index which remove all expired rows which is
      // the first set of rows
      currentTime = System.currentTimeMillis();
      if (nextTime > currentTime) {
        Thread.sleep(nextTime - currentTime);
      }
      TestUtil.doMajorCompaction(conn, indexTablePhysicalName);
      // Check the CDC index has the first set of rows
      TestUtil.assertRawRowCount(conn, TableName.valueOf(indexTablePhysicalName),
        expectedRawRowCount);
      // Rebuild the index and verify that it still have the same number of rows
      IndexToolIT.runIndexTool(false, schemaName, tableName, CDCUtil.getCDCIndexName(cdcName));
      TestUtil.assertRawRowCount(conn, TableName.valueOf(indexTablePhysicalName),
        expectedRawRowCount);

    }
  }

  /**
   * Test for PHOENIX-7524: CDC Query with OFFSET can throw IndexOutOfBoundsException
   *
   * Scenario: CDC query with OFFSET that exceeds available rows
   * Expected: Query should return empty result set
   */
  @Test
  public void testCDCQueryWithOffsetExceedingRows() throws Exception {
    String schemaName = getSchemaName();
    String tableName = getTableOrViewName(schemaName);
    String cdcName = getCDCName();

    try (Connection conn = newConnection()) {
      createTable(conn,
        "CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER)",
        encodingScheme, multitenant, tableSaltBuckets, false, null);
      createCDC(conn, "CREATE CDC " + cdcName + " ON " + tableName, encodingScheme);

      conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES ('a', 1, 2)");
      conn.commit();

      // This query should return empty result, not throw exception
      // OFFSET 1 with only 1 row means we skip the only row
      // IMPORTANT: Using PHOENIX_ROW_TIMESTAMP() > CURRENT_TIME() without subtraction
      // This means the WHERE clause filters out ALL rows (no row has timestamp in the future)
      // So we're trying to OFFSET past 0 rows
      String query =
        "SELECT * FROM " + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() > CURRENT_TIME() LIMIT 1 OFFSET 1";

      ResultSet rs = conn.createStatement().executeQuery(query);

      // Should return no rows without throwing exception
      assertFalse("Expected no rows when OFFSET exceeds available data", rs.next());
    }
  }

  private String getSchemaName() {
    return withSchemaName
      ? caseSensitiveNames
        ? SchemaUtil.getEscapedArgument(generateUniqueName().toLowerCase())
        : generateUniqueName()
      : null;
  }

  private String getTableOrViewName(String schemaName) {
    return caseSensitiveNames
      ? SchemaUtil.getTableName(schemaName,
        SchemaUtil.getEscapedArgument(generateUniqueName().toLowerCase()))
      : SchemaUtil.getTableName(schemaName, generateUniqueName());
  }

  private String getCDCName() {
    return caseSensitiveNames
      ? SchemaUtil.getEscapedArgument(generateUniqueName().toLowerCase())
      : generateUniqueName();
  }
}
