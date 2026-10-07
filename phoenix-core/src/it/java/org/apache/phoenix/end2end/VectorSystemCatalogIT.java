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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_CENTROID_GENERATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_DIMENSION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_DISTANCE_METRIC;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_HNSW_ALPHA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_HNSW_EF_CONSTRUCTION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_HNSW_M;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_INDEX_ALGORITHM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_LISTS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_IVF_SAMPLE_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_PQ_SEGMENTS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VECTOR_QUANTIZATION_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verifies that vector index metadata columns added to SYSTEM.CATALOG are exposed consistently
 * across raw catalog rows, JDBC DatabaseMetaData, and PTable definitions.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorSystemCatalogIT extends ParallelStatsDisabledIT {

  private static final List<String> VECTOR_METADATA_COLUMNS =
    Arrays.asList(VECTOR_INDEX_ALGORITHM, VECTOR_DISTANCE_METRIC, VECTOR_DIMENSION,
      VECTOR_IVF_LISTS, VECTOR_IVF_SAMPLE_SIZE, VECTOR_CENTROID_GENERATION, VECTOR_HNSW_M,
      VECTOR_HNSW_EF_CONSTRUCTION, VECTOR_HNSW_ALPHA, VECTOR_QUANTIZATION_TYPE, VECTOR_PQ_SEGMENTS);

  private static final Map<String, PDataType<?>> EXPECTED_COLUMN_TYPES = new HashMap<>();
  static {
    EXPECTED_COLUMN_TYPES.put(VECTOR_INDEX_ALGORITHM, PVarchar.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_DISTANCE_METRIC, PVarchar.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_DIMENSION, PInteger.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_IVF_LISTS, PInteger.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_IVF_SAMPLE_SIZE, PInteger.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_CENTROID_GENERATION, PLong.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_HNSW_M, PInteger.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_HNSW_EF_CONSTRUCTION, PInteger.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_HNSW_ALPHA, PDouble.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_QUANTIZATION_TYPE, PVarchar.INSTANCE);
    EXPECTED_COLUMN_TYPES.put(VECTOR_PQ_SEGMENTS, PInteger.INSTANCE);
  }

  /**
   * Verifies column existence, SQL data types, and nullability across SYSTEM.CATALOG rows,
   * DatabaseMetaData, and PTable schema representations.
   */
  @Test
  public void testAllVectorIndexMetadataColumnsExistWithCorrectSchema() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      // Query raw SYSTEM.CATALOG rows directly
      String query = "SELECT " + COLUMN_NAME + ", " + DATA_TYPE + ", " + NULLABLE + " FROM "
        + SYSTEM_CATALOG_NAME + " WHERE " + TABLE_NAME + " = ? AND " + COLUMN_NAME + " = ?";
      for (String colName : VECTOR_METADATA_COLUMNS) {
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
          stmt.setString(1, SYSTEM_CATALOG_TABLE);
          stmt.setString(2, colName);
          try (ResultSet rs = stmt.executeQuery()) {
            assertTrue("Column " + colName + " must exist in SYSTEM.CATALOG", rs.next());
            assertEquals(colName, rs.getString(COLUMN_NAME));
            PDataType<?> expectedType = EXPECTED_COLUMN_TYPES.get(colName);
            assertEquals("Data type mismatch for " + colName, expectedType.getSqlType(),
              rs.getInt(DATA_TYPE));
            assertEquals("Column " + colName + " should be nullable",
              DatabaseMetaData.columnNullable, rs.getInt(NULLABLE));
          }
        }
      }

      // Inspect JDBC DatabaseMetaData column definitions
      DatabaseMetaData dbmd = conn.getMetaData();
      for (String colName : VECTOR_METADATA_COLUMNS) {
        try (ResultSet rs =
          dbmd.getColumns(null, SYSTEM_CATALOG_SCHEMA, SYSTEM_CATALOG_TABLE, colName)) {
          assertTrue("DatabaseMetaData must return column " + colName, rs.next());
          assertEquals(colName, rs.getString("COLUMN_NAME"));
          PDataType<?> expectedType = EXPECTED_COLUMN_TYPES.get(colName);
          assertEquals(expectedType.getSqlType(), rs.getInt("DATA_TYPE"));
        }
      }

      // Validate uncached PTable column definitions
      PTable syscatTable = pconn.getTableNoCache(SYSTEM_CATALOG_NAME);
      assertNotNull("SYSTEM.CATALOG PTable must exist", syscatTable);
      for (String colName : VECTOR_METADATA_COLUMNS) {
        PColumn pcol = syscatTable.getColumnForColumnName(colName);
        assertNotNull("PColumn for " + colName + " must exist in PTable", pcol);
        assertEquals(EXPECTED_COLUMN_TYPES.get(colName), pcol.getDataType());
        assertTrue("Column " + colName + " must be nullable in PTable", pcol.isNullable());
      }
    }
  }
}
