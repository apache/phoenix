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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class AlterAddCascadeIndexIT extends ParallelStatsDisabledIT {

    public static final String SYNTAX_ERROR = "Syntax error";
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private static Connection conn;
    private Properties prop;
    private boolean isViewScenario, mutable;
    private String phoenixObjectName;
    private String indexNames;
    private final String tableDDLOptions;
    String fullIndexNameOne, fullIndexNameTwo, fullTableName;


    public AlterAddCascadeIndexIT(boolean isViewScenario, boolean mutable) {
        this.isViewScenario = isViewScenario;
        StringBuilder optionBuilder = new StringBuilder("COLUMN_ENCODED_BYTES=0");
        if (!mutable) {

            optionBuilder.append(", IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'");
        }
        this.mutable = mutable;
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameters(name="AlterAddCascadeIndexIT_isViewIndex={0},mutable={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { true, true},
                { true, false},
                { false, true},
                { false, false},
        });
    }

    @Before
    public void setup() throws SQLException {
        prop = new Properties();
        conn = DriverManager.getConnection(getUrl(), prop);
        conn.setAutoCommit(true);
        String schemaName = "S_"+generateUniqueName();
        String indexNameOne = "I_"+generateUniqueName();
        String indexNameTwo = "I_"+generateUniqueName();
        String tableName = "T_"+generateUniqueName();
        String viewName = "V_"+generateUniqueName();
        String fullViewName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
        fullTableName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + fullTableName + " (\n" +
                "      state CHAR(2) NOT NULL,\n" +
                "      city VARCHAR NOT NULL,\n" +
                "      population BIGINT,\n" +
                "      CONSTRAINT my_pk PRIMARY KEY (state, city)) " + tableDDLOptions);

        if(isViewScenario) {
            conn.createStatement().execute("CREATE VIEW IF NOT EXISTS " + fullViewName +
                    " (city_area INTEGER, avg_fam_size INTEGER) AS " +
                    "SELECT * FROM "+fullTableName+" WHERE state = 'CA'");

            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexNameOne + " ON " +
                    fullViewName+" (city_area) INCLUDE (population)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexNameTwo + " ON " +
                    fullViewName+" (avg_fam_size) INCLUDE (population)");
            phoenixObjectName = fullViewName;
        } else {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexNameOne + " ON " +
                    fullTableName+" (population)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexNameTwo + " ON " +
                    fullTableName+" (state, population)");
            phoenixObjectName = fullTableName;
        }
        fullIndexNameOne = SchemaUtil.getQualifiedTableName(schemaName, indexNameOne);
        fullIndexNameTwo = SchemaUtil.getQualifiedTableName(schemaName, indexNameTwo);
        indexNames = indexNameOne +", "+indexNameTwo;
    }


    // Test with ALTER TABLE/VIEW CASCADE INDEX ALL with upserting into new column
    @Test
    public void testAlterDBOAddCascadeIndexAllUpsert() throws Exception {
        String query = "ALTER " +(isViewScenario ? "VIEW " : "TABLE ") + phoenixObjectName + " ADD new_column_3 VARCHAR(64) CASCADE INDEX ALL";
        conn.createStatement().execute(query);
        PreparedStatement ps;
        if(isViewScenario) {
            ps = conn.prepareStatement("UPSERT INTO " + phoenixObjectName +
                    "(state,city,population,city_area,avg_fam_size,new_column_3) " +
                    "VALUES('CA','Santa Barbara',912332,1300,4,'test_column')");
        } else {
            ps = conn.prepareStatement("UPSERT INTO " + phoenixObjectName +
                    "(state,city,population,new_column_3) " +
                    "VALUES('CA','Santa Barbara',912332,'test_column')");
        }
        ps.executeUpdate();
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_3", PVarchar.INSTANCE.getSqlType(), 64)};
        ColumnInfo [] columnIndexArray =  {new ColumnInfo("0:new_column_3", PVarchar.INSTANCE.getSqlType(), 64)};
        if(isViewScenario) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 5, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 5, columnIndexArray, false);
            if (mutable) {
                assertNumberOfHBaseCells( "_IDX_"+fullTableName,6);
            }
            else {
                assertNumberOfHBaseCells( "_IDX_"+fullTableName,6);
            }
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 4, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 4, columnIndexArray, false);
            assertNumberOfHBaseCells( fullIndexNameOne,2);
            assertNumberOfHBaseCells( fullIndexNameOne,2);

        }

    }

    // Test with ALTER TABLE/VIEW CASCADE INDEX ALL with no indexes
    @Test
    public void testAlterDBOAddCascadeIndexAll_noIndexes() throws Exception {
        String schemaName = "S_"+generateUniqueName();
        String tableName = "T_"+generateUniqueName();
        String viewName = "V_"+generateUniqueName();

        String fullViewName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
        String fullTableName = SchemaUtil.getQualifiedTableName(schemaName, tableName);

        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + fullTableName + " (\n" +
                "      state CHAR(2) NOT NULL,\n" +
                "      city VARCHAR NOT NULL,\n" +
                "      population BIGINT,\n" +
                "      CONSTRAINT my_pk PRIMARY KEY (state, city)) " + tableDDLOptions);

        if(isViewScenario) {
            conn.createStatement().execute("CREATE VIEW IF NOT EXISTS " + fullViewName
                    + " (city_area INTEGER, avg_fam_size INTEGER) AS " + "SELECT * FROM "
                    + fullTableName + " WHERE state = 'CA'");
        }

        String query = "ALTER " +(isViewScenario ? "VIEW " : "TABLE ") + (isViewScenario ? fullViewName : fullTableName)
                + " ADD new_column_3 VARCHAR(64) CASCADE INDEX ALL";
        conn.createStatement().execute(query);

        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_3", PVarchar.INSTANCE.getSqlType(), 64)};
        if(isViewScenario) {
            assertDBODefinition(conn, fullViewName, PTableType.VIEW, 6, columnArray, false);
        } else {
            assertDBODefinition(conn, fullTableName, PTableType.TABLE, 4, columnArray, false);
        }

    }

    // Test with CASCADE INDEX <index_name>
    @Test
    public void testAlterDBOAddCascadeIndex() throws Exception {
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType())};
        ColumnInfo [] columnIndexArray =  {new ColumnInfo("0:new_column_1", PFloat.INSTANCE.getSqlType())};

        String query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 FLOAT CASCADE INDEX " + indexNames.split(",")[0];
        conn.createStatement().execute(query);
        if(isViewScenario) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 5, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 4, columnIndexArray, true);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 4, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 3, columnIndexArray, true);
        }
    }

    // Test with CASCADE INDEX <index_name>
    @Test
    public void testAlterDBOAddCascadeTwoColsOneIndex() throws Exception {
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType()),
                new ColumnInfo("new_column_2", PDouble.INSTANCE.getSqlType())};
        ColumnInfo [] columnIndexArray =  {new ColumnInfo("0:new_column_1", PFloat.INSTANCE.getSqlType()),
                new ColumnInfo("0:new_column_2", PDouble.INSTANCE.getSqlType())};
        String query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column_1 FLOAT, new_column_2 DOUBLE CASCADE INDEX " + indexNames.split(",")[0];
        conn.createStatement().execute(query);
        if(isViewScenario) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 7, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 6, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 4, columnIndexArray, true);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 5, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 5, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 3, columnIndexArray, true);
        }

    }

    // Test with CASCADE INDEX <index_name>, <index_name>
    @Test
    public void testAlterDBOAddCascadeIndexes() throws Exception {
        ColumnInfo [] columnArray = {new ColumnInfo("new_column_1", PDouble.INSTANCE.getSqlType())};
        ColumnInfo [] columnIndexArray = {new ColumnInfo("0:new_column_1", PDouble.INSTANCE.getSqlType())};
        String query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX " + indexNames;
        conn.createStatement().execute(query);
        if(isViewScenario) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 5, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 5, columnIndexArray, false);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 4, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 4, columnIndexArray, false);
        }
    }

    // Test with CASCADE INDEX <index_name>, <index_name>
    @Test
    public void testAlterDBOAddCascadeTwoColsTwoIndexes() throws Exception {
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType()),
                new ColumnInfo("new_column_2", PDouble.INSTANCE.getSqlType())};
        ColumnInfo [] columIndexArray =  {new ColumnInfo("0:new_column_1", PFloat.INSTANCE.getSqlType()),
                new ColumnInfo("0:new_column_2", PDouble.INSTANCE.getSqlType())};

        String query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 FLOAT, new_column_2 DOUBLE CASCADE INDEX " + indexNames;
        conn.createStatement().execute(query);
        if(isViewScenario) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 7, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 6, columIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 6, columIndexArray, false);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 5, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 5, columIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 5, columIndexArray, false);
        }

    }

    // Exception for invalid grammar
    @Test
    public void testAlterDBOException() throws SQLException {

        String query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ") + phoenixObjectName + " ADD new_column VARCHAR ALL";
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SYNTAX_ERROR));
        }

        query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column VARCHAR CASCADE " + indexNames.split(",")[0];
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SYNTAX_ERROR));
        }

        query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column VARCHAR INDEX " + indexNames.split(",")[0];
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SYNTAX_ERROR));
        }

        query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX INCORRECT_NAME";
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SQLExceptionCode.INCORRECT_INDEX_NAME.getMessage()));
        }

        String localIndex = generateUniqueName();
        String createLocalIndex = "CREATE LOCAL INDEX " + localIndex + " ON "
                + phoenixObjectName + "(avg_fam_size) INCLUDE (population)";
        if(!isViewScenario) {
            createLocalIndex = "CREATE LOCAL INDEX " + localIndex + " ON "
                    + phoenixObjectName + "(state, population)";

        }
        conn.createStatement().execute(createLocalIndex);
        query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX "+localIndex;
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SQLExceptionCode
                    .NOT_SUPPORTED_CASCADE_FEATURE_LOCAL_INDEX.getMessage()));
        }

        query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_2 DOUBLE CASCADE INDEX "+localIndex + "," + indexNames;
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SQLExceptionCode
                    .NOT_SUPPORTED_CASCADE_FEATURE_LOCAL_INDEX.getMessage()));

        }
    }

    // Exception for incorrect index name
    @Test
    public void testAlterDBOIncorrectIndexNameCombination() throws Exception {
        String query = "ALTER " + (isViewScenario ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX INCORRECT_NAME, "+ indexNames;
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SQLExceptionCode.INCORRECT_INDEX_NAME.getMessage()));
        }
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType())};
        ColumnInfo [] columnIndexArray =  {new ColumnInfo("0:new_column_1", PDecimal.INSTANCE.getSqlType())};
        if(isViewScenario) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 5, columnArray, true);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 4, columnIndexArray, true);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 4, columnIndexArray, true);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 3, columnArray, true);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 3, columnIndexArray, true);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 3, columnIndexArray, true);
        }

    }

    @Test
    public void testAlterTableCascadeIndexAllBigInt() throws Exception {
        String schemaName = "S_"+generateUniqueName();
        String tableName = "T_"+generateUniqueName();
        String indexNameThree = "I_"+generateUniqueName();
        String fullTableName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String fullIndexNameThree = SchemaUtil.getQualifiedTableName(schemaName, indexNameThree);
        String createTableQuery = "CREATE TABLE " + fullTableName + " (mykey INTEGER NOT NULL PRIMARY KEY, col1 BIGINT)";
        String createIndexQuery = "CREATE INDEX " + indexNameThree + " ON " + fullTableName + " (col1)";

        conn.createStatement().execute(createTableQuery);
        conn.createStatement().execute(createIndexQuery);
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + fullTableName + "(mykey, col1) VALUES(1, 2)");
        ps.executeUpdate();
        conn.commit();

        String alterTableQuery = "ALTER TABLE " + fullTableName + " ADD IF NOT EXISTS col3 BIGINT CASCADE INDEX ALL";
        conn.createStatement().execute(alterTableQuery);

        PreparedStatement pss = conn.prepareStatement("UPSERT INTO " + fullTableName + "(mykey, col1, col3) VALUES(6, 7, 8)");
        pss.executeUpdate();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + fullTableName);
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getLong(2));
        rs.next();
        assertEquals(6, rs.getInt(1));
        assertEquals(7L, rs.getLong(2));
        assertEquals(8L, rs.getLong(3));

        rs = conn.createStatement().executeQuery("SELECT * FROM " + fullIndexNameThree);
        rs.next();
        assertEquals(2L, rs.getLong(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals(7L, rs.getLong(1));
        assertEquals(6, rs.getInt(2));
        assertEquals(8L, rs.getLong(3));

        ColumnInfo[] columnArray = { new ColumnInfo("col3", PLong.INSTANCE.getSqlType()) };
        ColumnInfo[] columnIndexArray = { new ColumnInfo("0:col3", PLong.INSTANCE.getSqlType()) };

        assertDBODefinition(conn, fullTableName, PTableType.TABLE, 3, columnArray, false);
        assertDBODefinition(conn, fullIndexNameThree, PTableType.INDEX, 3, columnIndexArray, false);
    }

    private void assertDBODefinition(Connection conn, String phoenixObjectName, PTableType pTableType, int baseColumnCount,  ColumnInfo [] columnInfo, boolean fail)
            throws Exception {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(phoenixObjectName);
        String tableName = SchemaUtil.getTableNameFromFullName(phoenixObjectName);
        PreparedStatement p = conn.prepareStatement("SELECT * FROM SYSTEM.CATALOG "
                + "WHERE TABLE_SCHEM=? AND TABLE_NAME=? AND TABLE_TYPE=?");
        p.setString(1, schemaName.toUpperCase());
        p.setString(2, tableName.toUpperCase());
        p.setString(3, pTableType.getSerializedValue());
        ResultSet rs = p.executeQuery();
        assertTrue(rs.next());
        assertEquals("Mismatch in ColumnCount", baseColumnCount, rs.getInt("COLUMN_COUNT"));
        p = conn.prepareStatement("SELECT * FROM SYSTEM.CATALOG "
                + "WHERE TABLE_SCHEM=? AND TABLE_NAME=? AND COLUMN_NAME=? AND DATA_TYPE=?");
        p.setString(1, schemaName.toUpperCase());
        p.setString(2, tableName.toUpperCase());

        int iPos = baseColumnCount - columnInfo.length + 1;
        for(ColumnInfo column: columnInfo) {
            p.setString(3, column.getDisplayName().toUpperCase());
            p.setInt(4, column.getSqlType());
            rs = p.executeQuery();
            if(!fail) {
                assertTrue(rs.next());
                assertEquals(iPos, rs.getInt("ORDINAL_POSITION"));
                iPos++;
            } else {
                assertFalse(rs.next());
            }
        }
        rs.close();
        p.close();
    }

    public void assertNumberOfHBaseCells(String tableName, int expected) {
        try {
            ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
            Table table = cqs.getTable(Bytes.toBytes(tableName));

            Scan scan = new Scan();
            scan.setRaw(true);
            scan.readAllVersions();
            int count=0;
            ResultScanner scanner = table.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                count+=result.listCells().size();
            }
            assertEquals(expected, count);
        } catch (Exception e) {
            //ignore
        }

    }

}