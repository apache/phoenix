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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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

@RunWith(Parameterized.class)
public class AlterAddCascadeIndexIT extends ParallelStatsDisabledIT {

    public static final String SYNTAX_ERROR = "Syntax error";
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private static Connection conn;
    private Properties prop;
    private boolean isViewIndex;
    private String phoenixObjectName;
    private String indexNames;
    private final String tableDDLOptions;
    String fullIndexNameOne, fullIndexNameTwo;


    public AlterAddCascadeIndexIT(boolean isViewIndex, boolean mutable) {
        this.isViewIndex = isViewIndex;
        StringBuilder optionBuilder = new StringBuilder();
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true");
        }
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
        String fullTableName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + fullTableName + " (\n" +
                "      state CHAR(2) NOT NULL,\n" +
                "      city VARCHAR NOT NULL,\n" +
                "      population BIGINT,\n" +
                "      CONSTRAINT my_pk PRIMARY KEY (state, city)) " + tableDDLOptions);

        if(isViewIndex) {
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
        String query = "ALTER " +(isViewIndex? "VIEW " : "TABLE ") + phoenixObjectName + " ADD new_column_3 VARCHAR(64) CASCADE INDEX ALL";
        conn.createStatement().execute(query);
        PreparedStatement ps;
        if(isViewIndex) {
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
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 5, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 5, columnIndexArray, false);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray, false);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 4, columnIndexArray, false);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 4, columnIndexArray, false);
        }

    }

    // Test with CASCADE INDEX <index_name>
    @Test
    public void testAlterDBOAddCascadeIndex() throws Exception {
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType())};
        ColumnInfo [] columnIndexArray =  {new ColumnInfo("0:new_column_1", PDecimal.INSTANCE.getSqlType())};

        String query = "ALTER " + (isViewIndex? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 FLOAT CASCADE INDEX " + indexNames.split(",")[0];
        conn.createStatement().execute(query);
        if(isViewIndex) {
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
        ColumnInfo [] columnIndexArray =  {new ColumnInfo("0:new_column_1", PDecimal.INSTANCE.getSqlType()),
                new ColumnInfo("0:new_column_2", PDecimal.INSTANCE.getSqlType())};
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column_1 FLOAT, new_column_2 DOUBLE CASCADE INDEX " + indexNames.split(",")[0];
        conn.createStatement().execute(query);
        if(isViewIndex) {
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
        ColumnInfo [] columnIndexArray = {new ColumnInfo("0:new_column_1", PDecimal.INSTANCE.getSqlType())};
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX " + indexNames;
        conn.createStatement().execute(query);
        if(isViewIndex) {
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
        ColumnInfo [] columIndexArray =  {new ColumnInfo("0:new_column_1", PDecimal.INSTANCE.getSqlType()),
                new ColumnInfo("0:new_column_2", PDecimal.INSTANCE.getSqlType())};

        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 FLOAT, new_column_2 DOUBLE CASCADE INDEX " + indexNames;
        conn.createStatement().execute(query);
        if(isViewIndex) {
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

        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ") + phoenixObjectName + " ADD new_column VARCHAR ALL";
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SYNTAX_ERROR));
        }

        query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column VARCHAR CASCADE " + indexNames.split(",")[0];
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SYNTAX_ERROR));
        }

        query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column VARCHAR INDEX " + indexNames.split(",")[0];
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SYNTAX_ERROR));
        }

        query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX INCORRECT_NAME";
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SQLExceptionCode.INCORRECT_INDEX_NAME.getMessage()));
        }

        String localIndex = generateUniqueName();
        String createLocalIndex = "CREATE LOCAL INDEX " + localIndex + " ON "
                + phoenixObjectName + "(avg_fam_size) INCLUDE (population)";
        if(!isViewIndex) {
            createLocalIndex = "CREATE LOCAL INDEX " + localIndex + " ON "
                    + phoenixObjectName + "(state, population)";

        }
        conn.createStatement().execute(createLocalIndex);
        query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX "+localIndex;
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SQLExceptionCode
                    .NOT_SUPPORTED_CASCADE_FEATURE_LOCAL_INDEX.getMessage()));
        }

        query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
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
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX INCORRECT_NAME, "+ indexNames;
        try {
            conn.createStatement().execute(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SQLExceptionCode.INCORRECT_INDEX_NAME.getMessage()));
        }
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType())};
        ColumnInfo [] columnIndexArray =  {new ColumnInfo("0:new_column_1", PDecimal.INSTANCE.getSqlType())};
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 5, columnArray, true);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 4, columnIndexArray, true);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 4, columnIndexArray, true);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 3, columnArray, true);
            assertDBODefinition(conn, fullIndexNameOne, PTableType.INDEX, 3, columnIndexArray, true);
            assertDBODefinition(conn, fullIndexNameTwo, PTableType.INDEX, 3, columnIndexArray, true);
        }

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

}