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

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Date;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test IT is testing additional RVC OFFSET feature in more comprehensive cases, such as diff
 * combination of types, nullable, table, and view scenarios. However, it will take a while to
 * run. Instead of running for all builds, we should have a diff build strategy for the Phoenix.
 * A set of lightweight ITs should run every build, and another fully covered ITs build for daily or
 * weekly.
 */
@Category(ParallelStatsDisabledTest.class)
public class RowValueConstructorOffsetOptionalIT extends ParallelStatsDisabledIT {
    private final long TS = System.currentTimeMillis();
    private final String[] stringDataSet = new String[] {"aaa", "aab", "aac"};
    private final String[] nullableStringDataSet = new String[] {null, "aaa", "bbb"};
    private final Double[] doubleDataSet = new Double[] {1.0,1.1,1.11,};
    private final Short[] shortDataSet = new Short[] {1, 11, 110};
    private final Integer[] intDataSet = new Integer[] {1,11,110,};
    private final Long[] longDataSet =  new Long[] {
            new Long(1),
            new Long(11),
            new Long(111)
    };
    private final Date[] dateDataSet = new Date[] {
        new Date(TS - 1000),
        new Date(TS),
        new Date(TS + 1000)
    };
    private final BigDecimal[] decimalDataSet = new BigDecimal[] {
            new BigDecimal("0.01"),
            new BigDecimal("0.02"),
            new BigDecimal("0.03")
    };
    private final String[] types = new String[] {
            "CHAR", "VARCHAR", "TIMESTAMP", "DATE", "DECIMAL",
            "DOUBLE","SMALLINT","INTEGER","BIGINT"
    };

    private static Connection conn = null;
    private String PREFIX = "000";
    private int EXPECTED_ROWS_PER_TENANT = 10;
    private int NUMBER_OF_TENANT_VIEWS = 5;

    @BeforeClass
    public static synchronized void init() throws SQLException {
        conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
    }

    @AfterClass
    public static synchronized void cleanup() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Ignore
    @Test
    public void testSinglePkColumnTestSet() throws SQLException {
        for (String type: types) {
            testSinglePkColumnTypes(type);
        }
    }

    @Ignore
    @Test
    public void testMultiPkColumnsTestSet() throws SQLException {
        for (String type1: types) {
            for (String type2: types) {
                testMultiPkColumnTypes(type1, type2);
            }
        }
    }

    @Ignore
    @Test
    public void testMultiPkColumnsForNullableTestSet() throws SQLException {
        for (String type1: types) {
            testMultiPkColumnForNullableTypes(type1, "VARCHAR");
        }
    }

    @Ignore
    @Test
    public void testMultiTenantViewTests() throws SQLException {
        String baseTable = generateUniqueName();
        createMultiTenantBaseTable(baseTable);
        String ddl = "CREATE VIEW %s (PK1 INTEGER NOT NULL PRIMARY KEY)" +
                " AS SELECT * FROM " + baseTable + " WHERE PREFIX = '" + PREFIX + "'";
        for (int i = 0; i < NUMBER_OF_TENANT_VIEWS; i++) {
            createAndupsertDataToTenantView(ddl, EXPECTED_ROWS_PER_TENANT);
        }

        verifyMultiTenantTable(baseTable);
    }

    @Ignore
    @Test
    public void testMultiTenantViewOnGlobalViewTests() throws SQLException {
        String baseTable = generateUniqueName();
        String globalView = generateUniqueName();
        createMultiTenantBaseTable(baseTable);
        createGlobalView(baseTable, globalView);
        String ddl = "CREATE VIEW %s (PK1 INTEGER NOT NULL PRIMARY KEY)" +
                " AS SELECT * FROM " + globalView;
        for (int i = 0; i < NUMBER_OF_TENANT_VIEWS; i++) {
            createAndupsertDataToTenantView(ddl, EXPECTED_ROWS_PER_TENANT);
        }

        verifyMultiTenantTable(baseTable);
    }

    private void createGlobalView(String baseTable, String globalView) throws SQLException {
        String ddlBaseTable = "CREATE VIEW " + globalView +
                " AS SELECT * FROM " + baseTable + " WHERE PREFIX = '%s'";

        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format(ddlBaseTable, PREFIX));
        }
    }

    private void verifyMultiTenantTable(String baseTable) throws SQLException {
        String tenantId = "0";
        String sql = "SELECT * FROM " + baseTable + " LIMIT "
                + EXPECTED_ROWS_PER_TENANT + " OFFSET (TENANT_ID,PREFIX)=(?,?)";
        for (int i = 0; i <= NUMBER_OF_TENANT_VIEWS; i++) {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, tenantId);
                statement.setString(2, PREFIX);
                ResultSet rs = statement.executeQuery();

                if (i == NUMBER_OF_TENANT_VIEWS) {
                    assertFalse(rs.next());
                } else {
                    int numberOfRows = 0;
                    while (rs.next()) {
                        if (numberOfRows == 0) {
                            tenantId = rs.getString(1);
                        } else {
                            assertEquals(tenantId, rs.getString(1));
                        }
                        numberOfRows++;
                    }
                    assertEquals(EXPECTED_ROWS_PER_TENANT, numberOfRows);
                }
            }
        }
    }

    private void createAndupsertDataToTenantView(String ddl, int numberRows) throws SQLException {
        String tenantView = generateUniqueName();
        String tenantId = generateUniqueName();

        String upsertSql = "UPSERT INTO " + tenantView + " (PK1) VALUES (?)";

        try (Connection tenantConn = getTenantConnection(tenantId)) {
            try (Statement statement = tenantConn.createStatement()){
                statement.execute(String.format(ddl, tenantView));
            }
            for (int i = 0; i < numberRows; i++) {
                try (PreparedStatement preparedStatement = tenantConn.prepareStatement(upsertSql)) {
                    preparedStatement.setInt(1, i);
                    preparedStatement.execute();
                }
            }
            tenantConn.commit();
        }
    }

    private void createMultiTenantBaseTable(String baseTableName) throws SQLException {
        String ddlBaseTable = "CREATE TABLE " + baseTableName + " (" +
                        "TENANT_ID CHAR(10) NOT NULL, PREFIX CHAR(3) NOT NULL" +
                        "   CONSTRAINT PK PRIMARY KEY" +
                        "       (TENANT_ID,PREFIX)" +
                        ") MULTI_TENANT=TRUE";

        try (Statement statement = conn.createStatement()) {
            statement.execute(ddlBaseTable);
        }
    }

    private Connection getTenantConnection(String tenantId) throws SQLException {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }


    private void testMultiPkColumnForNullableTypes(String type, String nullableType) throws SQLException {
        Object[][] data1  = new Object[2][];
        Object[][] data2  = new Object[2][];
        data1[0] = getData(type);
        data1[1] = getNullableData(nullableType);
        data2[0] = data1[1];
        data2[1] = data1[0];

        String ascTableName1 = generateUniqueName();
        String descTableName1 = generateUniqueName();
        String ascTableName2 = generateUniqueName();
        String descTableName2 = generateUniqueName();
        String ddlAsc1 = "CREATE TABLE " + ascTableName1 +
                " (PK1 %s NOT NULL,PK2 %s,CONSTRAINT PK PRIMARY KEY (PK1, PK2))";
        String ddlDesc1 = "CREATE TABLE " + descTableName1 +
                " (PK1 %s NOT NULL,PK2 %s,CONSTRAINT PK PRIMARY KEY (PK1 DESC, PK2 DESC))";
        String ddlAsc2 = "CREATE TABLE " + ascTableName2 +
                " (PK1 %s,PK2 %s NOT NULL,CONSTRAINT PK PRIMARY KEY (PK1, PK2))";
        String ddlDesc2 = "CREATE TABLE " + descTableName2 +
                " (PK1 %s,PK2 %s NOT NULL,CONSTRAINT PK PRIMARY KEY (PK1 DESC, PK2 DESC))";


        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format(ddlAsc1, getTypeString(type), nullableType));
            statement.execute(String.format(ddlDesc1, getTypeString(type), nullableType));
            statement.execute(String.format(ddlAsc2, nullableType, getTypeString(type)));
            statement.execute(String.format(ddlDesc2, nullableType, getTypeString(type)));
        }
        upsertRowsToMultiPkColumnTable(data1, ascTableName1, descTableName1, type, nullableType);
        verifyMultiPkColumnResults(data1, ascTableName1, descTableName1, type, nullableType);
        upsertRowsToMultiPkColumnTable(data2, ascTableName2, descTableName2, nullableType, type);
        verifyMultiPkColumnResults(data2, ascTableName2, descTableName2, nullableType, type);
    }

    private Object[] getNullableData(String type) {
        Object[] data = null;
        switch (type) {
            case "VARCHAR":
                data = nullableStringDataSet;
                break;
        }
        return data;
    }

    private void testSinglePkColumnTypes(String type) throws SQLException {
        String ascTableName = generateUniqueName();
        String descTableName = generateUniqueName();
        String ddlAscTmp = "CREATE TABLE " + ascTableName + " (PK1 %s PRIMARY KEY)";
        String ddlDescTmp = "CREATE TABLE " + descTableName + " (PK1 %s PRIMARY KEY DESC)";

        String ddlAsc = String.format(ddlAscTmp, getTypeString(type));
        String ddlDesc = String.format(ddlDescTmp, getTypeString(type));

        Object[] data = getData(type);

        try (Statement statement = conn.createStatement()) {
            statement.execute(ddlAsc);
            statement.execute(ddlDesc);
        }

        upsertRowsIntoSinglePkColumnTable(data, ascTableName, descTableName, type);

        verifySinglePkColumnResults(data, ascTableName,descTableName, type);
    }

    private Object[] getData(String type) {
        Object[] data = null;
        switch (type) {
            case "CHAR":
            case "VARCHAR":
                data = stringDataSet;
                break;
            case "TIMESTAMP":
            case "DATE":
                data = dateDataSet;
                break;
            case "DECIMAL":
                data = decimalDataSet;
                break;
            case "DOUBLE":
                data = doubleDataSet;
                break;
            case "SMALLINT":
                data = shortDataSet;
                break;
            case "INTEGER":
                data = intDataSet;
                break;
            case "BIGINT":
                data = longDataSet;
                break;
        }
        return data;
    }

    private void upsertRowsIntoSinglePkColumnTable (Object[] data, String ascTableName,
                                        String descTableName, String type) throws SQLException {
        String upsertAscTableSql = "UPSERT INTO " + ascTableName + " (PK1) VALUES (?)";
        String upsertDescTableSql = "UPSERT INTO " + descTableName + "(PK1) VALUES (?)";
        for (Object val : data) {
            upsertRowIntoSinglePkColumn(upsertAscTableSql, type, val);
            upsertRowIntoSinglePkColumn(upsertDescTableSql, type, val);
        }
    }

    private void upsertRowIntoSinglePkColumn(
            String sql, String type, Object val) throws SQLException {
        try (PreparedStatement pStatement = conn.prepareStatement(sql)) {
            setPreparedStatementValue(pStatement, type, val, 1);
            pStatement.execute();
            conn.commit();
        }
    }

    private void verifySinglePkColumnResults(Object[] data, String ascTableName,
                                         String descTableName, String type) throws SQLException {
        String selectAscTableSql = "SELECT * FROM " + ascTableName + " OFFSET (PK1)=(?)";
        String selectDescTableSql = "SELECT * FROM " + descTableName + " OFFSET (PK1)=(?)";

        verifySinglePkColumnResults(data, selectAscTableSql, type, true);
        verifySinglePkColumnResults(data, selectDescTableSql, type, false);
    }

    private void verifySinglePkColumnResults(Object[] data, String sql, String type, boolean isAsc)
            throws SQLException {
        try (PreparedStatement pStatement = conn.prepareStatement(sql)) {
            setPreparedStatementValue(pStatement, type, data[1], 1);
            ResultSet rs = pStatement.executeQuery();

            assertTrue(rs.next());
            verifyResult(data, rs, type, isAsc, 1);
            assertFalse(rs.next());
        }
    }

    private void verifyResult(Object[] data, ResultSet rs, String type,
                              boolean isAsc, int index) throws SQLException {
        switch (type) {
            case "CHAR":
            case "VARCHAR":
                if (isAsc) {
                    assertEquals(data[2], rs.getString(index));
                } else {
                    assertEquals(data[0], rs.getString(index));
                }
                break;
            case "TIMESTAMP":
                if (isAsc) {
                    assertEquals(data[2], rs.getTimestamp(index));
                } else {
                    assertEquals(data[0], rs.getTimestamp(index));
                }
                break;
            case "DATE":
                if (isAsc) {
                    assertEquals(data[2], rs.getDate(index));
                } else {
                    assertEquals(data[0], rs.getDate(index));
                }
                break;
            case "DECIMAL":
                if (isAsc) {
                    assertEquals(data[2], rs.getBigDecimal(index));
                } else {
                    assertEquals(data[0], rs.getBigDecimal(index));
                }
                break;
            case "DOUBLE":
                if (isAsc) {
                    assertEquals(data[2], rs.getDouble(index));
                } else {
                    assertEquals(data[0], rs.getDouble(index));
                }
                break;
            case "SMALLINT":
                if (isAsc) {
                    assertEquals(data[2], rs.getShort(index));
                } else {
                    assertEquals(data[0], rs.getShort(index));
                }
                break;
            case "INTEGER":
                if (isAsc) {
                    assertEquals(data[2], rs.getInt(index));
                } else {
                    assertEquals(data[0], rs.getInt(index));
                }
                break;
            case "BIGINT":
                if (isAsc) {
                    assertEquals(data[2], rs.getLong(index));
                } else {
                    assertEquals(data[0], rs.getLong(index));
                }
                break;
        }
    }

    private void testMultiPkColumnTypes(String type1, String type2) throws SQLException {
        Object[][] data  = new Object[2][];
        data[0] = getData(type1);
        data[1] = getData(type2);

        String ascTableName = generateUniqueName();
        String descTableName = generateUniqueName();
        String ddlAsc = "CREATE TABLE " + ascTableName +
                " (PK1 %s NOT NULL,PK2 %s NOT NULL,CONSTRAINT PK PRIMARY KEY (PK1, PK2))";
        String ddlDesc = "CREATE TABLE " + descTableName +
                " (PK1 %s NOT NULL,PK2 %s NOT NULL,CONSTRAINT PK PRIMARY KEY (PK1 DESC, PK2 DESC))";

        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format(ddlAsc, getTypeString(type1), getTypeString(type2)));
            statement.execute(String.format(ddlDesc, getTypeString(type1), getTypeString(type2)));
        }

        upsertRowsToMultiPkColumnTable(data, ascTableName, descTableName, type1, type2);
        verifyMultiPkColumnResults(data, ascTableName, descTableName, type1, type2);
    }

    private String getTypeString(String type) {
        if (type.equals("CHAR") || type.equals("VARCHAR")) {
            type = type + "(3)";
        }
        return type;
    }

    private void upsertRowsToMultiPkColumnTable(Object[][] data, String ascTableName,
                        String descTableName, String type1, String type2) throws SQLException {
        String upsertAscTableSql = "UPSERT INTO " + ascTableName + " (PK1,PK2) VALUES (?,?)";
        String upsertDescTableSql = "UPSERT INTO " + descTableName + "(PK1,PK2) VALUES (?,?)";
        for (int i = 0; i < data[0].length; i++) {
            upsertRowIntoMultiPkColumn(upsertAscTableSql, type1, type2, data[0][i], data[1][i]);
            upsertRowIntoMultiPkColumn(upsertDescTableSql, type1, type2, data[0][i], data[1][i]);
        }
    }

    private void setPreparedStatementValue(PreparedStatement pStatement, String type, Object val,
                                           int index) throws SQLException {
        switch (type) {
            case "CHAR":
            case "VARCHAR":
                pStatement.setString(index, (String) val);
                break;
            case "TIMESTAMP":
            case "DATE":
                pStatement.setDate(index, (Date) val);
                break;
            case "DECIMAL":
                pStatement.setBigDecimal(index, (BigDecimal)val);
                break;
            case "DOUBLE":
                pStatement.setDouble(index, (Double) val);
                break;
            case "SMALLINT":
                pStatement.setShort(index, (Short) val);
                break;
            case "INTEGER":
                pStatement.setInt(index, (Integer) val);
                break;
            case "BIGINT":
                pStatement.setLong(index, (Long) val);
                break;
        }
    }

    private void upsertRowIntoMultiPkColumn(String sql, String type1, String type2,
                            Object val1, Object val2) throws SQLException {
        try (PreparedStatement pStatement = conn.prepareStatement(sql)) {
            setPreparedStatementValue(pStatement, type1, val1, 1);
            setPreparedStatementValue(pStatement, type2, val2, 2);
            pStatement.execute();
            conn.commit();
        }
    }

    private void verifyMultiPkColumnResults(Object[][] data, String ascTableName,
                            String descTableName, String type1, String type2) throws SQLException {
        String selectAscTableSql = "SELECT * FROM " + ascTableName + " OFFSET (PK1,PK2)=(?,?)";
        String selectDescTableSql = "SELECT * FROM " + descTableName + " OFFSET (PK1,PK2)=(?,?)";

        verifyMultiPkColumnResults(data, selectAscTableSql, type1, type2, true);
        verifyMultiPkColumnResults(data, selectDescTableSql, type1, type2, false);
    }

    private void verifyMultiPkColumnResults(Object[][] data, String sql,
             String type1, String type2, boolean isAsc) throws SQLException {
        try (PreparedStatement pStatement = conn.prepareStatement(sql)) {
            setPreparedStatementValue(pStatement, type1, data[0][1], 1);
            setPreparedStatementValue(pStatement, type2, data[1][1], 2);
            ResultSet rs = pStatement.executeQuery();

            if (!isAsc && data[0][0] == null) {
                assertFalse(String.format("Type %s and %s should not have expected row", type1, type2),rs.next());
            } else {
                assertTrue(String.format("Type %s and %s should have expected row", type1, type2), rs.next());
                verifyResult(data[0], rs, type1, isAsc, 1);
                verifyResult(data[1], rs, type2, isAsc, 2);
                assertFalse(rs.next());
            }
        }
    }
}

