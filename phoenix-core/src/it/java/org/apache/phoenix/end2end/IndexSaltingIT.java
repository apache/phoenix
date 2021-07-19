package org.apache.phoenix.end2end;

import org.apache.phoenix.end2end.index.BaseLocalIndexIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexSaltingIT extends BaseLocalIndexIT {
    public IndexSaltingIT(boolean isNamespaceMapped) {
        super(isNamespaceMapped);
    }

    private String createTable = "CREATE TABLE %s  (ID INTEGER NOT NULL PRIMARY KEY, " +
            "NAME VARCHAR, ZIP INTEGER) ";
    private String createIndexTable = "CREATE INDEX %s ON %s (ZIP) INCLUDE(NAME) ";
    private String selectQuery = "SELECT SALT_BUCKETS FROM SYSTEM.CATALOG " +
            "WHERE TABLE_NAME = '%s' AND TABLE_TYPE = 'i'";

    @Test
    public void testSecondaryIndexWithSaltingOptionOnBaseTable() throws Exception {
        String dataTableName = generateUniqueName();
        String indexTableName = generateUniqueName();
        String indexTableWithSaltingName = generateUniqueName();
        String localIndexTableWithSaltingName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String stmString1 = String.format(createTable +  "SALT_BUCKETS=10", dataTableName);
            conn.createStatement().execute(stmString1);
            conn.commit();

            // create index table without salting option should not be salted
            String stmtString2 = String.format(createIndexTable, indexTableName, dataTableName);
            conn.createStatement().execute(stmtString2);
            conn.commit();

            String stmtString3 = String.format(selectQuery, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(stmtString3);
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertFalse(rs.next());

            // testing index with diff salting value
            String stmtString4 = String.format(createIndexTable + "SALT_BUCKETS=5",
                    indexTableWithSaltingName, dataTableName);
            conn.createStatement().execute(stmtString4);
            conn.commit();

            String stmtString5 = String.format(selectQuery, indexTableWithSaltingName);
            rs = conn.createStatement().executeQuery(stmtString5);
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertFalse(rs.next());

            // local index should not have a salting option.
            try {
                String stmtString6 = String.format(
                        "CREATE LOCAL INDEX %s ON %s (ZIP) INCLUDE(NAME) SALT_BUCKETS=5",
                        localIndexTableWithSaltingName, dataTableName);
                conn.createStatement().execute(stmtString6);
                conn.commit();
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SALT_LOCAL_INDEX.getErrorCode(),
                        e.getErrorCode());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSecondaryIndexWithoutSaltingOptionOnBaseTable() throws Exception {
        String dataTableName = generateUniqueName();
        String indexTableName = generateUniqueName();
        String indexTableWithSaltingName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String stmString1 = String.format(createTable, dataTableName);
            conn.createStatement().execute(stmString1);
            conn.commit();

            String stmtString2 = String.format(createIndexTable, indexTableName, dataTableName);
            conn.createStatement().execute(stmtString2);
            conn.commit();

            String stmtString3 = String.format(selectQuery, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(stmtString3);
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertFalse(rs.next());

            String stmtString4 = String.format(createIndexTable + "SALT_BUCKETS=5",
                    indexTableWithSaltingName, dataTableName);
            conn.createStatement().execute(stmtString4);
            conn.commit();

            String stmtString5 = String.format(selectQuery, indexTableWithSaltingName);
            rs = conn.createStatement().executeQuery(stmtString5);
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

}
