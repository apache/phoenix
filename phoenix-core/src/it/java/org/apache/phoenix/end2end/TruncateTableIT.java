package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableNotFoundException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class TruncateTableIT extends ParallelStatsDisabledIT {

    @Test
    public void testTruncateTable() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        try {
            String createTableDDL =
                "CREATE TABLE " + tableName + " (pk char(2) not null primary key)";
            conn.createStatement().execute(createTableDDL);
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement(
                "UPSERT INTO " + tableName + " VALUES('a')");
            stmt.execute();
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("TRUNCATE TABLE " + tableName);
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            conn.createStatement().execute("DROP TABLE " + tableName);
            conn.close();
        } catch (SQLException e) {
            fail();
        }
    }

    @Test
    public void testTruncateTableNotExist() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = "nonExistentTable";
        try {
            conn.createStatement().execute("TRUNCATE TABLE " + tableName);
            fail();
        } catch (TableNotFoundException e) {
            return;
        }
    }

    @Test
    public void testTruncateTableNonExistentSchema() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String schemaName = "nonExistentSchema";
        String tableName = generateUniqueName();

        try {
            conn.createStatement()
                .execute("CREATE TABLE " + tableName + " (C1 INTEGER PRIMARY KEY)");
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement(
                "UPSERT INTO " + tableName + " VALUES(1)");
            stmt.execute();
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            conn.createStatement().execute("TRUNCATE TABLE " + schemaName + "." + tableName);
            fail();
        } catch (SQLException e) {
            conn.createStatement().execute("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testTruncateTableWithImplicitSchema() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String createTableWithSchema = "CREATE TABLE " + schemaName + "." + tableName + " (C1 char(2) NOT NULL PRIMARY KEY)";

        try {
            conn.createStatement().execute(createTableWithSchema);
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + schemaName + "." + tableName + " values('a')");
            stmt.execute();
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + schemaName + "." + tableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            stmt = conn.prepareStatement("UPSERT INTO " + schemaName + "." + tableName + " values('b')");
            stmt.execute();
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + schemaName + "." + tableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("TRUNCATE TABLE " + schemaName + "." + tableName);
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + schemaName + "." + tableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("DROP TABLE " + schemaName + "." + tableName);
            conn.close();
        } catch (SQLException e) {
            fail();
        }
    }

    @Test
    public void testTruncateTableWithExplicitSchema() throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));

        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();

        String schemaCreateStmt = "CREATE SCHEMA IF NOT EXISTS " + schemaName;
        String tableCreateStmt = "CREATE TABLE IF NOT EXISTS " + tableName + " (C1 char(2) NOT NULL PRIMARY KEY) SALT_BUCKETS=100";

        try {
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(true);
            conn.createStatement().execute(schemaCreateStmt);
            conn.createStatement().execute("USE " + schemaName);
            conn.createStatement().execute(tableCreateStmt);

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES ('a')");
            stmt.execute();

            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.setAutoCommit(true);
            conn.createStatement().execute("USE " + schemaName);
            stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES ('b')");
            stmt.execute();
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("USE " + schemaName);
            conn.createStatement().execute("TRUNCATE TABLE " + tableName);
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            conn.createStatement().execute("DROP TABLE " + schemaName + "." + tableName);
            conn.close();
        } catch (SQLException e) {
            fail();
        }
    }
}
