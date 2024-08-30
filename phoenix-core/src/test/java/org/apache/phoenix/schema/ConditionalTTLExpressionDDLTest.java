package org.apache.phoenix.schema;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class ConditionalTTLExpressionDDLTest extends BaseConnectionlessQueryTest {

    private static void assertConditonTTL(Connection conn, String tableName, String ttlExpr) throws SQLException {
        PTable table = conn.unwrap(PhoenixConnection.class).getTable(tableName);
        TTLExpression expected = new ConditionTTLExpression(ttlExpr);
        assertEquals(expected, table.getTTL());
    }

    @Test
    public void testBasicExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String quotedValue = "k1 > 5 AND col1 < ''zzzzzz''";
        String ttl = "k1 > 5 AND col1 < 'zzzzzz'";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, quotedValue);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testNotBooleanExpr() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 + 100";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testWrongArgumentValue() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "k1 = ''abc''";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
        }
    }

    @Test
    public void testNullExpression() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar, col2 date " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "col1 is NULL AND col2 < CURRENT_DATE() + 30000";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testBooleanColumn() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, expired BOOLEAN " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "expired";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testNot() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, expired BOOLEAN " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "NOT expired";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws SQLException {
        String ddlTemplate = "create table %s (k1 bigint not null, k2 bigint not null, col1 varchar " +
                "constraint pk primary key (k1,k2 desc)) TTL = '%s'";
        String ttl = "PHOENIX_ROW_TIMESTAMP() < CURRENT_DATE() - 100";
        String tableName = generateUniqueName();
        String ddl = String.format(ddlTemplate, tableName, ttl);
        try (Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            conn.createStatement().execute(ddl);
            assertConditonTTL(conn, tableName, ttl);
        }
    }
}
