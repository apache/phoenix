package org.apache.phoenix.end2end;

import org.apache.phoenix.util.QueryUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

@Category(ParallelStatsDisabledTest.class)
public class SubBinaryFunctionIT extends ParallelStatsDisabledIT {

    @Test
    public void testBinary() throws Exception {
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName + "(" +
                "    id INTEGER NOT NULL,\n" +
                "    BIN_PK BINARY(4) NOT NULL,\n" +
                "    BIN_COL BINARY(8) \n" +
                "    CONSTRAINT pk PRIMARY KEY (id, BIN_PK)" +
                ")");

        byte[] b11 = new byte[] {83, -101, -102, 91};
        byte[] b12 = new byte[] {4, 1, -19, 8, 0, -73, 3, 4};
        byte[] b21 = new byte[] {-1, 1, 20, -28,};
        byte[] b22 = new byte[] {10, 55, 0, 19, -5, -34, 0, 0};
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?, ?)");
        upsertRow(stmt, 1, b11, b12);
        upsertRow(stmt, 2, b21, b22);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, 1, 3), SUBBINARY(BIN_COL, 0, 4) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 0, 3);
        assertSubBinary(b12, rs.getBytes(2), 0, 4);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 0, 3);
        assertSubBinary(b22, rs.getBytes(2), 0, 4);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, 2), SUBBINARY(BIN_COL, 5) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 1, 3);
        assertSubBinary(b12, rs.getBytes(2), 4, 4);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 1, 3);
        assertSubBinary(b22, rs.getBytes(2), 4, 4);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, -3, 2), SUBBINARY(BIN_COL, -6, 3) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 1, 2);
        assertSubBinary(b12, rs.getBytes(2), 2, 3);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 1, 2);
        assertSubBinary(b22, rs.getBytes(2), 2, 3);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, -1), SUBBINARY(BIN_COL, -3) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 3, 1);
        assertSubBinary(b12, rs.getBytes(2), 5, 3);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 3, 1);
        assertSubBinary(b22, rs.getBytes(2), 5, 3);

        PreparedStatement stmt2 = conn.prepareStatement("SELECT id FROM " + tableName + " WHERE SUBBINARY(BIN_COL, 2, 6) = ?");
        stmt2.setBytes(1, new byte[] {55, 0, 19, -5, -34, 0});
        rs = stmt2.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
    }

    @Test
    public void testVarbinary() throws Exception {
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName + "(" +
                "    id INTEGER NOT NULL,\n" +
                "    BIN_PK VARBINARY NOT NULL,\n" +
                "    BIN_COL VARBINARY \n" +
                "    CONSTRAINT pk PRIMARY KEY (id, BIN_PK)" +
                ")");


        byte[] b11 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
        byte[] b12 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
        byte[] b21 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};
        byte[] b22 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?, ?)");
        upsertRow(stmt, 1, b11, b12);
        upsertRow(stmt, 2, b21, b22);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, 0, 4), SUBBINARY(BIN_COL, 1, 3) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 0, 4);
        assertSubBinary(b12, rs.getBytes(2), 0, 3);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 0, 4);
        assertSubBinary(b22, rs.getBytes(2), 0, 3);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, 5), SUBBINARY(BIN_COL, 7) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 4, 6);
        assertSubBinary(b12, rs.getBytes(2), 6, 6);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 4, 6);
        assertSubBinary(b22, rs.getBytes(2), 6, 4);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, -4, 3), SUBBINARY(BIN_COL, -3, 1) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 6, 3);
        assertSubBinary(b12, rs.getBytes(2), 9, 1);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 6, 3);
        assertSubBinary(b22, rs.getBytes(2), 7, 1);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, -2), SUBBINARY(BIN_COL, -2) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 8, 2);
        assertSubBinary(b12, rs.getBytes(2), 10, 2);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 8, 2);
        assertSubBinary(b22, rs.getBytes(2), 8, 2);

        PreparedStatement stmt2 = conn.prepareStatement("SELECT id FROM " + tableName + " WHERE SUBBINARY(BIN_COL, 2, 6) = ?");
        stmt2.setBytes(1, new byte[] {1, 20, -28, 0, -1, 0});
        rs = stmt2.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
    }

    @Test
    public void testVarbinaryEncoded() throws Exception {
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName + "(" +
                "    id INTEGER NOT NULL,\n" +
                "    BIN_PK VARBINARY_ENCODED NOT NULL,\n" +
                "    BIN_COL VARBINARY_ENCODED \n" +
                "    CONSTRAINT pk PRIMARY KEY (id, BIN_PK)" +
                ")");


        byte[] b11 = new byte[] {56, 50, 19, 0, 34, 83, -101, -102, 91, 92};
        byte[] b12 = new byte[] {10, 55, 0, 19, -5, -34, 0, -12, 0, 0, 0, 1};
        byte[] b21 = new byte[] {-11, 55, -119, 0, 8, 0, 1, 2, -4, 33};
        byte[] b22 = new byte[] {1, 1, 20, -28, 0, -1, 0, -11, -21, -1};
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?, ?)");
        upsertRow(stmt, 1, b11, b12);
        upsertRow(stmt, 2, b21, b22);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, 0, 4), SUBBINARY(BIN_COL, 1, 3) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 0, 4);
        assertSubBinary(b12, rs.getBytes(2), 0, 3);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 0, 4);
        assertSubBinary(b22, rs.getBytes(2), 0, 3);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, 5), SUBBINARY(BIN_COL, 7) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 4, 6);
        assertSubBinary(b12, rs.getBytes(2), 6, 6);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 4, 6);
        assertSubBinary(b22, rs.getBytes(2), 6, 4);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, -4, 3), SUBBINARY(BIN_COL, -3, 1) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 6, 3);
        assertSubBinary(b12, rs.getBytes(2), 9, 1);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 6, 3);
        assertSubBinary(b22, rs.getBytes(2), 7, 1);

        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_PK, -1), SUBBINARY(BIN_COL, -1) FROM " + tableName);
        rs.next();
        assertSubBinary(b11, rs.getBytes(1), 9, 1);
        assertSubBinary(b12, rs.getBytes(2), 11, 1);
        rs.next();
        assertSubBinary(b21, rs.getBytes(1), 9, 1);
        assertSubBinary(b22, rs.getBytes(2), 9, 1);


        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_COL, 2, 6) FROM " + tableName + " WHERE id = 2");
        rs.next();
        System.out.println("rs.getBytes(1) = " + Arrays.toString(rs.getBytes(1)));
        PreparedStatement stmt2 = conn.prepareStatement("SELECT id FROM " + tableName + " WHERE SUBBINARY(BIN_COL, 2, 6) = ?");
        stmt2.setBytes(1, new byte[] {1, 20, -28, 0, -1, 0});
        rs = stmt2.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
    }



    @Test
    public void testSubBinaryFunction() throws Exception {
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName + "(" +
                "    id INTEGER PRIMARY KEY,\n" +
                "    VAR_BIN_COL VARBINARY,\n" +
                "    BIN_COL BINARY(8)" +
                ")");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1, X'010203040506', X'01020304')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (2, X'000000000000', X'00000000')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (3, X'FFEEAABB', X'FFEEAABBFFEEAABB')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (4, null, X'0101010101010101')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (5, X'', X'04030201')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (6, X'0204', X'')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (7, X'010101', null)");
        conn.commit();

        ResultSet rs;

        // first n bytes
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE SUBBINARY(VAR_BIN_COL, 1, 4) = X'01020304'");
        Assert.assertTrue(rs.next() && rs.getInt(1) == 1);
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE SUBBINARY(BIN_COL, 1, 3) = X'FFEEAA'");
        Assert.assertTrue(rs.next() && rs.getInt(1) == 3);

        // length not provided, padding for fixed-length binary
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE SUBBINARY(VAR_BIN_COL, 2) = X'EEAABB'");
        Assert.assertTrue(rs.next() && rs.getInt(1) == 3);
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE SUBBINARY(BIN_COL, 2) = X'03020100000000'");
        Assert.assertTrue(rs.next() && rs.getInt(1) == 5);

        // offset > length of column value
        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(VAR_BIN_COL, 15, 2) FROM " + tableName + " WHERE id = 1");
        Assert.assertTrue(rs.next() && rs.getBytes(1) == null);
        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_COL, 15, 2) FROM " + tableName + " WHERE id = 2");
        Assert.assertTrue(rs.next() && rs.getBytes(1) == null);

        // negative offset with length
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE SUBBINARY(VAR_BIN_COL, -2, 1) = X'05'");
        Assert.assertTrue(rs.next() && rs.getInt(1) == 1);
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE SUBBINARY(BIN_COL, -2, 1) = X'AA'");
        Assert.assertTrue(rs.next() && rs.getInt(1) == 3);

        //negative offset without length
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE SUBBINARY(VAR_BIN_COL, -3) = X'000000'");
        Assert.assertTrue(rs.next() && rs.getInt(1) == 2);
        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE SUBBINARY(BIN_COL, -2) = X'0101'");
        Assert.assertTrue(rs.next() && rs.getInt(1) == 4);

        // empty column value
        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(VAR_BIN_COL, 2, 3) FROM " + tableName + " WHERE id = 5");
        Assert.assertTrue(rs.next() && rs.getBytes(1) == null);
        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_COL, 1, 4) FROM " + tableName + " WHERE id = 6");
        Assert.assertTrue(rs.next() && rs.getBytes(1) == null);

        // null values
        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(VAR_BIN_COL, 1, 4) FROM " + tableName + " WHERE id = 4");
        Assert.assertTrue(rs.next() && rs.getBytes(1) == null);
        rs = conn.createStatement().executeQuery("SELECT SUBBINARY(BIN_COL, 2, 3) FROM " + tableName + " WHERE id = 7");
        Assert.assertTrue(rs.next() && rs.getBytes(1) == null);
    }

    @Test
    public void testExplainPlanWithSubBinaryFunctionInPK() throws Exception {
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName
                + " (id INTEGER NOT NULL, VAR_BIN_COL VARBINARY NOT NULL, DESCRIPTION VARCHAR CONSTRAINT pk PRIMARY KEY (id, VAR_BIN_COL))");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1, X'0102030405', 'desc1')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1, X'010101', 'desc5')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (2, X'0000000001', 'desc2')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (3, X'FFEEAABB', 'desc3')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (4, X'AA', 'desc4')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (6, X'AAFFAA', 'desc6')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (7, X'BB', 'desc7')");
        conn.commit();

        String sql = "SELECT * FROM " + tableName + " WHERE id = 1 AND SUBBINARY(VAR_BIN_COL, 0, 1) = X'01'";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
        rs = conn.createStatement().executeQuery("EXPLAIN " + sql);
        String plan = QueryUtil.getExplainPlan(rs);
        Assert.assertTrue(plan.contains("RANGE SCAN OVER " + tableName + " [1,X'01'] - [1,X'02']"));
    }

    private void upsertRow(PreparedStatement stmt, int id, byte[] b1, byte[] b2) throws SQLException {
        stmt.setInt(1, id);
        stmt.setBytes(2, b1);
        stmt.setBytes(3, b2);
        stmt.executeUpdate();
    }

    private void assertSubBinary(byte[] expected, byte[] actual, int start, int length) {
        for (int i = 0; i < length; i++) {
            Assert.assertEquals(expected[start + i], actual[i]);
        }
    }
}
