package org.apache.phoenix.end2end;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

@Category(ParallelStatsDisabledTest.class)
public class SubBinaryFunctionIT extends ParallelStatsDisabledIT {

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
}
