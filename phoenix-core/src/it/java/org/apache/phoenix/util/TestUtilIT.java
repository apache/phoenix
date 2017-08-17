package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.junit.Test;

public class TestUtilIT extends ParallelStatsDisabledIT {
    @Test
    public void testRowCountIndexScrutiny() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            
            int count = conn.createStatement().executeUpdate("DELETE FROM " + fullIndexName + " WHERE \":K\"='a' AND \"0:V\"='ccc'");
            assertEquals(1,count);
            conn.commit();
            try {
                TestUtil.scrutinizeIndex(conn, fullTableName, fullIndexName);
                fail();
            } catch (AssertionError e) {
                assertEquals(e.getMessage(),"Expected data table row count to match expected:<2> but was:<1>");
            }
        }
    }
    @Test
    public void testExtraRowIndexScrutiny() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR, v2 VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v) INCLUDE (v2)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb','0')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc','1')");
            conn.commit();
            
            conn.createStatement().executeUpdate("UPSERT INTO " + fullIndexName + " VALUES ('bbb','x','0')");
            conn.commit();
            try {
                TestUtil.scrutinizeIndex(conn, fullTableName, fullIndexName);
                fail();
            } catch (AssertionError e) {
                assertEquals(e.getMessage(),"Expected to find PK in data table: ('x')");
            }
        }
    }
    
    @Test
    public void testValueIndexScrutiny() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR, v2 VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v) INCLUDE (v2)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb','0')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc','1')");
            conn.commit();
            
            conn.createStatement().executeUpdate("UPSERT INTO " + fullIndexName + " VALUES ('ccc','a','2')");
            conn.commit();
            try {
                TestUtil.scrutinizeIndex(conn, fullTableName, fullIndexName);
                fail();
            } catch (AssertionError e) {
                assertEquals(e.getMessage(),"Expected equality for V2, but '2'!='1'");
            }
        }
    }


}
