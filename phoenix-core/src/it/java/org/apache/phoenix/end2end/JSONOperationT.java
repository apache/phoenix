package org.apache.phoenix.end2end;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class JSONOperationT extends BaseHBaseManagedTimeIT{
	private String json = "{\"k1\":\"val\","		+"\"k2\":true,"		+"\"k3\":2 ,"		+"\"k4\":2.5 "+
				",\"k5\":[1,\"val2\",false,3.5]"+
				",\"k6\":{\"nestk1\":\"nestval\",\"nestk2\":77,\"nestk3\":2.1,\"nestk4\":[9,8.4,\"nestarrayval\"]}}";
	@Test
    public void testJsonPointAsElementForTypeInteger() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 -> 'k3' = 2";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testJsonPointAsElementForTypeFloat() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 -> 'k4' = 2.5";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testJsonPointAsElementForTypeVarchar() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 -> 'k1' LIKE 'val'";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testJsonPointAsText() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 ->> 'k3' LIKE '2'";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testJsonPathAsElement() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 #> '{k6,nestk4,1}' = 8.4";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testJsonPathAsText() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 #>> '{k6,nestk4,0}' LIKE '9'";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testJsonSubSet() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 @> '{\"k1\":\"val\"}'";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testJsonSuperSet() throws Exception {
        String smalljson="{\"k1\":\"val\"}";
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 <@ "+"'"+json+"'";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(smalljson, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", smalljson, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testJsonKeySearch() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 ? 'k3'";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testsonMultiKeySearchOr() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 ?| array['k3','k100']";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
	@Test
    public void testsonMultiKeySearchAnd() throws Exception {
        String selectQuery = "SELECT col1 FROM testJson WHERE col1 ?& array['k3','k4']";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
		private Connection getConnection() throws SQLException {
		       	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		        Connection conn = DriverManager.getConnection(getUrl(), props);
		        conn.setAutoCommit(false);
		        return conn;
		}
		
		private void createTableAndUpsertRecord(String json, String pk, Connection conn) throws SQLException {
			String ddl =
		           "CREATE TABLE testJson" + "  (pk VARCHAR NOT NULL PRIMARY KEY, " + "col1 json)";
				createTestTable(getUrl(), ddl);
		
		       upsertRecord(json, pk, conn);
		}
		
		private void upsertRecord(String json, String pk, Connection conn) throws SQLException {
		      	String query = "UPSERT INTO testJson(pk, col1) VALUES(?,?)";
		      	PreparedStatement stmt = conn.prepareStatement(query);
		      	stmt.setString(1, pk);
		      	stmt.setString(2, json);
		      	stmt.execute();
		      	conn.commit();
		}
}
