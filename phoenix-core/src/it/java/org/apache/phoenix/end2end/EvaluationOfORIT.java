package org.apache.phoenix.end2end;

 
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

 
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Before;
import org.junit.Test;

public class EvaluationOfORIT extends BaseClientManagedTimeIT{
	private static final String tenantId = getOrganizationId();
	private long ts;
	private Date date;
	
	@Before
        public void initTable() throws Exception {
            ts = nextTimestamp();
            initATableValues(tenantId, getDefaultSplits(tenantId), date=new Date(System.currentTimeMillis()), ts);        
        }
	
	@Test
	public void testPKOrNotPKInOREvaluation() throws SQLException {
            Connection conn = DriverManager.getConnection(getUrl());
            String create = "CREATE TABLE DIE ( ID INTEGER NOT NULL PRIMARY KEY,NAME VARCHAR(50) NOT NULL)";
            PreparedStatement createStmt = conn.prepareStatement(create);
            createStmt.executeUpdate();
            conn.close();
            Connection conn1 = DriverManager.getConnection(getUrl());
            PreparedStatement stmt = conn1.prepareStatement(
                    "upsert into " +
                    "DIE VALUES (?, ?)");
            stmt.setInt(1, 1);
            stmt.setString(2, "Tester1");
            stmt.execute();
            
            stmt.setInt(1,2);
            stmt.setString(2, "Tester2");
            stmt.execute();
            
            stmt.setInt(1,3);
            stmt.setString(2, "Tester3");
            stmt.execute();
            
            stmt.setInt(1,4);
            stmt.setString(2, "LikeTester1");
            stmt.execute();
            
            stmt.setInt(1,5);
            stmt.setString(2, "LikeTester2");
            stmt.execute();
            
            stmt.setInt(1,6);
            stmt.setString(2, "LikeTesterEnd");
            stmt.execute();
            
            stmt.setInt(1,7);
            stmt.setString(2, "LikeTesterEnd2");
            stmt.execute();
            
            stmt.setInt(1,8);
            stmt.setString(2, "Tester3");
            stmt.execute();
            
            stmt.setInt(1,9);
            stmt.setString(2, "Tester4");
            stmt.execute();
            
            stmt.setInt(1,10);
            stmt.setString(2, "Tester5");
            stmt.execute();
            
            stmt.setInt(1,11);
            stmt.setString(2, "Tester6");
            stmt.execute();
            
            stmt.setInt(1,12);
            stmt.setString(2, "tester6");
            stmt.execute();
            
            stmt.setInt(1,13);
            stmt.setString(2, "lester1");
            stmt.execute();
            
            stmt.setInt(1,14);
            stmt.setString(2, "le50ster1");
            stmt.execute();
            
            stmt.setInt(1,15);
            stmt.setString(2, "LE50ster1");
            stmt.execute();
            
            stmt.setInt(1,16);
            stmt.setString(2, "LiketesterEnd");
            stmt.execute();
            
            stmt.setInt(1,17);
            stmt.setString(2, "la50ster1");
            stmt.execute();
            
            stmt.setInt(1,18);
            stmt.setString(2, "lA50ster0");
            stmt.execute();
            
            stmt.setInt(1,19);
            stmt.setString(2, "lA50ster2");
            stmt.execute();
            
            stmt.setInt(1,20);
            stmt.setString(2, "la50ster0");
            stmt.execute();
            
            stmt.setInt(1,21);
            stmt.setString(2, "la50ster2");
            stmt.execute();
            
            stmt.setInt(1,22);
            stmt.setString(2, "La50ster3");
            stmt.execute();
            
            stmt.setInt(1,23);
            stmt.setString(2, "la50ster3");
            stmt.execute();
            
            stmt.setInt(1,24);
            stmt.setString(2, "l[50ster3");
            stmt.execute();
            
            stmt.setInt(1,25);
            stmt.setString(2, "Tester1");
            stmt.execute();
            
            stmt.setInt(1,26);
            stmt.setString(2, "Tester100");
            stmt.execute();		   
            
            conn1.commit();
            conn1.close();
            
            Connection conn2 = DriverManager.getConnection(getUrl());
            String select = "Select * from DIE where ID=6 or Name between 'Tester1' and 'Tester3'";
            ResultSet rs;
            rs = conn2.createStatement().executeQuery(select);
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(6,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(8,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(25,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(26,rs.getInt(1));		        
            conn2.close();               
	}
    
	@Test
        public void testUnfoundSingleColumnCaseStatement() throws Exception {
            String query = "SELECT entity_id, b_string FROM ATABLE WHERE organization_id=? and CASE WHEN a_integer = 0 or a_integer != 0 THEN 1 ELSE 0 END = 0";
            String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
            Properties props = new Properties(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(url, props);
            // Set ROW5.A_INTEGER to null so that we have one row
            // where the else clause of the CASE statement will
            // fire.
            url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
            Connection upsertConn = DriverManager.getConnection(url, props);
            String upsertStmt =
                "upsert into " +
                "ATABLE(" +
                "    ENTITY_ID, " +
                "    ORGANIZATION_ID, " +
                "    A_INTEGER) " +
                "VALUES ('" + ROW5 + "','" + tenantId + "', null)";
            upsertConn.setAutoCommit(true); // Test auto commit
            // Insert all rows at ts
            PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
            stmt.execute(); // should commit too
            upsertConn.close();
            
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW5, rs.getString(1));
            assertFalse(rs.next());
            conn.close();
        }
}
