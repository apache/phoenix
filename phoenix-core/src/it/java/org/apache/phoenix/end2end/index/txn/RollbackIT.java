package org.apache.phoenix.end2end.index.txn;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class RollbackIT extends BaseHBaseManagedTimeIT {
	
	private final boolean localIndex;
	private final boolean mutable;

	public RollbackIT(boolean localIndex, boolean mutable) {
		this.localIndex = localIndex;
		this.mutable = mutable;
	}
	
	@BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.DEFAULT_TRANSACTIONAL_ATTRIB, Boolean.toString(true));
        // We need this b/c we don't allow a transactional table to be created if the underlying
        // HBase table already exists (since we don't know if it was transactional before).
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
	
	@Parameters(name="localIndex = {0} , mutable = {1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {     
                 { false, false }, { false, true },
                 { true, false }, { true, true }  
           });
    }
    
    @Test
    public void testRollbackOfUncommittedKeyValueIndexInsert() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE DEMO(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"+(!mutable? " IMMUTABLE_ROWS=true" : ""));
            stmt.execute("CREATE "+(localIndex? "LOCAL " : "")+"INDEX DEMO_idx ON DEMO (v1) INCLUDE(v2)");
            
            stmt.executeUpdate("upsert into DEMO values('x', 'y', 'a')");
            
            //assert values in data table
            ResultSet rs = stmt.executeQuery("select k, v1, v2 from DEMO ORDER BY k");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select k, v1, v2  from DEMO ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert values in data table
            rs = stmt.executeQuery("select k, v1, v2 from DEMO ORDER BY k");
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select k, v1, v2 from DEMO ORDER BY v1");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRollbackOfUncommittedRowKeyIndexInsert() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE DEMO(k VARCHAR, v1 VARCHAR, v2 VARCHAR, CONSTRAINT pk PRIMARY KEY (v1, v2))"+(!mutable? " IMMUTABLE_ROWS=true" : ""));
            stmt.execute("CREATE "+(localIndex? "LOCAL " : "")+"INDEX DEMO_idx ON DEMO (v1, k)");
            
            stmt.executeUpdate("upsert into DEMO values('x', 'y', 'a')");

            ResultSet rs = stmt.executeQuery("select k, v1, v2 from DEMO ORDER BY v1");
            
            //assert values in data table
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select k, v1 from DEMO ORDER BY v2");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert values in data table
            rs = stmt.executeQuery("select k, v1, v2 from DEMO");
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select k, v1 from DEMO ORDER BY v2");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
}

