package org.apache.phoenix.end2end.index.txn;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.jdbc.PhoenixConnection;
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
public class MutableRollbackIT extends BaseHBaseManagedTimeIT {
	
	private final boolean localIndex;

	public MutableRollbackIT(boolean localIndex) {
		this.localIndex = localIndex;
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
	
	@Parameters(name="localIndex = {0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(new Boolean[] {     
                 false, true  
           });
    }
	
	@Test
    public void testRollbackOfUncommittedExistingKeyValueIndexUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE DEMO1(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            stmt.execute("CREATE TABLE DEMO2(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX DEMO1_idx ON DEMO1 (v1) INCLUDE(v2)");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX DEMO2_idx ON DEMO2 (v1) INCLUDE(v2)");
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'y', 'a')");
            conn.commit();
            
            //assert rows exists in DEMO1
            ResultSet rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert no rows exists in DEMO2
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO2");
            assertFalse(rs.next());
            
            //assert no rows exists in DEMO2_idx
            rs = stmt.executeQuery("select k, v1 from DEMO2 ORDER BY v1");
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'y', 'b')");
            stmt.executeUpdate("upsert into DEMO2 values('a', 'b', 'c')");
            
            //assert new covered column value 
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("b", rs.getString(3));
            assertFalse(rs.next());
            
            //assert new covered column value 
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("b", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in DEMO2
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO2");
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertEquals("c", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in DEMO2 index table
            rs = stmt.executeQuery("select k, v1 from DEMO2 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in DEMO1
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert no rows exists in DEMO2
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO2");
            assertFalse(rs.next());
            
            //assert no rows exists in DEMO2_idx
            rs = stmt.executeQuery("select k, v1 from DEMO2 ORDER BY v1");
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'z', 'a')");
            stmt.executeUpdate("upsert into DEMO2 values('a', 'b', 'c')");
            conn.commit();

            assertDataAndIndexRows(stmt);
            stmt.executeUpdate("delete from DEMO1 where  k='x'");
            stmt.executeUpdate("delete from DEMO2 where  v1='b'");
            
            //assert no rows exists in DEMO1
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO1");
            assertFalse(rs.next());
            //assert no rows exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertFalse(rs.next());

            //assert no rows exists in DEMO2
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO2");
            assertFalse(rs.next());
            //assert no rows exists in DEMO2_idx
            rs = stmt.executeQuery("select k, v1 from DEMO2 ORDER BY v1");
            assertFalse(rs.next());
            
            conn.rollback();
            assertDataAndIndexRows(stmt);
        } finally {
            conn.close();
        }
    }

	@Test
    public void testRollbackOfUncommittedExistingRowKeyIndexUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE DEMO1(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            stmt.execute("CREATE TABLE DEMO2(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX DEMO1_idx ON DEMO1 (v1, k)");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX DEMO2_idx ON DEMO2 (v1, k)");
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'y', 'a')");
            conn.commit();
            
            //assert rows exists in DEMO1 
            ResultSet rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertFalse(rs.next());
            
            //assert no rows exists in DEMO2
            rs = stmt.executeQuery("select k, v1, v2 from DEMO2");
            assertFalse(rs.next());
            
            //assert no rows exists in DEMO2_idx
            rs = stmt.executeQuery("select k, v1 from DEMO2 ORDER BY v1");
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'z', 'a')");
            stmt.executeUpdate("upsert into DEMO2 values('a', 'b', 'c')");
            
            assertDataAndIndexRows(stmt);
            
            conn.rollback();
            
            //assert original row exists in DEMO1
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertFalse(rs.next());
            
            //assert no rows exists in DEMO2
            rs = stmt.executeQuery("select k, v1, v2 from DEMO2");
            assertFalse(rs.next());
            
            //assert no rows exists in DEMO2_idx
            rs = stmt.executeQuery("select k, v1 from DEMO2 ORDER BY v1");
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'z', 'a')");
            stmt.executeUpdate("upsert into DEMO2 values('a', 'b', 'c')");
            conn.commit();

            assertDataAndIndexRows(stmt);
            stmt.executeUpdate("delete from DEMO1 where  k='x'");
            stmt.executeUpdate("delete from DEMO2 where  v1='b'");
            
            //assert no rows exists in DEMO1
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertFalse(rs.next());
            //assert no rows exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertFalse(rs.next());

            //assert no rows exists in DEMO2
            rs = stmt.executeQuery("select k, v1, v2 from DEMO2");
            assertFalse(rs.next());
            //assert no rows exists in DEMO2_idx
            rs = stmt.executeQuery("select k, v1 from DEMO2 ORDER BY v1");
            assertFalse(rs.next());
            
            conn.rollback();
            assertDataAndIndexRows(stmt);

        } finally {
            conn.close();
        }
    }

	protected static void printRawTable(Connection conn, String tableName) throws SQLException, IOException {
	    HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName));
	    Scan scan = new Scan();
	    scan.setRaw(true);;
	    scan.setMaxVersions();
	    ResultScanner scanner = htable.getScanner(scan);
	    Result r = null;
	    System.out.println("**************** " + tableName);
	    while ((r = scanner.next()) != null) {
	        System.out.println("    **********" + r);
	    }
	}
	
    private static void assertDataAndIndexRows(Statement stmt) throws SQLException, IOException {
        ResultSet rs;
        //assert new covered row key value exists in DEMO1
        rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO1");
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("z", rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());
        
        //assert new covered row key value exists in DEMO1_idx
        rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("z", rs.getString(2));
        assertFalse(rs.next());
        
        //assert rows exists in DEMO2
        rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from DEMO2");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("b", rs.getString(2));
        assertEquals("c", rs.getString(3));
        assertFalse(rs.next());
        
        //assert rows exists in DEMO2 index table
        rs = stmt.executeQuery("select k, v1 from DEMO2 ORDER BY v1");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("b", rs.getString(2));
        assertFalse(rs.next());
    }
    
    @Test
    public void testMultiRollbackOfUncommittedExistingRowKeyIndexUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE DEMO1(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX DEMO1_idx ON DEMO1 (v1, k)");
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'yyyy', 'a')");
            conn.commit();
            
            //assert rows exists in DEMO1 
            ResultSet rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'zzz', 'a')");
            
            //assert new covered row key value exists in DEMO1
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("zzz", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert new covered row key value exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("zzz", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in DEMO1
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'zz', 'a')");
            
            //assert new covered row key value exists in DEMO1
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("zz", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert new covered row key value exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("zz", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in DEMO1
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertFalse(rs.next());
                        
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCheckpointAndRollback() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE DEMO1(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX DEMO1_idx ON DEMO1 (v1)");
            
            stmt.executeUpdate("upsert into DEMO1 values('x', 'a', 'a')");
            conn.commit();
            
            ResultSet rs;
            
            stmt.executeUpdate("upsert into DEMO1(k,v1) SELECT k,v1||'a' FROM DEMO1");
            
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aa", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aa", rs.getString(2));
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into DEMO1(k,v1) SELECT k,v1||'a' FROM DEMO1");
            
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aaa", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aaa", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in DEMO1
            rs = stmt.executeQuery("select k, v1, v2 from DEMO1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in DEMO1_idx
            rs = stmt.executeQuery("select k, v1 from DEMO1 ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
}
