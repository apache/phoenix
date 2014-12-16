package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class MinMaxAggregateFunctionIT extends BaseHBaseManagedTimeIT {
	
	@Test
	public void minMax() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
			conn.prepareStatement("create table TT("
						+ "VAL1 integer not null, "
						+ "VAL2 char(2), "
						+ "VAL3 varchar, "
						+ "VAL4 varchar "
						+ "constraint PK primary key (VAL1))")
				.execute();
			conn.commit();
			
			conn.prepareStatement("upsert into TT values (0, '00', '00', '0')")
				.execute();
			conn.prepareStatement("upsert into TT values (1, '01', '01', '1')")
				.execute();
			conn.prepareStatement("upsert into TT values (2, '02', '02', '2')")
				.execute();
			conn.commit();
			
			ResultSet rs = conn.prepareStatement("select min(VAL2) from TT").executeQuery();
			assertTrue(rs.next());
			assertEquals("00", rs.getString(1));

			rs = conn.prepareStatement("select min(VAL3) from TT").executeQuery();
			assertTrue(rs.next());
			assertEquals("00", rs.getString(1));
			
			rs = conn.prepareStatement("select max(VAL2)from TT").executeQuery();
			assertTrue(rs.next());
			assertEquals("02", rs.getString(1));

			rs = conn.prepareStatement("select max(VAL3)from TT").executeQuery();
			assertTrue(rs.next());
			assertEquals("02", rs.getString(1));
			
			rs = conn.prepareStatement("select min(VAL1), min(VAL2), min(VAL3), min(VAL4) from TT").executeQuery();
			assertTrue(rs.next());
			assertEquals(0, rs.getInt(1));
			assertEquals("00", rs.getString(2));
			assertEquals("00", rs.getString(3));
			assertEquals("0", rs.getString(4));
			

			rs = conn.prepareStatement("select max(VAL1), max(VAL2), max(VAL3), max(VAL4) from TT").executeQuery();
			
			assertTrue(rs.next());
			assertEquals(2, rs.getInt(1));
			assertEquals("02", rs.getString(2));
			assertEquals("02", rs.getString(3));
			assertEquals("2", rs.getString(4));
        } finally {
            conn.close();
        }
	}
}
