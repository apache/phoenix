package org.apache.phoenix.rpc;

import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.MUTABLE_INDEX_DATA_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Maps;

/**
 * Verifies the number of rpcs calls from {@link MetaDataClient} updateCache() 
 * for transactional and non-transactional tables.
 */
public class UpdateCacheIT extends BaseHBaseManagedTimeIT {
	
	public static final int NUM_MILLIS_IN_DAY = 86400000;

    @Before
    public void setUp() throws SQLException {
        ensureTableCreated(getUrl(), MUTABLE_INDEX_DATA_TABLE);
    }

	@BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
	
	public static void validateRowKeyColumns(ResultSet rs, int i) throws SQLException {
		assertTrue(rs.next());
		assertEquals(rs.getString(1), "varchar" + String.valueOf(i));
		assertEquals(rs.getString(2), "char" + String.valueOf(i));
		assertEquals(rs.getInt(3), i);
		assertEquals(rs.getInt(4), i);
		assertEquals(rs.getBigDecimal(5), new BigDecimal(i*0.5d));
		Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i - 1) * NUM_MILLIS_IN_DAY);
		assertEquals(rs.getDate(6), date);
	}
	
	public static void setRowKeyColumns(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setString(1, "varchar" + String.valueOf(i));
        stmt.setString(2, "char" + String.valueOf(i));
        stmt.setInt(3, i);
        stmt.setLong(4, i);
        stmt.setBigDecimal(5, new BigDecimal(i*0.5d));
        Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i - 1) * NUM_MILLIS_IN_DAY);
        stmt.setDate(6, date);
    }
	
	@Test
	public void testUpdateCache() throws Exception {
		String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE;
		String selectSql = "SELECT * FROM "+fullTableName;
		// use a spyed ConnectionQueryServices so we can verify calls to getTable
		ConnectionQueryServices connectionQueryServices = Mockito.spy(driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)));
		Properties props = new Properties();
		props.putAll(PhoenixEmbeddedDriver.DEFFAULT_PROPS.asMap());
		Connection conn = connectionQueryServices.connect(getUrl(), props);
		try {
			conn.setAutoCommit(false);
			ResultSet rs = conn.createStatement().executeQuery(selectSql);
	     	assertFalse(rs.next());
	     	reset(connectionQueryServices);
	     	
	        String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
	        PreparedStatement stmt = conn.prepareStatement(upsert);
			// upsert three rows
	        setRowKeyColumns(stmt, 1);
			stmt.execute();
			setRowKeyColumns(stmt, 2);
			stmt.execute();
			setRowKeyColumns(stmt, 3);
			stmt.execute();
			conn.commit();
			// verify only one rpc to getTable occurs after commit is called
			verify(connectionQueryServices, times(1)).getTable((PName)isNull(), eq(PVarchar.INSTANCE.toBytes(INDEX_DATA_SCHEMA)), eq(PVarchar.INSTANCE.toBytes(MUTABLE_INDEX_DATA_TABLE)), anyLong(), anyLong());
			reset(connectionQueryServices);
			
			rs = conn.createStatement().executeQuery(selectSql);
			validateRowKeyColumns(rs, 1);
			validateRowKeyColumns(rs, 2);
			validateRowKeyColumns(rs, 3);
	        assertFalse(rs.next());
	        
	        rs = conn.createStatement().executeQuery(selectSql);
	        validateRowKeyColumns(rs, 1);
	        validateRowKeyColumns(rs, 2);
	        validateRowKeyColumns(rs, 3);
	        assertFalse(rs.next());
	        
	        rs = conn.createStatement().executeQuery(selectSql);
	        validateRowKeyColumns(rs, 1);
	        validateRowKeyColumns(rs, 2);
	        validateRowKeyColumns(rs, 3);
	        assertFalse(rs.next());
	        conn.commit();
	        // there should be one rpc to getTable per query
	        verify(connectionQueryServices, times(3)).getTable((PName)isNull(), eq(PVarchar.INSTANCE.toBytes(INDEX_DATA_SCHEMA)), eq(PVarchar.INSTANCE.toBytes(MUTABLE_INDEX_DATA_TABLE)), anyLong(), anyLong());
		}
        finally {
        	conn.close();
        }
	}
}
