package org.apache.phoenix.calcite;

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class CalciteDMLIT extends BaseCalciteIT {
    private static final Properties PROPS = new Properties();
    
    @Before
    public void initTable() throws Exception {
        final String url = getOldUrl();
        ensureTableCreated(url, ATABLE_NAME);
    }

    /** Tests a simple command that is defined in Phoenix's extended SQL parser. 
     * @throws Exception */
    @Ignore
    @Test public void testCommit() throws Exception {
        start(PROPS).sql("commit").execute();
    }

    @Test
    public void testUpsertValues() throws Exception {
        start(PROPS).sql("upsert into atable(organization_id, entity_id) values('1', '1')")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixTableModify(table=[[phoenix, ATABLE]], operation=[INSERT], updateColumnList=[[]], flattened=[false])\n" +
                       "    PhoenixClientProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[null], B_STRING=[null], A_INTEGER=[null], A_DATE=[null], A_TIME=[null], A_TIMESTAMP=[null], X_DECIMAL=[null], X_LONG=[null], X_INTEGER=[null], Y_INTEGER=[null], A_BYTE=[null], A_SHORT=[null], A_FLOAT=[null], A_DOUBLE=[null], A_UNSIGNED_FLOAT=[null], A_UNSIGNED_DOUBLE=[null])\n" +
                       "      PhoenixValues(tuples=[[{ '1              ', '1              ' }]])\n")
            .executeUpdate()
            .close();
        start(false, 1L).sql("select organization_id, entity_id from aTable")
            .resultIs(0, new Object[][] {{"1              ", "1              "}})
            .close();
        final Sql sql = start(PROPS).sql("select * from atable where organization_id = ?");
        PreparedStatement stmt = sql.prepareStatement();
        stmt.setString(1, "1");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("1              ", rs.getString(2));
        assertFalse(rs.next());
        sql.close();
    }

    @Test
    public void testUpsertSelect() throws Exception {
        startPhoenixStandalone(PROPS).sql("create table srcTable(pk0 integer not null, pk1 integer not null, c0 varchar(5), c1 varchar(5) constraint pk primary key (pk0, pk1))")
            .execute()
            .close();
        startPhoenixStandalone(PROPS).sql("create table tgtTable(pk0 integer not null, pk1 integer not null, f0 varchar(5), f1 varchar(5), f2 varchar(5) constraint pk primary key (pk0, pk1))")
            .execute()
            .close();
        start(PROPS).sql("upsert into srcTable values(1, 10, '00100', '01000')")
            .executeUpdate()
            .close();
        start(PROPS).sql("upsert into srcTable values(2, 20, '00200', '02000')")
            .executeUpdate()
            .close();
        start(PROPS).sql("upsert into srcTable values(3, 30, '00300', '03000')")
            .executeUpdate()
            .close();
        final Sql sql = start(PROPS).sql("upsert into tgtTable(pk0, pk1, f0, f2) select * from srcTable where pk1 <> ?");
        final PreparedStatement stmt = sql.prepareStatement();
        stmt.setInt(1, 20);
        stmt.executeUpdate();
        sql.close();
        start(false, 1L).sql("select * from tgtTable")
            .resultIs(0, new Object[][] {
                {1, 10, "00100", null, "01000"},
                {3, 30, "00300", null, "03000"}})
            .close();
    }
    
    @Test public void testUpsertWithPreparedStatement() throws Exception {
        final Sql sql = start(PROPS).sql("upsert into atable(organization_id, entity_id) values(?, ?)");
        final PreparedStatement stmt = sql.prepareStatement();
        stmt.setString(1, "x00000000000001");
        stmt.setString(2, "y00000000000001");
        stmt.executeUpdate();
        stmt.setString(1, "x00000000000002");
        stmt.setString(2, "y00000000000002");
        stmt.executeUpdate();
        stmt.setString(1, "x00000000000003");
        stmt.setString(2, "y00000000000003");
        stmt.executeUpdate();
        sql.close();
        start(PROPS).sql("select organization_id, entity_id, a_string from atable where organization_id like 'x%'")
            .resultIs(0, new Object[][] {
                {"x00000000000001", "y00000000000001", null},
                {"x00000000000002", "y00000000000002", null},
                {"x00000000000003", "y00000000000003", null}})
            .close();
    }
    
    @Test public void testDelete() throws Exception {
        start(PROPS).sql("upsert into atable(organization_id, entity_id) values('1', '1')")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixTableModify(table=[[phoenix, ATABLE]], operation=[INSERT], updateColumnList=[[]], flattened=[false])\n" +
                       "    PhoenixClientProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[null], B_STRING=[null], A_INTEGER=[null], A_DATE=[null], A_TIME=[null], A_TIMESTAMP=[null], X_DECIMAL=[null], X_LONG=[null], X_INTEGER=[null], Y_INTEGER=[null], A_BYTE=[null], A_SHORT=[null], A_FLOAT=[null], A_DOUBLE=[null], A_UNSIGNED_FLOAT=[null], A_UNSIGNED_DOUBLE=[null])\n" +
                       "      PhoenixValues(tuples=[[{ '1              ', '1              ' }]])\n")
            .executeUpdate()
            .close();
        start(PROPS).sql("select organization_id, entity_id from aTable where organization_id = '1'")
            .resultIs(0, new Object[][] {{"1              ", "1              "}})
            .close();
        start(PROPS).sql("delete from atable where organization_id = '1' and entity_id = '1'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixTableModify(table=[[phoenix, ATABLE]], operation=[DELETE], updateColumnList=[[]], flattened=[false])\n" +
                       "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[AND(=($0, CAST('1'):CHAR(15) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL), =($1, CAST('1'):CHAR(15) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL))])\n")
            .executeUpdate()
            .close();
        start(PROPS).sql("select * from aTable where organization_id = '1'")
            .resultIs(new Object[][] {})
            .close();
    }
    
    @Test public void testDateTimeTimestampAsBindVariables() throws Exception {
        start(PROPS).sql("create table t0(a date not null, b time not null, c timestamp constraint pk primary key(a, b))").execute().close();
        Sql sql = start(PROPS).sql("upsert into t0 values(?, ?, ?)");
        PreparedStatement stmt = sql.prepareStatement();
        stmt.setDate(1, Date.valueOf("2016-07-12"));
        stmt.setTime(2, Time.valueOf("12:30:28"));
        stmt.setTimestamp(3, Timestamp.valueOf("2016-7-12 12:30:28"));
        stmt.executeUpdate();
        stmt.setDate(1, Date.valueOf("2016-07-12"));
        stmt.setTime(2, Time.valueOf("12:34:09"));
        stmt.setTimestamp(3, Timestamp.valueOf("2016-7-12 12:34:09"));
        stmt.executeUpdate();
        stmt.setDate(1, Date.valueOf("2016-07-16"));
        stmt.setTime(2, Time.valueOf("09:20:08"));
        stmt.setTimestamp(3, Timestamp.valueOf("2016-7-16 09:20:08"));
        stmt.executeUpdate();
        sql.close();
        start(PROPS).sql("select * from t0")
            .resultIs(new Object[][]{
                {Date.valueOf("2016-07-12"), Time.valueOf("12:30:28"), Timestamp.valueOf("2016-7-12 12:30:28")},
                {Date.valueOf("2016-07-12"), Time.valueOf("12:34:09"), Timestamp.valueOf("2016-7-12 12:34:09")},
                {Date.valueOf("2016-07-16"), Time.valueOf("09:20:08"), Timestamp.valueOf("2016-7-16 09:20:08")}})
            .close();
    }
}
