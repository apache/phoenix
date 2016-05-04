package org.apache.phoenix.calcite;

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class CalciteDMLIT extends BaseCalciteIT {
    private static final Properties PROPS = new Properties();
    
    @Before
    public void initTable() throws Exception {
        final String url = getUrl();
        ensureTableCreated(url, ATABLE_NAME);
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
        start(PROPS).sql("upsert into tgtTable(pk0, pk1, f0, f2) select * from srcTable where pk1 <> 20")
            .executeUpdate()
            .close();
        start(false, 1L).sql("select * from tgtTable")
            .resultIs(0, new Object[][] {
                {1, 10, "00100", null, "01000"},
                {3, 30, "00300", null, "03000"}})
            .close();
    }
}
