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
            //.executeUpdate()
            .close();
    }
}
