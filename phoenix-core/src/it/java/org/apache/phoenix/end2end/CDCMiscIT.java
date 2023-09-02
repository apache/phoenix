package org.apache.phoenix.end2end;

import org.apache.phoenix.query.BaseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Category(ParallelStatsDisabledTest.class)
public class CDCMiscIT extends ParallelStatsDisabledIT {
    @Test
    public void testCreate() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
                "create table  " + tableName + " ( k integer PRIMARY KEY," + " v1 integer,"
                        + " v2 integer)");

        String cdcName = generateUniqueName();
        conn.createStatement().execute("CREATE CDC " + cdcName
                + " ON " + tableName + "(PHOENIX_ROW_TIMESTAMP()) INDEX_TYPE=global");
        conn.close();
    }
}
