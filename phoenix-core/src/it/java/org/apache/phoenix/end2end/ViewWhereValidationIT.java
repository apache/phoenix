package org.apache.phoenix.end2end;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class ViewWhereValidationIT extends ParallelStatsDisabledIT {
    @Test
    public void testTenantViewUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String schemaName = "TEST_SCHEMA_001";
            String dataTableName = "TEST_TABLE_001";
            String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
            String globalViewName = "TEST_GLOBAL_VIEW_001";
            String globalViewFullName = SchemaUtil.getTableName(schemaName, globalViewName);
            String viewName = "TEST_VIEW_0";
            String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
            String view1Name = "TEST_VIEW_1";
            String view1FullName = SchemaUtil.getTableName(schemaName, view1Name);
            String leafViewName = "TEST_LEAF_VIEW_0";
            String leafViewFullName = SchemaUtil.getTableName(schemaName, leafViewName);
            String leafView1Name = "TEST_LEAF_VIEW_1";
            String leafView1FullName = SchemaUtil.getTableName(schemaName, leafView1Name);
            String indexTableName1 = "TEST_IDX_001";
            String indexTableFullName1 = SchemaUtil.getTableName(schemaName, indexTableName1);
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (OID CHAR(15) NOT NULL, KP CHAR(3) NOT NULL, VAL1 INTEGER, VAL2 INTEGER CONSTRAINT PK PRIMARY KEY (OID, KP)) MULTI_TENANT=true");
            conn.commit();
            conn.createStatement().execute(String.format("CREATE VIEW IF NOT EXISTS %s(ID1 INTEGER not null, COL4 VARCHAR, CONSTRAINT pk PRIMARY KEY (ID1)) AS SELECT * FROM %s WHERE KP = 'P01'", globalViewFullName, dataTableFullName));
            conn.commit();
            conn.createStatement().execute(String.format(
                    "CREATE INDEX IF NOT EXISTS %s ON %s(ID1) include (COL4)", indexTableName1, globalViewFullName));
            conn.createStatement().execute(String.format("CREATE VIEW IF NOT EXISTS %s(TP INTEGER not null, ROW_ID CHAR(15) NOT NULL,COLA VARCHAR CONSTRAINT pk PRIMARY KEY (TP,ROW_ID)) AS SELECT * FROM %s WHERE ID1 = 42724", viewFullName, globalViewFullName));
            conn.commit();
            conn.createStatement().execute(String.format("CREATE VIEW IF NOT EXISTS %s AS SELECT " +
                    "* from %s WHERE TP = 31", leafViewFullName, viewFullName));
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + leafViewFullName + " (OID, ROW_ID, COL4, COLA) values ('00D0y0000000001', '00Z0y0000000001','d07223','a05493')");
            conn.commit();
            TestUtil.dumpTable(conn, TableName.valueOf(dataTableFullName));
            TestUtil.dumpTable(conn, TableName.valueOf(("_IDX_" + dataTableFullName)));

            conn.createStatement().execute(String.format("CREATE VIEW IF NOT EXISTS %s AS SELECT * from %s WHERE ID1 = 42725", view1FullName, globalViewFullName));
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + view1FullName + " (OID, COL4) " +
                    "values ('00D0y0000000001','d07223')");
            conn.commit();

            conn.createStatement().execute(String.format("CREATE VIEW IF NOT EXISTS %s AS SELECT " +
                    "* from %s WHERE TP = 32", leafView1FullName, viewFullName));
            conn.commit();

            conn.createStatement().execute("UPSERT INTO " + leafViewFullName + " (OID, ROW_ID, COL4, COLA) values ('00D0y0000000001', '00Z0y0000000001','d07223','a05493')");
            conn.commit();

        }
    }
}