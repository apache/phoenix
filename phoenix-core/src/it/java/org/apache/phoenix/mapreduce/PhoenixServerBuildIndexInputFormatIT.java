package org.apache.phoenix.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

public class PhoenixServerBuildIndexInputFormatIT  extends ParallelStatsDisabledIT {

    @Test
    public void testQueryPlanWithSource() throws Exception {
        PhoenixServerBuildIndexInputFormat inputFormat;
        Configuration conf = new Configuration(getUtility().getConfiguration());
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        String viewName = generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        String viewIndexName = generateUniqueName();
        String viewIndexFullName = SchemaUtil.getTableName(schemaName, viewIndexName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) ");
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName, dataTableFullName));
            conn.createStatement().execute("CREATE VIEW " + viewFullName +
                " AS SELECT * FROM " + dataTableFullName);
            conn.commit();

            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", viewIndexName, viewFullName));

            PhoenixConfigurationUtil.setIndexToolDataTableName(conf, dataTableFullName);
            PhoenixConfigurationUtil.setIndexToolIndexTableName(conf, indexTableFullName);
            // use data table as source (default)
            assertTableSource(conf, conn);

            // use index table as source
            PhoenixConfigurationUtil.setIndexToolSourceTable(conf, SourceTable.INDEX_TABLE_SOURCE);
            assertTableSource(conf, conn);

            PhoenixConfigurationUtil.setIndexToolDataTableName(conf, viewFullName);
            PhoenixConfigurationUtil.setIndexToolIndexTableName(conf, viewIndexFullName);
            PhoenixConfigurationUtil.setIndexToolSourceTable(conf, SourceTable.DATA_TABLE_SOURCE);

            assertTableSource(conf, conn);

            PhoenixConfigurationUtil.setIndexToolSourceTable(conf, SourceTable.INDEX_TABLE_SOURCE);
            assertTableSource(conf, conn);
        }
    }

    private void assertTableSource(Configuration conf, Connection conn) throws Exception {
        String dataTableFullName = PhoenixConfigurationUtil.getIndexToolDataTableName(conf);
        String indexTableFullName = PhoenixConfigurationUtil.getIndexToolIndexTableName(conf);
        SourceTable sourceTable = PhoenixConfigurationUtil.getIndexToolSourceTable(conf);
        boolean fromIndex = sourceTable.equals(SourceTable.INDEX_TABLE_SOURCE);
        PTable pDataTable = PhoenixRuntime.getTable(conn, dataTableFullName);
        PTable pIndexTable = PhoenixRuntime.getTable(conn, indexTableFullName);

        PhoenixServerBuildIndexInputFormat inputFormat = new PhoenixServerBuildIndexInputFormat();
        QueryPlan queryPlan = inputFormat.getQueryPlan(Job.getInstance(), conf);
        PTable actual = queryPlan.getTableRef().getTable();

        if (!fromIndex) {
            assertEquals(pDataTable, actual);
        } else {
            assertEquals(pIndexTable, actual);
        }
    }
}
