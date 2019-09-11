/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.tool.FindViewCorruptionTool;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class FindViewCorruptionToolIT extends ParallelStatsEnabledIT {

    private static final String filePath = "/tmp/";
    String CREATE_TABLE_QUERY = "CREATE TABLE %s (A BIGINT PRIMARY KEY, B VARCHAR)";
    String CREATE_VIEW_QUERY = "CREATE VIEW %s AS SELECT * FROM %s";
    String CREATE_MULTI_TENANT_TABLE_QUERY = "CREATE TABLE %s (TENANT_ID VARCHAR NOT NULL, " +
            "A VARCHAR NOT NULL, B BIGINT, CONSTRAINT pk PRIMARY KEY(TENANT_ID, A)) MULTI_TENANT=true";

    @Test
    public void testMultiLevelCorruptedView() throws Exception {
        String tableName1 = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        String viewName3 = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, tableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, viewName1, tableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, viewName2, tableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, viewName3, viewName2));
        //Testing no corrupted view case
        assertEquals(0,runFindCorruptedViewTool(true, false, null, null));

        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_NAME, TABLE_TYPE, COLUMN_COUNT) VALUES('" + viewName1 + "','v',1) ");
        conn.commit();
        //Testing corrupted view on a base table
        assertEquals(1, runFindCorruptedViewTool(true, false, null, null));

        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_NAME, TABLE_TYPE, COLUMN_COUNT) VALUES('" + viewName2 + "','v',1) ");
        conn.commit();
        //Testing multi layer corrupted view
        assertEquals(2, runFindCorruptedViewTool(true, false, null, null));

        String schema1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        String fullTableName1 = schema1 + '.' +  tableName2;
        String viewName4 = generateUniqueName();
        String fullViewName1 = schema1 + '.' +  viewName4;
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, fullTableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, fullViewName1, fullTableName1));
        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_SCHEM,TABLE_NAME, TABLE_TYPE, COLUMN_COUNT) VALUES('" +
                schema1 + "', '"+ viewName4 + "','v',10) ");
        conn.commit();
        //Testing corrupted view with schema enabled
        assertEquals(3, runFindCorruptedViewTool(true, true, null, null));
        //Testing output logging file
        verifyLineCount(filePath + FindViewCorruptionTool.fileName,3);
        //Testing corrupted view for a namespace
        assertEquals(1, runFindCorruptedViewTool(true, false, schema1, null));

        String schema2 = generateUniqueName();
        String tableName3 = generateUniqueName();
        String fullTableName2 = schema2 + '.' +  tableName3;
        String viewName5 = generateUniqueName();
        String tenantId = generateUniqueName();
        String fullViewName2 = schema2 + '.' +  viewName5;

        conn.createStatement().execute(
                String.format(CREATE_MULTI_TENANT_TABLE_QUERY, fullTableName2));
        Connection tenantConnection = tenantConnection(getUrl(), tenantId);
        tenantConnection.createStatement().execute(
                String.format(CREATE_VIEW_QUERY, fullViewName2, fullTableName2));

        //Testing corrupted view for a specific tenant
        assertEquals(0, runFindCorruptedViewTool(true, false, null, generateUniqueName()));
        assertEquals(0, runFindCorruptedViewTool(true, false, null, tenantId));

        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TENANT_ID, TABLE_SCHEM,TABLE_NAME, TABLE_TYPE, COLUMN_COUNT) VALUES('" +
                tenantId + "', '"+ schema2 + "', '"+ viewName5 + "','v', 0) ");
        conn.commit();
        assertEquals(1, runFindCorruptedViewTool(true, false, null, tenantId));

        //Testing get all corrupted view string list and write to a customized path/filename APIs
        FindViewCorruptionTool tool = getFindCorruptionTool();
        assertEquals(4, tool.run(getArgValues(true,false,null, null)));
        assertEquals(4, tool.getCorruptedViews().size());
        String randomFilename = generateUniqueName();
        tool.writeCorruptedViews(filePath, randomFilename);
        verifyLineCount(filePath + randomFilename, 4);
    }

    private Connection tenantConnection(String url, String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(url, props);
    }

    private void verifyLineCount(String fileName, long lineCount) throws IOException {
        LineNumberReader reader = new LineNumberReader(new FileReader(fileName));
        while (reader.readLine() != null) {
        }
        int count = reader.getLineNumber();
        if (count != lineCount) {
            System.out.println((count + " != " + lineCount));
        }
        assertTrue(count == lineCount);
    }

    public static String[] getArgValues(boolean getCorruptedViewCount, boolean outputPath,
                                        String schemaName, String tenantId) {
        final List<String> args = Lists.newArrayList();
        if (outputPath) {
            args.add("-op");
            args.add(filePath);
        }
        if (getCorruptedViewCount) {
            args.add("-c");
        }
        if (schemaName != null) {
            args.add("-s");
            args.add(schemaName);
        }
        if (tenantId != null) {
            args.add("-id");
            args.add(tenantId);
        }
        return args.toArray(new String[0]);
    }

    public static int runFindCorruptedViewTool(boolean getCorruptedViewCount, boolean outputPath,
                                               String schemaName, String tenantId)
            throws Exception {
        FindViewCorruptionTool tool = getFindCorruptionTool();
        final String[] cmdArgs =
                getArgValues(getCorruptedViewCount, outputPath, schemaName, tenantId);
        return tool.run(cmdArgs);
    }

    public static FindViewCorruptionTool getFindCorruptionTool() {
        FindViewCorruptionTool tool = new FindViewCorruptionTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        tool.setConf(conf);
        return tool;
    }

}
