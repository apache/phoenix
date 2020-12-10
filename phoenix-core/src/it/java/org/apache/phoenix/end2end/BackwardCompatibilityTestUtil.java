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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;

public final class BackwardCompatibilityTestUtil {
    public static final String SQL_DIR = "sql_files/";
    public static final String RESULTS_AND_GOLD_FILES_DIR = "gold_files/";
    public static final String COMPATIBLE_CLIENTS_JSON =
            "compatible_client_versions.json";
    public static final String BASH = "/bin/bash";
    public static final String EXECUTE_QUERY_SH = "scripts/execute_query.sh";
    public static final String QUERY_PREFIX = "query_";
    public static final String RESULT_PREFIX = "result_";
    public static final String GOLD_PREFIX = "gold_";
    public static final String SQL_EXTENSION = ".sql";
    public static final String TEXT_EXTENSION = ".txt";
    public static final String CREATE_ADD = "create_add";
    public static final String CREATE_TMP_TABLE = "create_tmp_table";
    public static final String CREATE_DIVERGED_VIEW = "create_diverged_view";
    public static final String ADD_DATA = "add_data";
    public static final String ADD_DELETE = "add_delete";
    public static final String ADD_VIEW_INDEX = "add_view_index";
    public static final String DELETE = "delete";
    public static final String DELETE_FOR_SPLITABLE_SYSCAT = "delete_for_splitable_syscat";
    public static final String VIEW_INDEX = "view_index";
    public static final String SELECT_AND_DROP_TABLE = "select_and_drop_table";
    public static final String QUERY_CREATE_ADD = QUERY_PREFIX + CREATE_ADD;
    public static final String QUERY_ADD_DATA = QUERY_PREFIX + ADD_DATA;
    public static final String QUERY_ADD_DELETE = QUERY_PREFIX + ADD_DELETE;
    public static final String QUERY_DELETE = QUERY_PREFIX + DELETE;
    public static final String QUERY_DELETE_FOR_SPLITTABLE_SYSCAT =
            QUERY_PREFIX + DELETE_FOR_SPLITABLE_SYSCAT;
    public static final String QUERY_SELECT_AND_DROP_TABLE = QUERY_PREFIX + SELECT_AND_DROP_TABLE;
    public static final String QUERY_CREATE_DIVERGED_VIEW = QUERY_PREFIX + CREATE_DIVERGED_VIEW;
    public static final String QUERY_VIEW_INDEX = QUERY_PREFIX + VIEW_INDEX;
    public static final String INDEX_REBUILD_ASYNC = "index_rebuild_async";
    public static final String QUERY_INDEX_REBUILD_ASYNC = QUERY_PREFIX
        + INDEX_REBUILD_ASYNC;
    public static final String MVN_HOME = "maven.home";
    public static final String JAVA_TMP_DIR = "java.io.tmpdir";

    public enum UpgradeProps {
        NONE,
        SET_MAX_LOOK_BACK_AGE
    }

    private BackwardCompatibilityTestUtil() {
    }

    public static List<String> computeClientVersions() throws Exception {
        String hbaseVersion = VersionInfo.getVersion();
        Pattern p = Pattern.compile("\\d+\\.\\d+");
        Matcher m = p.matcher(hbaseVersion);
        String hbaseProfile = null;
        if (m.find()) {
            hbaseProfile = m.group();
        }
        List<String> clientVersions = Lists.newArrayList();
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        try (InputStream inputStream = BackwardCompatibilityIT.class
                .getClassLoader().getResourceAsStream(COMPATIBLE_CLIENTS_JSON)) {
            assertNotNull(inputStream);
            JsonNode jsonNode = mapper.readTree(inputStream);
            JsonNode HBaseProfile = jsonNode.get(hbaseProfile);
            for (final JsonNode clientVersion : HBaseProfile) {
                clientVersions.add(clientVersion.textValue() + "-HBase-" + hbaseProfile);
            }
        }
        return clientVersions;
    }

    // Executes the queries listed in the operation file with a given client version
    public static void executeQueryWithClientVersion(String clientVersion, String operation,
                                                     String zkQuorum) throws Exception {
        List<String> cmdParams = Lists.newArrayList();
        cmdParams.add(BASH);
        // Note that auto-commit is true for queries executed via SQLline
        URL fileUrl = BackwardCompatibilityIT.class.getClassLoader().getResource(EXECUTE_QUERY_SH);
        assertNotNull(fileUrl);
        cmdParams.add(new File(fileUrl.getFile()).getAbsolutePath());
        cmdParams.add(zkQuorum);
        cmdParams.add(clientVersion);

        fileUrl = BackwardCompatibilityIT.class.getClassLoader()
                .getResource(SQL_DIR + operation + SQL_EXTENSION);
        assertNotNull(fileUrl);
        cmdParams.add(new File(fileUrl.getFile()).getAbsolutePath());
        fileUrl = BackwardCompatibilityIT.class.getClassLoader().getResource(
                RESULTS_AND_GOLD_FILES_DIR);
        assertNotNull(fileUrl);
        String resultFilePath = new File(fileUrl.getFile()).getAbsolutePath() + "/" +
                RESULT_PREFIX + operation + TEXT_EXTENSION;
        cmdParams.add(resultFilePath);
        cmdParams.add(System.getProperty(JAVA_TMP_DIR));

        if (System.getProperty(MVN_HOME) != null) {
            cmdParams.add(System.getProperty(MVN_HOME));
        }

        ProcessBuilder pb = new ProcessBuilder(cmdParams);
        final Process p = pb.start();
        final StringBuffer sb = new StringBuffer();
        //Capture the output stream if any from the execution of the script
        Thread outputStreamThread = new Thread() {
            @Override
            public void run() {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(p.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                } catch (final Exception e) {
                    sb.append(e.getMessage());
                }
            }
        };
        outputStreamThread.start();
        //Capture the error stream if any from the execution of the script
        Thread errorStreamThread = new Thread() {
            @Override
            public void run() {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(p.getErrorStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                } catch (final Exception e) {
                    sb.append(e.getMessage());
                }
            }
        };
        errorStreamThread.start();
        p.waitFor();
        assertEquals(String.format("Executing the query failed%s. Check the result file: %s",
                sb.length() > 0 ? sb.append(" with : ").toString() : "", resultFilePath),
                0, p.exitValue());
    }


    public static void checkForPreConditions(String compatibleClientVersion, Configuration conf) throws Exception {
        // For the first code cut of any major version, there wouldn't be any backward compatible
        // clients. Hence the test wouldn't run and just return true when the client
        // version to be tested is same as current version
        assumeFalse(compatibleClientVersion.contains(MetaDataProtocol.CURRENT_CLIENT_VERSION));
        // Make sure that cluster is clean before test execution with no system tables
        try (org.apache.hadoop.hbase.client.Connection conn =
                     ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            assertFalse(admin.tableExists(TableName.valueOf(QueryConstants.SYSTEM_SCHEMA_NAME,
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE)));
        }
    }

    // Saves the result set to a text file to be compared against the gold file for difference
    private static void saveResultSet(ResultSet rs, BufferedWriter br) throws Exception {
        ResultSetMetaData rsm = rs.getMetaData();
        int columnCount = rsm.getColumnCount();
        StringBuilder row = new StringBuilder(formatStringWithQuotes(rsm.getColumnName(1)));
        for (int i = 2; i <= columnCount; i++) {
            row.append(",").append(formatStringWithQuotes(rsm.getColumnName(i)));
        }
        br.write(row.toString());
        br.write("\n");
        while (rs.next()) {
            row = new StringBuilder(formatStringWithQuotes(rs.getString(1)));
            for (int i = 2; i <= columnCount; i++) {
                row.append(",").append(formatStringWithQuotes(rs.getString(i)));
            }
            br.write(row.toString());
            br.write("\n");
        }
    }

    private static String formatStringWithQuotes(String str) {
        return (str != null) ? String.format("\'%s\'", str) : "\'\'";
    }

    private static BufferedReader getBufferedReaderForResource(String relativePath)
            throws FileNotFoundException {
        URL fileUrl = BackwardCompatibilityTestUtil.class.getClassLoader().getResource(relativePath);
        assertNotNull(fileUrl);
        return new BufferedReader(new FileReader(new File(fileUrl.getFile())));
    }

    // Compares the result file against the gold file to match for the expected output
    // for the given operation
    public static void assertExpectedOutput(String result) throws Exception {
        List<String> resultFile = Lists.newArrayList();
        List<String> goldFile = Lists.newArrayList();
        String line;
        try (BufferedReader resultFileReader = getBufferedReaderForResource(
                RESULTS_AND_GOLD_FILES_DIR + RESULT_PREFIX + result + TEXT_EXTENSION)) {
            while ((line = resultFileReader.readLine()) != null) {
                resultFile.add(line.trim());
            }
        }
        try (BufferedReader goldFileReader = getBufferedReaderForResource(
                RESULTS_AND_GOLD_FILES_DIR + GOLD_PREFIX + result + TEXT_EXTENSION)) {
            while ((line = goldFileReader.readLine()) != null) {
                line = line.trim();
                if ( !(line.isEmpty() || line.startsWith("*") || line.startsWith("/"))) {
                    goldFile.add(line);
                }
            }
        }

        // We take the first line in gold file and match against the result file to exclude any
        // other WARNING messages that comes as a result of the query execution
        int index = resultFile.indexOf(goldFile.get(0));
        assertNotEquals("Mismatch found between gold file and result file", -1, index);
        resultFile = resultFile.subList(index, resultFile.size());
        assertEquals(goldFile, resultFile);
    }

    // Executes the SQL commands listed in the given operation file from the sql_files directory
    public static void executeQueriesWithCurrentVersion(String operation, String url,
                                                        UpgradeProps upgradeProps) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        if (UpgradeProps.SET_MAX_LOOK_BACK_AGE.equals(upgradeProps)) {
            // any value < 31 is enough to test relaxing the MaxLookBack age
            // checks during an upgrade because during upgrade, SCN for the
            // connection is set to be the phoenix version timestamp
            // (31 as of now: MIN_SYSTEM_TABLE_TIMESTAMP_4_16_0 / MIN_SYSTEM_TABLE_TIMESTAMP_5_1_0)
            // Hence, keeping value: 15
            props.put(CompatBaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                    Integer.toString(15));
        }

        try (Connection conn = DriverManager.getConnection(url, props)) {
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader =
                         getBufferedReaderForResource(SQL_DIR + operation + SQL_EXTENSION)) {
                String sqlCommand;
                while ((sqlCommand = reader.readLine()) != null) {
                    sqlCommand = sqlCommand.trim();
                    if (sqlCommand.length() == 0 || sqlCommand.startsWith("/")
                            || sqlCommand.startsWith("*")) {
                        continue;
                    }
                    sb.append(sqlCommand);
                }
            }
            ResultSet rs;
            String[] sqlCommands = sb.toString().split(";");

            URL fileUrl = BackwardCompatibilityIT.class.getClassLoader().getResource(
                    RESULTS_AND_GOLD_FILES_DIR);
            assertNotNull(fileUrl);
            final String resultFile = new File(fileUrl.getFile()).getAbsolutePath() + "/" +
                    RESULT_PREFIX + operation + TEXT_EXTENSION;
            try (BufferedWriter br = new BufferedWriter(new FileWriter(resultFile))) {
                for (String command : sqlCommands) {
                    try (PreparedStatement stmt = conn.prepareStatement(command)) {
                        stmt.execute();
                        rs = stmt.getResultSet();
                        if (rs != null) {
                            saveResultSet(rs, br);
                        }
                    }
                    conn.commit();
                }
            }
        }
    }
}
