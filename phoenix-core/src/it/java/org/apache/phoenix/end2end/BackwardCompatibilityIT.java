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

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;

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
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;


/**
 * This class is meant for testing all compatible client versions 
 * against the current server version. It runs SQL queries with given 
 * client versions and compares the output against gold files
 */

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class BackwardCompatibilityIT {

    private static final String SQL_DIR = "sql_files/";
    private static final String RESULT_DIR = "gold_files/";
    private static final String COMPATIBLE_CLIENTS_JSON =
            "compatible_client_versions.json";
    private static final String BASH = "/bin/bash";
    private static final String EXECUTE_QUERY_SH = "scripts/execute_query.sh";
    private static final String RESULT_PREFIX = "result_";
    private static final String SQL_EXTENSION = ".sql";
    private static final String TEXT_EXTENSION = ".txt";
    private static final String CREATE_ADD = "create_add";
    private static final String ADD_DATA = "add_data";
    private static final String ADD_DELETE = "add_delete";
    private static final String QUERY = "query";
    private static final String QUERY_MORE = "query_more";
    private static final String QUERY_ADD_DELETE = "query_add_delete";
    private static final String MVN_HOME = "maven.home";
    private static final String JAVA_TMP_DIR = "java.io.tmpdir";

    private final String compatibleClientVersion;
    private static Configuration conf;
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static String url;
    private String tmpDir;

    public BackwardCompatibilityIT(String compatibleClientVersion) {
        this.compatibleClientVersion = compatibleClientVersion;
    }

    @Parameters(name = "BackwardCompatibilityIT_compatibleClientVersion={0}")
    public static synchronized Collection<String> data() throws Exception {
        return computeClientVersions();
    }

    @Before
    public synchronized void doSetup() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        conf = HBaseConfiguration.create();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        hbaseTestUtil.startMiniCluster();
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        checkForPreConditions();
    }
    
    @After
    public void cleanUpAfterTest() throws Exception {
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            hbaseTestUtil.shutdownMiniCluster();
        }
        System.setProperty("java.io.tmpdir", tmpDir);
    }
    
    private static List<String> computeClientVersions() throws Exception {
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

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testUpsertWithOldClient() throws Exception {
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD);
        executeQueriesWithCurrentVersion(QUERY);
        assertExpectedOutput(CREATE_ADD, QUERY);
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the old client
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testSelectWithOldClient() throws Exception {
        // Insert data with new client and read with old client
        executeQueriesWithCurrentVersion(CREATE_ADD);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY);
        assertExpectedOutput(CREATE_ADD, QUERY);
    }

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client 
     * 4. New Client inserts more data into the tables created by old client 
     * 5. Old Client reads the data inserted by new client
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testSelectUpsertWithNewClient() throws Exception {
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD);
        executeQueriesWithCurrentVersion(QUERY);
        assertExpectedOutput(CREATE_ADD, QUERY);

        // Insert more data with new client and read with old client
        executeQueriesWithCurrentVersion(ADD_DATA);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_MORE);
        assertExpectedOutput(ADD_DATA, QUERY_MORE);
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the old client 
     * 4. Old Client inserts more data into the tables created by old client 
     * 5. New Client reads the data inserted by new client
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testSelectUpsertWithOldClient() throws Exception {
        // Insert data with new client and read with old client
        executeQueriesWithCurrentVersion(CREATE_ADD);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY);
        assertExpectedOutput(CREATE_ADD, QUERY);

        // Insert more data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, ADD_DATA);
        executeQueriesWithCurrentVersion(QUERY_MORE);
        assertExpectedOutput(ADD_DATA, QUERY_MORE);
    }

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client 
     * 4. Old Client creates and deletes the data
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testUpsertDeleteWithOldClient() throws Exception {
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD);
        executeQueriesWithCurrentVersion(QUERY);
        assertExpectedOutput(CREATE_ADD, QUERY);

        // Deletes with the old client
        executeQueryWithClientVersion(compatibleClientVersion, ADD_DELETE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_ADD_DELETE);
        assertExpectedOutput(ADD_DELETE, QUERY_ADD_DELETE);
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the old client 
     * 4. New Client creates and deletes the data
     * 
     * @throws Exception thrown if any errors encountered during query execution or file IO
     */
    @Test
    public void testUpsertDeleteWithNewClient() throws Exception {
        // Insert data with old client and read with new client
        executeQueriesWithCurrentVersion(CREATE_ADD);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY);
        assertExpectedOutput(CREATE_ADD, QUERY);

        // Deletes with the new client
        executeQueriesWithCurrentVersion(ADD_DELETE);
        executeQueriesWithCurrentVersion(QUERY_ADD_DELETE);
        assertExpectedOutput(ADD_DELETE, QUERY_ADD_DELETE);
    }
    
    private void checkForPreConditions() throws Exception {
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

    // Executes the queries listed in the operation file with a given client version
    private void executeQueryWithClientVersion(String clientVersion, String operation)
            throws Exception {
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
        fileUrl = BackwardCompatibilityIT.class.getClassLoader().getResource(RESULT_DIR);
        assertNotNull(fileUrl);
        cmdParams.add(new File(fileUrl.getFile()).getAbsolutePath() + "/" +
                RESULT_PREFIX + operation + TEXT_EXTENSION);
        cmdParams.add(System.getProperty(JAVA_TMP_DIR));

        if (System.getProperty(MVN_HOME) != null) {
            cmdParams.add(System.getProperty(MVN_HOME));
        }

        ProcessBuilder pb = new ProcessBuilder(cmdParams);
        final Process p = pb.start();
        final StringBuffer sb = new StringBuffer();
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
        assertEquals(sb.toString(), 0, p.exitValue());
    }

    // Executes the SQL commands listed in the given operation file from the sql_files directory
    private void executeQueriesWithCurrentVersion(String operation) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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

            URL fileUrl = BackwardCompatibilityIT.class.getClassLoader().getResource(RESULT_DIR);
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

    // Saves the result set to a text file to be compared against the gold file for difference
    private void saveResultSet(ResultSet rs, BufferedWriter br) throws Exception {
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

    private String formatStringWithQuotes(String str) {
        return (str != null) ? String.format("\'%s\'", str) : "\'\'";
    }

    private BufferedReader getBufferedReaderForResource(String relativePath)
            throws FileNotFoundException {
        URL fileUrl = getClass().getClassLoader().getResource(relativePath);
        assertNotNull(fileUrl);
        return new BufferedReader(new FileReader(new File(fileUrl.getFile())));
    }

    // Compares the result file against the gold file to match for the expected output
    // for the given operation
    private void assertExpectedOutput(String gold, String result) throws Exception {
        List<String> resultFile = Lists.newArrayList();
        List<String> goldFile = Lists.newArrayList();
        String line;
        try (BufferedReader resultFileReader = getBufferedReaderForResource(
                RESULT_DIR + RESULT_PREFIX + result + TEXT_EXTENSION)) {
            while ((line = resultFileReader.readLine()) != null) {
                resultFile.add(line.trim());
            }
        }
        try (BufferedReader goldFileReader = getBufferedReaderForResource(
                RESULT_DIR + "gold_query_" + gold + TEXT_EXTENSION)) {
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
        resultFile = resultFile.subList(index, resultFile.size());
        assertEquals(resultFile, goldFile);
    }
}