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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This class is meant for testing all compatible client versions 
 * against the current server version. It runs SQL queries with given 
 * client versions and compares the output against gold files
 */

@RunWith(Parameterized.class)
public class BackwardCompatibilityIT extends BaseOwnClusterIT {

    private static final String SQL_DIR = "src/it/resources/sql_files/";
    private static final String RESULT_DIR = "src/it/resources/gold_files/";
    private static final String RESULT_PREFIX = "result_";
    private static final String SQL_EXTENSION = ".sql";
    private static final String TEXT_EXTENSION = ".txt";
    private static final String CREATE_ADD = "create_add";
    private static final String ADD_DATA = "add_data";
    private static final String ADD_DELETE = "add_delete";
    private static final String QUERY = "query";
    private static final String QUERY_ADD_DELETE = "query_add_delete";

    private final String compatibleClientVersion;

    public BackwardCompatibilityIT(String compatibleClientVersion) {
        this.compatibleClientVersion = compatibleClientVersion;
    }

    @Parameters(name = "BackwardCompatibilityIT_compatibleClientVersion={0}")
    public static synchronized Collection<String> data() {
        return MetaDataProtocol.COMPATIBLE_CLIENT_VERSIONS;
    }

    @Before
    public synchronized void doSetup() throws Exception {
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
    }

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client
     * 
     * @throws Exception
     */
    @Test
    public void testUpsertWithOldClient() throws Exception {
        checkForCurrentVersion();
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD);
        executeQueriesWithCurrentVersion(QUERY);
        assertEquals(true, compareOutput(CREATE_ADD, QUERY));
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the old client
     * 
     * @throws Exception
     */
    @Test
    public void testSelectWithOldClient() throws Exception {
        checkForCurrentVersion();
        // Insert data with new client and read with old client
        executeQueriesWithCurrentVersion(CREATE_ADD);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY);
        assertEquals(true, compareOutput(CREATE_ADD, QUERY));
    }

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client 
     * 4. New Client inserts more data into the tables created by old client 
     * 5. Old Client reads the data inserted by new client
     * 
     * @throws Exception
     */
    @Test
    public void testSelectUpsertWithNewClient() throws Exception {
        checkForCurrentVersion();
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD);
        executeQueriesWithCurrentVersion(QUERY);
        assertEquals(true, compareOutput(CREATE_ADD, QUERY));

        // Insert more data with new client and read with old client
        executeQueriesWithCurrentVersion(ADD_DATA);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY);
        assertEquals(true, compareOutput(ADD_DATA, QUERY));
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the old client 
     * 4. Old Client inserts more data into the tables created by old client 
     * 5. New Client reads the data inserted by new client
     * 
     * @throws Exception
     */
    @Test
    public void testSelectUpsertWithOldClient() throws Exception {
        checkForCurrentVersion();
        // Insert data with new client and read with old client
        executeQueriesWithCurrentVersion(CREATE_ADD);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY);
        assertEquals(true, compareOutput(CREATE_ADD, QUERY));

        // Insert more data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, ADD_DATA);
        executeQueriesWithCurrentVersion(QUERY);
        assertEquals(true, compareOutput(ADD_DATA, QUERY));
    }

    /**
     * Scenario: 
     * 1. Old Client connects to the updated server 
     * 2. Old Client creates tables and inserts data 
     * 3. New Client reads the data inserted by the old client 
     * 4. Old Client creates and deletes the data
     * 
     * @throws Exception
     */
    @Test
    public void testUpsertDeleteWithOldClient() throws Exception {
        checkForCurrentVersion();
        // Insert data with old client and read with new client
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_ADD);
        executeQueriesWithCurrentVersion(QUERY);
        assertEquals(true, compareOutput(CREATE_ADD, QUERY));

        // Deletes with the old client
        executeQueryWithClientVersion(compatibleClientVersion, ADD_DELETE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_ADD_DELETE);
        assertEquals(true, compareOutput(ADD_DELETE, QUERY_ADD_DELETE));
    }

    /**
     * Scenario: 
     * 1. New Client connects to the updated server 
     * 2. New Client creates tables and inserts data 
     * 3. Old Client reads the data inserted by the old client 
     * 4. New Client creates and deletes the data
     * 
     * @throws Exception
     */
    @Test
    public void testUpsertDeleteWithNewClient() throws Exception {
        checkForCurrentVersion();
        // Insert data with old client and read with new client
        executeQueriesWithCurrentVersion(CREATE_ADD);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY);
        assertEquals(true, compareOutput(CREATE_ADD, QUERY));

        // Deletes with the new client
        executeQueriesWithCurrentVersion(ADD_DELETE);
        executeQueriesWithCurrentVersion(QUERY_ADD_DELETE);
        assertEquals(true, compareOutput(ADD_DELETE, QUERY_ADD_DELETE));
    }
    
    private void checkForCurrentVersion() {
        // For the first code cut of any major version, there wouldn't be any backward compatible
        // clients. Hence the test wouldn't run and just return true when the client  
        // version to be tested is same as current version
        assumeFalse(compatibleClientVersion.contains(MetaDataProtocol.CURRENT_CLIENT_VERSION));
    }

    // Executes the queries listed in the operation file with a given client version
    private void executeQueryWithClientVersion(String clientVersion, String operation)
            throws Exception {
        String BASH = "/bin/bash";
        String EXECUTE_QUERY_SH = "src/it/scripts/execute_query.sh";

        List<String> cmdParams = Lists.newArrayList();
        cmdParams.add(BASH);
        cmdParams.add(EXECUTE_QUERY_SH);
        cmdParams.add(getZkUrl());
        cmdParams.add(clientVersion);

        cmdParams.add(new File(SQL_DIR + operation + SQL_EXTENSION).getAbsolutePath());
        cmdParams.add(
            new File(RESULT_DIR + RESULT_PREFIX + operation + TEXT_EXTENSION).getAbsolutePath());
        cmdParams.add(System.getProperty("java.io.tmpdir"));

        if (System.getProperty("maven.home") != null) {
            cmdParams.add(System.getProperty("maven.home"));
        }

        ProcessBuilder pb = new ProcessBuilder(cmdParams);
        final Process p = pb.start();
        final StringBuffer sb = new StringBuffer();
        //Capture the error stream if any from the execution of the script
        Thread errorStreamThread = new Thread() {
            @Override
            public void run() {
                try {
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(p.getErrorStream()));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                    reader.close();
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        };
        errorStreamThread.start();
        assertEquals(sb.toString(), 0, p.waitFor());
    }

    // Executes the queries listed in the given operation file from the sql_files directory
    private ResultSet executeQueriesWithCurrentVersion(String operation) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        StringBuilder sb = new StringBuilder();
        BufferedReader reader =
                new BufferedReader(new FileReader(SQL_DIR + operation + SQL_EXTENSION));
        String query;
        ResultSet rs = null;
        while ((query = reader.readLine()) != null) {
            query = query.trim();
            if (query.length() == 0 || query.startsWith("/") || query.startsWith("*")) continue;
            sb.append(query);
        }
        reader.close();
        
        String[] queries = sb.toString().split(";");
        BufferedWriter br = new BufferedWriter(
                        new FileWriter(RESULT_DIR + RESULT_PREFIX + operation + TEXT_EXTENSION));
        for (int i = 0; i < queries.length; i++) {
            PreparedStatement stmt = conn.prepareStatement(queries[i]);
            stmt.execute();
            rs = stmt.getResultSet();
            if (rs != null) {
                saveResultSet(rs, br);
            }
            conn.commit();
        }
        br.close();
        conn.close();
        return rs;
    }

    // Saves the result set to a text file to be compared against the gold file for difference
    private void saveResultSet(ResultSet rs, BufferedWriter br) throws Exception {
        ResultSetMetaData rsm = rs.getMetaData();
        int columnCount = rsm.getColumnCount();
        String row = formatStringWithQuotes(rsm.getColumnName(1));
        for (int i = 2; i <= columnCount; i++) {
            row = row + "," + formatStringWithQuotes(rsm.getColumnName(i));
        }
        br.write(row);
        br.write("\n");
        while (rs.next()) {
            row = formatStringWithQuotes(rs.getString(1));
            for (int i = 2; i <= columnCount; i++) {
                row = row + "," + formatStringWithQuotes(rs.getString(i));
            }
            br.write(row);
            br.write("\n");
        }
    }

    private String formatStringWithQuotes(String str) {
        return (str != null) ? String.format("\'%s\'", str) : "\'\'";
    }

    // Compares the result file against the gold file to match for the expected output
    // for the given operation
    private boolean compareOutput(String gold, String result) throws Exception {
        BufferedReader goldFileReader = new BufferedReader(new FileReader(
                        new File(RESULT_DIR + "gold_query_" + gold + TEXT_EXTENSION)));
        BufferedReader resultFileReader = new BufferedReader(new FileReader(
                        new File(RESULT_DIR + RESULT_PREFIX + result + TEXT_EXTENSION)));

        List<String> resultFile = Lists.newArrayList();
        List<String> goldFile = Lists.newArrayList();

        String line = null;
        while ((line = resultFileReader.readLine()) != null) {
            resultFile.add(line.trim());
        }
        resultFileReader.close();

        while ((line = goldFileReader.readLine()) != null) {
            line = line.trim();
            if ( !(line.isEmpty() || line.startsWith("*") || line.startsWith("/"))) {
                goldFile.add(line);
            }           
        }
        goldFileReader.close();

        // We take the first line in gold file and match against the result file to exclude any
        // other WARNING messages that comes as a result of the query execution
        int index = resultFile.indexOf(goldFile.get(0));
        resultFile = resultFile.subList(index, resultFile.size());
        return resultFile.equals(goldFile);
    }
}