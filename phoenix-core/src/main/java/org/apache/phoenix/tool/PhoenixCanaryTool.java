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
package org.apache.phoenix.tool;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A Canary Tool to perform synthetic tests for Query Server
 */
public class PhoenixCanaryTool extends Configured implements Tool {

    private static String TEST_SCHEMA_NAME = "TEST";
    private static String TEST_TABLE_NAME = "PQSTEST";
    private static String FQ_TABLE_NAME = "TEST.PQSTEST";
    private boolean USE_NAMESPACE = true;

    private Sink sink = new StdOutSink();

    /**
     * Base class for a Canary Test
     */
    private abstract static class CanaryTest {

        CanaryTestResult result = new CanaryTestResult();

        Connection connection = null;

        private void onCreate(Connection connection) {
            result.setTimestamp(getCurrentTimestamp());
            result.setStartTime(System.currentTimeMillis());
            this.connection = connection;
        }

        abstract void onExecute() throws Exception;

        private void onExit() {
            result.setExecutionTime(System.currentTimeMillis() - result.getStartTime());
        }

        CanaryTestResult runTest(Connection connection) {
            try {
                onCreate(connection);
                onExecute();
                result.setSuccessful(true);
                result.setMessage("Test " + result.getTestName() + " successful");
            } catch (Exception e) {
                result.setSuccessful(false);
                result.setMessage(Throwables.getStackTraceAsString(e));
            } finally {
                onExit();
            }
            return result;
        }
    }

    /**
     * Test which prepares environment before other tests run
     */
    static class PrepareTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("prepare");
            Statement statement = connection.createStatement();
            DatabaseMetaData dbm = connection.getMetaData();
            ResultSet tables = dbm.getTables(null, TEST_SCHEMA_NAME, TEST_TABLE_NAME, null);
            if (tables.next()) {
                // Drop test Table if exists
                statement.executeUpdate("DROP TABLE IF EXISTS " + FQ_TABLE_NAME);
            }

            // Drop test schema if exists
            if (TEST_SCHEMA_NAME != null) {
                statement = connection.createStatement();
                statement.executeUpdate("DROP SCHEMA IF EXISTS " + TEST_SCHEMA_NAME);
            }
        }
    }

    /**
     * Create Schema Test
     */
    static class CreateSchemaTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("createSchema");
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA_NAME);
        }
    }

    /**
     * Create Table Test
     */
    static class CreateTableTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("createTable");
            Statement statement = connection.createStatement();
            // Create Table
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS" + FQ_TABLE_NAME + " (mykey " + "INTEGER "
                    + "NOT " + "NULL PRIMARY KEY, " + "mycolumn VARCHAR)");
        }
    }

    /**
     * Upsert Data into Table Test
     */
    static class UpsertTableTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("upsertTable");
            // Insert data
            Statement statement = connection.createStatement();
            statement.executeUpdate("UPSERT INTO " + FQ_TABLE_NAME + " VALUES (1, " +
                    "'Hello" + " World')");
            connection.commit();
        }
    }

    /**
     * Read data from Table Test
     */
    static class ReadTableTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("readTable");
            // Query for table
            PreparedStatement ps = connection.prepareStatement("SELECT * FROM " + FQ_TABLE_NAME);
            ResultSet rs = ps.executeQuery();

            // Check correctness
            int totalRows = 0;
            while (rs.next()) {
                totalRows += 1;
                Integer myKey = rs.getInt(1);
                String myColumn = rs.getString(2);
                if (myKey != 1 || !myColumn.equals("Hello World")) {
                    throw new Exception("Retrieved values do not match the inserted " + "values");
                }
            }
            if (totalRows != 1) {
                throw new Exception(totalRows + " rows fetched instead of just one.");
            }
            ps.close();
            rs.close();
        }
    }

    /**
     * Delete test table Test
     */
    static class DeleteTableTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("deleteTable");
            Statement statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS" + FQ_TABLE_NAME);

            // Check if table dropped
            DatabaseMetaData dbm = connection.getMetaData();
            ResultSet tables = dbm.getTables(null, TEST_SCHEMA_NAME, TEST_TABLE_NAME, null);
            if (tables.next()) {
                throw new Exception("Test Table could not be dropped");
            }
        }
    }

    /**
     * Delete test Schema Test
     */
    static class DeleteSchemaTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("deleteSchema");
            Statement statement = connection.createStatement();
            statement.executeUpdate("DROP SCHEMA IF EXISTS " + TEST_SCHEMA_NAME);
        }
    }

    /**
     * Sink interface used by the canary to output information
     */
    public interface Sink {
        List<CanaryTestResult> getResults();

        void updateResults(CanaryTestResult result);

        void publishResults() throws Exception;

        void clearResults();
    }

    /**
     * Implementation of Std Out Sink
     */
    public static class StdOutSink implements Sink {
        private List<CanaryTestResult> results = new ArrayList<>();

        @Override
        public void updateResults(CanaryTestResult result) {
            results.add(result);
        }

        @Override
        public List<CanaryTestResult> getResults() {
            return results;
        }

        @Override
        public void publishResults() throws Exception {

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            String resultJson = gson.toJson(results);
            System.out.println(resultJson);
        }

        @Override
        public void clearResults() {
            results.clear();
        }
    }

    /**
     * Implementation of File Out Sink
     */
    public static class FileOutSink implements Sink {
        private List<CanaryTestResult> results = new ArrayList<>();
        File dir;
        String logfileName;
        String propFileName = "phoenix-canary-file-sink.properties";

        public FileOutSink() throws Exception {
            Properties prop = new Properties();
            InputStream input = ClassLoader.getSystemResourceAsStream(propFileName);
            if (input == null) {
                throw new Exception("Cannot load " + propFileName + " file for " + "FileOutSink.");
            }
            prop.load(input);
            logfileName = prop.getProperty("file.name");
            dir = new File(prop.getProperty("file.location"));
            dir.mkdirs();
        }

        @Override
        public void updateResults(CanaryTestResult result) {
            results.add(result);
        }

        @Override
        public List<CanaryTestResult> getResults() {
            return results;
        }

        @Override
        public void publishResults() throws Exception {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            String resultJson = gson.toJson(results);
            String fileName = logfileName + "-" + new SimpleDateFormat("yyyy.MM.dd.HH" + ".mm" +
                    ".ss").format(new Date()) + ".log";
            File file = new File(dir, fileName);
            Files.write(Bytes.toBytes(resultJson), file);
        }

        @Override
        public void clearResults() {
            results.clear();
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixCanaryTool.class);

    private static String getCurrentTimestamp() {
        return new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.ms").format(new Date());
    }

    private static Namespace parseArgs(String[] args) {

        ArgumentParser parser = ArgumentParsers.newFor("Phoenix Canary Test Tool").build()
                .description("Phoenix Canary Test Tool");

        parser.addArgument("--hostname", "-hn").type(String.class).nargs("?").help("Hostname on "
                + "which Phoenix is running.");

        parser.addArgument("--port", "-p").type(String.class).nargs("?").help("Port on " +
                "which Phoenix is running.");

        parser.addArgument("--constring", "-cs").type(String.class).nargs("?").help("Pass an " +
                "explicit connection String to connect to Phoenix. " +
                "default: jdbc:phoenix:thin:serialization=PROTOBUF;url=[hostName:port]");

        parser.addArgument("--timeout", "-t").type(String.class).nargs("?").setDefault("60").help
                ("Maximum time for which the app should run before returning error. default:" + "" +
                        " 60 sec");

        parser.addArgument("--testschema", "-ts").type(String.class).nargs("?").setDefault
                (TEST_SCHEMA_NAME).help("Custom name for the test table. " + "default: " +
                TEST_SCHEMA_NAME);

        parser.addArgument("--testtable", "-tt").type(String.class).nargs("?").setDefault
                (TEST_TABLE_NAME).help("Custom name for the test table." + " default: " +
                TEST_TABLE_NAME);

        parser.addArgument("--logsinkclass", "-lsc").type(String.class).nargs("?").setDefault
                ("PhoenixCanaryTool$StdOutSink").help
                ("Path to a Custom implementation for log sink class. default: stdout");

        Namespace res = null;
        try {
            res = parser.parseKnownArgs(args, null);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }
        return res;
    }

    private CanaryTestResult appInfo = new CanaryTestResult();
    private Connection connection = null;

    @Override
    public int run(String[] args) throws Exception {

        try {
            Namespace cArgs = parseArgs(args);
            if (cArgs == null) {
                LOG.error("Argument parsing failed.");
                throw new RuntimeException("Argument parsing failed");
            }

            final String hostName = cArgs.getString("hostname");
            final String port = cArgs.getString("port");
            final String timeout = cArgs.getString("timeout");
            final String conString = cArgs.getString("constring");
            final String testSchemaName = cArgs.getString("testschema");
            final String testTableName = cArgs.getString("testtable");
            final String logSinkClass = cArgs.getString("logsinkclass");

            TEST_TABLE_NAME = testTableName;
            TEST_SCHEMA_NAME = testSchemaName;
            FQ_TABLE_NAME = testSchemaName + "." + testTableName;

            // Check if at least one from host+port or con string is provided.
            if ((hostName == null || port == null) && conString == null) {
                throw new RuntimeException("Provide at least one from host+port or constring");
            }

            int timeoutVal = Integer.parseInt(timeout);

            // Dynamically load a class for sink
            sink = (Sink) ClassLoader.getSystemClassLoader().loadClass(logSinkClass).newInstance();

            long startTime = System.currentTimeMillis();

            String connectionURL = (conString != null) ? conString :
                    "jdbc:phoenix:thin:serialization=PROTOBUF;url=" + hostName + ":" + port;

            appInfo.setTestName("appInfo");
            appInfo.setMiscellaneous(connectionURL);

            Properties connProps = new Properties();
            connProps.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
            connProps.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");

            try {
                connection = DriverManager.getConnection(connectionURL, connProps);
            } catch (Exception e) {
                LOG.info("Namespace mapping cannot be set. Using default schema");
                USE_NAMESPACE = false;
                connection = DriverManager.getConnection(connectionURL);
                TEST_SCHEMA_NAME = null;
                FQ_TABLE_NAME = TEST_TABLE_NAME;
            }

            SimpleTimeLimiter limiter = new SimpleTimeLimiter();

            limiter.callWithTimeout(new Callable<Void>() {

                public Void call() {

                    sink.clearResults();

                    // Execute tests

                    LOG.info("Starting PrepareTest");
                    sink.updateResults(new PrepareTest().runTest(connection));

                    if (USE_NAMESPACE) {
                        LOG.info("Starting CreateSchemaTest");
                        sink.updateResults(new CreateSchemaTest().runTest(connection));
                    }

                    LOG.info("Starting CreateTableTest");
                    sink.updateResults(new CreateTableTest().runTest(connection));

                    LOG.info("Starting UpsertTableTest");
                    sink.updateResults(new UpsertTableTest().runTest(connection));

                    LOG.info("Starting ReadTableTest");
                    sink.updateResults(new ReadTableTest().runTest(connection));

                    LOG.info("Starting DeleteTableTest");
                    sink.updateResults(new DeleteTableTest().runTest(connection));

                    if (USE_NAMESPACE) {
                        LOG.info("Starting DeleteSchemaTest");
                        sink.updateResults(new DeleteSchemaTest().runTest(connection));
                    }
                    return null;
                }
            }, timeoutVal, TimeUnit.SECONDS, true);

            long estimatedTime = System.currentTimeMillis() - startTime;

            appInfo.setExecutionTime(estimatedTime);
            appInfo.setSuccessful(true);

        } catch (Exception e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            appInfo.setMessage(Throwables.getStackTraceAsString(e));
            appInfo.setSuccessful(false);

        } finally {
            sink.updateResults(appInfo);
            sink.publishResults();
            connection.close();
        }

        return 0;
    }

    public static void main(final String[] args) {
        int result = 0;
        try {
            LOG.info("Starting Phoenix Canary Test tool...");
            result = ToolRunner.run(new PhoenixCanaryTool(), args);
        } catch (Exception e) {
            LOG.error("Error in running Phoenix Canary Test tool. " + e);
        }
        LOG.info("Exiting Phoenix Canary Test tool...");
    }
}
