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
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A Canary Tool to perform synthetic tests for Phoenix
 * It assumes that TEST.PQSTEST or the schema.table passed in the argument
 * is already present as following command
 * CREATE TABLE IF NOT EXISTS TEST.PQSTEST (mykey INTEGER NOT NULL
 * PRIMARY KEY, mycolumn VARCHAR, insert_date TIMESTAMP);
 *
 */
public class PhoenixCanaryTool extends Configured implements Tool {

    private static String TEST_SCHEMA_NAME = "TEST";
    private static String TEST_TABLE_NAME = "PQSTEST";
    private static String FQ_TABLE_NAME = "TEST.PQSTEST";
    private static Timestamp timestamp;
    private static final int MAX_CONNECTION_ATTEMPTS = 5;
    private final int FIRST_TIME_RETRY_TIMEOUT = 5000;
    private Sink sink = new StdOutSink();
    public static final String propFileName = "phoenix-canary-file-sink.properties";

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

    static class UpsertTableTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("upsertTable");
            // Insert data
            timestamp = new Timestamp(System.currentTimeMillis());
            String stmt = "UPSERT INTO " + FQ_TABLE_NAME
                    + "(mykey, mycolumn, insert_date) VALUES (?, ?, ?)";
            PreparedStatement ps = connection.prepareStatement(stmt);
            ps.setInt(1, 1);
            ps.setString(2, "Hello World");
            ps.setTimestamp(3, timestamp);
            ps.executeUpdate();
            connection.commit();
        }
    }

    static class ReadTableTest extends CanaryTest {
        void onExecute() throws Exception {
            result.setTestName("readTable");
            PreparedStatement ps = connection.prepareStatement("SELECT * FROM "
                    + FQ_TABLE_NAME+" WHERE INSERT_DATE = ?");
            ps.setTimestamp(1,timestamp);
            ResultSet rs = ps.executeQuery();

            int totalRows = 0;
            while (rs.next()) {
                totalRows += 1;
                Integer myKey = rs.getInt(1);
                String myColumn = rs.getString(2);
                if (myKey != 1 || !myColumn.equals("Hello World")) {
                    throw new Exception("Retrieved values do not " +
                            "match the inserted values");
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
     * Sink interface used by the canary to output information
     */
    public interface Sink {
        List<CanaryTestResult> getResults();

        void updateResults(CanaryTestResult result);

        void publishResults() throws Exception;

        void clearResults();
    }

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
        public void publishResults() {

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

    private static final Logger logger = LoggerFactory.getLogger(PhoenixCanaryTool.class);

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
                ("org.apache.phoenix.tool.PhoenixCanaryTool$StdOutSink").help
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
                logger.error("Argument parsing failed.");
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

            connection = getConnectionWithRetry(connectionURL);

            if (connection == null) {
                logger.error("Failed to get connection after multiple retries; the connection is null");
            }

            SimpleTimeLimiter limiter = new SimpleTimeLimiter();

            limiter.callWithTimeout(new Callable<Void>() {

                public Void call() {

                    sink.clearResults();

                    // Execute tests
                    logger.info("Starting UpsertTableTest");
                    sink.updateResults(new UpsertTableTest().runTest(connection));

                    logger.info("Starting ReadTableTest");
                    sink.updateResults(new ReadTableTest().runTest(connection));
                    return null;

                }
            }, timeoutVal, TimeUnit.SECONDS, true);

            long estimatedTime = System.currentTimeMillis() - startTime;

            appInfo.setExecutionTime(estimatedTime);
            appInfo.setSuccessful(true);

        } catch (Exception e) {
            logger.error(Throwables.getStackTraceAsString(e));
            appInfo.setMessage(Throwables.getStackTraceAsString(e));
            appInfo.setSuccessful(false);

        } finally {
            sink.updateResults(appInfo);
            sink.publishResults();
            connection.close();
        }

        return 0;
    }

    private Connection getConnectionWithRetry(String connectionURL) {
        Connection connection=null;
        try{
            connection = getConnectionWithRetry(connectionURL, true);
        } catch (Exception e) {
            logger.info("Failed to get connection with namespace enabled", e);
            try {
                connection = getConnectionWithRetry(connectionURL, false);
            } catch (Exception ex) {
                logger.info("Failed to get connection without namespace enabled", ex);
            }
        }
        return connection;
    }

    private Connection getConnectionWithRetry(String connectionURL, boolean namespaceFlag)
        throws Exception {
        Properties connProps = new Properties();
        Connection connection = null;

        connProps.setProperty("phoenix.schema.mapSystemTablesToNamespace", String.valueOf(namespaceFlag));
        connProps.setProperty("phoenix.schema.isNamespaceMappingEnabled", String.valueOf(namespaceFlag));

        RetryCounter retrier = new RetryCounter(MAX_CONNECTION_ATTEMPTS,
                FIRST_TIME_RETRY_TIMEOUT, TimeUnit.MILLISECONDS);
        logger.info("Trying to get the connection with "
                + retrier.getMaxAttempts() + " attempts with "
                + "connectionURL :" + connectionURL
                + "connProps :" + connProps);
        while (retrier.shouldRetry()) {
            try {
                connection = DriverManager.getConnection(connectionURL, connProps);
            } catch (SQLException e) {
                logger.info("Trying to establish connection with "
                        + retrier.getAttemptTimes() + " attempts", e);
            }
            if (connection != null) {
                logger.info("Successfully established connection within "
                        + retrier.getAttemptTimes() + " attempts");
                break;
            }
            retrier.sleepUntilNextRetry();
        }
        return connection;
    }

    public static void main(final String[] args) {
        try {
            logger.info("Starting Phoenix Canary Test tool...");
            ToolRunner.run(new PhoenixCanaryTool(), args);
        } catch (Exception e) {
            logger.error("Error in running Phoenix Canary Test tool. " + e);
        }
        logger.info("Exiting Phoenix Canary Test tool...");
    }
}
