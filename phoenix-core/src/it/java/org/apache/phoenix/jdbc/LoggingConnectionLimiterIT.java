/**
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
package org.apache.phoenix.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.log.ConnectionLimiter;
import org.apache.phoenix.log.LoggingConnectionLimiter;
import org.apache.phoenix.query.BaseTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Random;


import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@Category(NeedsOwnMiniClusterTest.class)
public abstract class LoggingConnectionLimiterIT extends BaseTest {
    private static enum ActivityType {
        QUERY, LOAD
    }
    //private static final Logger LOG = LoggerFactory.getLogger(LoggingConnectionLimiterIT.class);
    private static final Instant NOW = Instant.now();
    private static final String tableName = generateUniqueName();
    private static final String UPSERT_SQL = "UPSERT INTO " + tableName + "(ORGANIZATION_ID, CLIENT_TYPE, GROUP_ID, MY_KEY, MY_VALUE, SIZE, NEXT_CHUNK, POD, CREATED_DATE, EXPIRY_DATE) values (?,?,?,?,?,?,?,?,?,?)";
    private static final String GROUP_CONDITION = "ORGANIZATION_ID=? and CLIENT_TYPE=? and GROUP_ID=?";
    private static final String KEY_CONDITION = GROUP_CONDITION + " and MY_KEY=?";

    private static final String SELECT_KEY_SQL = "SELECT EXPIRY_DATE, NEXT_CHUNK, MY_VALUE, CREATED_DATE FROM " + tableName + " WHERE " + KEY_CONDITION;
    protected static final String CREATE_TABLE_SQL = String.format("CREATE TABLE IF NOT EXISTS %s (  \n" +
            "  ORGANIZATION_ID CHAR(18) NOT NULL,  \n" +
            "  CLIENT_TYPE VARCHAR NOT NULL,  \n" +
            "  GROUP_ID VARCHAR NOT NULL,  \n" +
            "  MY_KEY VARCHAR NOT NULL,  \n" +
            "  MY_VALUE VARBINARY,  \n" +
            "  SIZE INTEGER,\n" +
            "  NEXT_CHUNK BOOLEAN,\n" +
            "  POD VARCHAR,  \n" +
            "  CREATED_DATE DATE,\n" +
            "  EXPIRY_DATE DATE,\n" +
            "  CONSTRAINT PK_DATA PRIMARY KEY   \n" +
            "  (  \n" +
            "    ORGANIZATION_ID,  \n" +
            "    CLIENT_TYPE,  \n" +
            "    GROUP_ID,  \n" +
            "    MY_KEY  \n" +
            "  )  \n" +
            ") IMMUTABLE_ROWS=true, VERSIONS=1, DISABLE_TABLE_SOR=true, REPLICATION_SCOPE=1, TTL=864000", tableName);

    protected static final String ORG_ID = "org000000000000001";
    protected static final String GROUP_ID = "groupId";

    @Rule
    public TestName testName = new TestName();

    @Test
    public void testWhenAllConnectionsClosed() throws Exception {
        /**
         * Case: when connections are closed
         * Expectation:
         * No failures due to throttling.
         * # of not closed connection = 0
         * logs contain o=upserted (indicating upsert operation)
         */

        int loadFailures = runSampleActivity(ActivityType.LOAD, 10, 100, 10, 0);
        ConnectionLimiter limiter = getConnectionLimiter();
        assertTrue(limiter instanceof LoggingConnectionLimiter);
        int count = ((LoggingConnectionLimiter) limiter).getConnectionCount();
        assertTrue("Should not have any failures", loadFailures == 0);
        assertTrue("Num connections not closed not as expected", count == 0);
        Map<String, String> logs = ((LoggingConnectionLimiter) limiter).getActivityLog();
        for (Map.Entry<String, String> logEntry : logs.entrySet()) {
            assertTrue(logEntry.getValue().contains("o=upserted"));
        }

    }

    @Test
    public void testActivityLogsOnUpsertsWhenNoFailures() throws Exception {
        /**
         * Case: when connections not closed is less than throttling threshold
         * Expectation:
         * No failures due to throttling.
         * # of not closed connection = numDoNotClose
         * logs contain o=upserted (indicating upsert operation)
         */

        int loadFailures = runSampleActivity(ActivityType.LOAD, 10, 100, 10, 10);
        ConnectionLimiter limiter = getConnectionLimiter();
        assertTrue(limiter instanceof LoggingConnectionLimiter);
        int count = ((LoggingConnectionLimiter) limiter).getConnectionCount();
        assertTrue("Should not have any failures", loadFailures == 0);
        assertTrue("Num connections not closed not as expected", count == 10);
        Map<String, String> logs = ((LoggingConnectionLimiter) limiter).getActivityLog();
        for (Map.Entry<String, String> logEntry : logs.entrySet()) {
            assertTrue(logEntry.getValue().contains("o=upserted"));
        }
    }

    @Test
    public void testActivityLogsOnQueryWhenNoFailures() throws Exception {
        /**
         * Case: when connections that are not closed is less than throttling threshold
         * Expectation:
         * No failures due to throttling.
         * # of not closed connection = numDoNotClose
         * logs contain o=queried (indicating query operation)
         */
        int queryFailures = runSampleActivity(ActivityType.QUERY, 10, 100, 10, 10);
        ConnectionLimiter limiter = getConnectionLimiter();
        assertTrue(limiter instanceof LoggingConnectionLimiter);
        int count = ((LoggingConnectionLimiter) limiter).getConnectionCount();
        assertTrue("Should not have any failures", queryFailures == 0);
        assertTrue("Num connections not closed not as expected", count == 10);
        Map<String, String> logs = ((LoggingConnectionLimiter) limiter).getActivityLog();
        for (Map.Entry<String, String> logEntry : logs.entrySet()) {
            assertTrue(logEntry.getValue().contains("o=queried"));
        }
    }

    @Test
    public void testActivityLogsOnUpsertWhenFailures() throws Exception {
        /**
         * Case: when connections not closed is => throttling threshold
         * Expectation:
         * Some failures due to throttling.
         * Throttling will kick when the # of active threads + # of not closed connection >= throttling threshold.
         * logs contain o=upserted (indicating upsert operation)
         */
        int loadFailures = runSampleActivity(ActivityType.LOAD, 10, 100, 10, 20);
        ConnectionLimiter limiter = getConnectionLimiter();
        assertTrue(limiter instanceof LoggingConnectionLimiter);
        int count = ((LoggingConnectionLimiter) limiter).getConnectionCount();
        assertTrue("Should have some failures", loadFailures > 0);
        assertTrue(String.format("Num connections not closed not as expected [expected >= %d, actual = %d", 10, count), count >= 10);
        Map<String, String> logs = ((LoggingConnectionLimiter) limiter).getActivityLog();
        for (Map.Entry<String, String> logEntry : logs.entrySet()) {
            assertTrue(logEntry.getValue().contains("o=upserted"));
        }

    }

    @Test
    public void testActivityLogsOnQueryWhenFailures() throws Exception {
        /**
         * Case: when connections not closed is => throttling threshold
         * Expectation:
         * Some failures due to throttling.
         * Throttling will kick when the # of active threads + # of not closed connection >= throttling threshold.
         * logs contain o=queried (indicating query operation)
         */
        int queryFailures = runSampleActivity(ActivityType.QUERY, 10, 100, 10, 20);
        ConnectionLimiter limiter = getConnectionLimiter();
        assertTrue(limiter instanceof LoggingConnectionLimiter);
        int count = ((LoggingConnectionLimiter) limiter).getConnectionCount();
        assertTrue("Should have some failures", queryFailures > 0);
        assertTrue(String.format("Num connections not closed not as expected [expected >= %d, actual = %d", 10, count), count >= 10);
        Map<String, String> logs = ((LoggingConnectionLimiter) limiter).getActivityLog();
        for (Map.Entry<String, String> logEntry : logs.entrySet()) {
            assertTrue(logEntry.getValue().contains("o=queried"));
        }

    }

    protected abstract ConnectionLimiter getConnectionLimiter() throws Exception ;

    protected int runSampleActivity(ActivityType activityType, int clientPool, int clientQueue, int numRows, int connNotClosed) throws Exception {
        /**
         * clientPool : number of active client threads
         * clientQueue : total number of client calls per test run
         */

        Random rnd = new Random();

        ExecutorService executorService = new ThreadPoolExecutor(clientPool, clientPool, 10,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(clientQueue));


        ArrayList<CompletableFuture<String>> clientCallList = new ArrayList<>();
        ArrayList<CompletableFuture<?>> mayHaveFailedList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(clientQueue);
        Set<Integer> skipCloseForClients = new HashSet<>();
        for (int i = 0;i < clientQueue && connNotClosed > 0;i++) {
            skipCloseForClients.add(rnd.nextInt(clientQueue));
            if (skipCloseForClients.size() == connNotClosed) break;
        }
        Set<Integer> skippedCloseForClients = new HashSet<>();
        for (int i = 0; i < clientQueue; i++) {

            CompletableFuture<String> mockCompletableFuture = new CompletableFuture<>();
            CompletableFuture<?> mayHaveException = mockCompletableFuture.whenCompleteAsync((r, e) -> {
                String resultInfo = activityType + " activity started";
                int index = Integer.parseInt(r);
                try {
                    int rowsToWrite = numRows;
                    String orgId = StringUtils.rightPad(BaseTest.generateUniqueName(), 15).substring(0, 15);
                    String groupId = testName.getMethodName();
                    //LOG.info("Client : " + resultInfo + ":" + r);
                    Connection connection = getConnection();
                    try {
                        connection.setAutoCommit(false);
                        switch (activityType) {
                            case LOAD:
                                loadData(connection, orgId, groupId, rowsToWrite, 20);
                                break;
                            case QUERY:
                                loadData(connection, orgId, groupId, rowsToWrite, 20);
                                queryData(connection, orgId, groupId);
                                break;
                            default:
                                break;
                        }
                        resultInfo = "activity completed";
                        //LOG.info("Client : " + resultInfo + ":" + r);
                    } finally {
                        if (!skipCloseForClients.contains(index)) {
                            connection.close();
                        }
                        if (skipCloseForClients.contains(index)) {
                            resultInfo = "skipped close";
                            //LOG.info("Client : " + resultInfo + ":" + r);
                            skippedCloseForClients.add(Integer.valueOf(r));
                        }
                    }
                } catch (SQLException ex) {
                    resultInfo = "failed sqle";
                    //LOG.error(resultInfo, ex);
                    throw new RuntimeException(ex);
                } catch (Exception ex) {
                    resultInfo = "failed general";
                    //LOG.error(resultInfo, ex);
                    throw new RuntimeException(ex);
                } finally {
                    latch.countDown();
                }
            }, executorService);
            mayHaveFailedList.add(mayHaveException);
            clientCallList.add(mockCompletableFuture);
        }

        // Explicitly complete the future (client call) to invoke open and close methods.
        for (int i = 0; i < clientCallList.size(); i++) {
            clientCallList.get(i).complete(String.valueOf(i));
        }
        // Wait for all client calls to finish
        latch.await();
        executorService.shutdown();

        AtomicInteger failedCount = new AtomicInteger();
        // Iterate thru client calls that may have failed.
        mayHaveFailedList.forEach(cf -> {
            cf.whenComplete((r, e) -> {
                if (e != null)  {
                    failedCount.incrementAndGet();
                    //LOG.info("Failed message: " + e.getMessage());
                } else {
                    //LOG.info("Success message: " + r);
                }
            });
        });

        return failedCount.get();
    }

    protected static void loadData(Connection connection, String orgId, String groupId, int rows, int batchSize) throws SQLException {
        Integer counter = 0;
        //See W-8064529 for reuse of the preparedstatement
        for (int i = 0; i < rows; i++) {
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_SQL)) {
                ps.setString(1, orgId);
                ps.setString(2, "CLIENT_TYPE");
                ps.setString(3, groupId);
                ps.setString(4, String.valueOf(counter++));
                ps.setBytes(5, new byte[]{counter.byteValue()});
                ps.setInt(6, 1);
                ps.setBoolean(7, false);
                ps.setString(8, "pod");
                ps.setTimestamp(9, Timestamp.from(NOW));
                ps.setTimestamp(10, Timestamp.from(NOW.plusSeconds(3600)));
                int result = ps.executeUpdate();
                if (result != 1) {
                    throw new RuntimeException("Phoenix error: upsert count is not one. It is " + result);
                }
            }
            if (!connection.getAutoCommit() && counter % batchSize == 0) {
                connection.commit();
            }
        }
        if (!connection.getAutoCommit()) {
            connection.commit(); //send any remaining rows
        }
    }

    protected void queryData(Connection connection, String orgId, String groupId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_KEY_SQL)) {

            statement.setString(1, orgId);
            statement.setString(2, "CLIENT_TYPE");
            statement.setString(3, groupId);
            statement.setString(4, "3");

            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            //counter gets incremented prior to putting value so 3+1=4
            assertArrayEquals(new byte[]{Integer.valueOf(4).byteValue()}, rs.getBytes(3));
            assertFalse(rs.next());
        }
    }

    protected abstract Connection getConnection() throws SQLException;

}
