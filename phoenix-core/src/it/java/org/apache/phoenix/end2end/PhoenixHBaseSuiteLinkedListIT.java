package org.apache.phoenix.end2end;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test case is based off of HBase's BigLinkedList IT.
 * Source: https://github.com/apache/hbase/blob/master/hbase-it/src/test/java/org/apache/hadoop/hbase/test/IntegrationTestBigLinkedList.java
 *
 * This test creates 250 000 nodes and connects them into a 10 000 linked lists. Once the linking process is finished,
 * the test runs through them and verifies that there are no broken links or holes, since these would
 * indicate data loss.
 *
 * Further Work: The idea is to have this test running while a background process begins shutting down system
 * modules, thus testing the robustness of the system.
 *
 * Note: If more processing power is available, the test can be scaled up.
 */
public class PhoenixHBaseSuiteLinkedListIT extends BaseHBaseManagedTimeIT {
    protected static final Log LOG = LogFactory.getLog(PhoenixHBaseSuiteLinkedListIT.class);
    final String tableName = "BigLinkedList";

    final String TestTable = "CREATE TABLE IF NOT EXISTS " + tableName +
            "(a_key INTEGER NOT NULL, " +
            "a_prev INTEGER NOT NULL, " +
            "CONSTRAINT my_pk PRIMARY KEY (a_key, a_prev))";

    /**
     * Thread that does full-row writes into a table.
     */
    private class NodeWriter extends MultithreadedTestUtil.TestThread {
        Connection conn;
        String tableName;
        int keyOffset;
        int keyRange;
        AtomicInteger prevKeyCounter;

        private NodeWriter(MultithreadedTestUtil.TestContext ctx, Connection conn, String tableName, int keyOffset, int keyRange, AtomicInteger prevKeyCounter) throws IOException {
            super(ctx);
            this.conn = conn;
            this.tableName = tableName;
            this.keyOffset = keyOffset;
            this.keyRange = keyRange;
            this.prevKeyCounter = prevKeyCounter;
        }

        public void doWork() throws SQLException {
            for (int n = keyOffset; n < (keyOffset + keyRange); ++n) {
                int prev = prevKeyCounter.getAndIncrement();
                synchronized (conn) {
                    conn.createStatement().execute("UPSERT INTO " + tableName +
                            " VALUES (" + Integer.toString(n) + "," + Integer.toString(prev) + ")");
                }
            }
            synchronized (conn) {
                conn.commit();
            }
        }
    }

    /**
     * Thread that does single-row reads in a table, looking for partially
     *
     * completed rows.
     */
    private class ListVerifier extends MultithreadedTestUtil.TestThread {
        Connection conn;
        String tableName;
        int keyOffset;
        int keyRange;
        int listLength;
        int numLists;

        private ListVerifier(MultithreadedTestUtil.TestContext ctx, Connection conn, String tableName, int keyOffset,
                             int keyRange, int listLength, int numLists) throws IOException {
            super(ctx);
            this.conn = conn;
            this.tableName = tableName;
            this.keyOffset = keyOffset;
            this.keyRange = keyRange;
            this.listLength = listLength;
            this.numLists = numLists;
        }

        public void doWork() throws SQLException {
                ResultSet rs = conn.createStatement().executeQuery(
                        "SELECT a_prev FROM " + tableName +
                                " WHERE a_key >= " + Integer.toString(keyOffset) +
                                " AND a_key < " + Integer.toString(keyOffset + keyRange));
                while (rs.next()) {
                    int prevKey = rs.getInt(1);
                    assertTrue(prevKey > (listLength-1)*numLists && prevKey <= listLength*numLists);

                    ResultSet nextNodeSeeker;
                    for(int i = 0; i < listLength-1;  ++i){
                        int nextKey;
                        nextNodeSeeker = conn.createStatement().executeQuery(
                                "SELECT a_prev FROM " + tableName +
                                        " WHERE a_key = " + Integer.toString(prevKey));
                        if(nextNodeSeeker.next()){
                            nextKey = nextNodeSeeker.getInt(1);
                            assertTrue(nextKey > ((listLength-2)-i)*numLists && nextKey <= ((listLength-1)-i)*numLists);
                            prevKey = nextKey;
                        } else {
                            fail("There is a hole in a linked list. Key " + Integer.toString(prevKey) + " resulted" +
                                    " in an empty query result.");
                        }
                    }
                }
        }
    }

    @Test
    public void testContinuousIngest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String query = TestTable;

        PreparedStatement statement = conn.prepareStatement(query);
        statement.execute();
        synchronized (conn){
            conn.commit();
        }

        //START: Test Configurations
        int startingOffset = 1;
        int listLength = 25;
        int numLists = 10000;
        int numWriters = 5;
        int keysPerWriter = numLists/numWriters;
        //END:   Test Configurations

        //START: Spawn Writer Threads.
        //       These prepare the node information in the form of table rows.
        MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(getTestClusterConfig());
        AtomicInteger sharedPrevKeyCounter = new AtomicInteger((listLength-1)*numLists+1);
        for(int i = 0; i<numWriters; ++i){
            ctx.addThread(new NodeWriter(ctx, conn, tableName, startingOffset+i*(keysPerWriter), (keysPerWriter), sharedPrevKeyCounter));
        }
        ctx.startThreads();

        for(int i = 1; i < listLength; ++i ){
            ctx.stop();
            if(!ctx.removeAllThreads()){
                LOG.error("Failed to clear the current array of threads inside the test context.", null);
                fail();
            }
            sharedPrevKeyCounter = new AtomicInteger(startingOffset+((i-1)*numLists));
            for(int j = 0; j<numWriters; ++j){
                ctx.addThread(new NodeWriter(ctx, conn, tableName, startingOffset+j*(keysPerWriter)+i*numLists,
                        (keysPerWriter), sharedPrevKeyCounter));
            }
            ctx.startThreads();
        }

        ctx.stop();
        if(!ctx.removeAllThreads()){
            LOG.error("Failed to clear the current array of threads inside the test context.", null);
            fail();
        }
        //END:   Spawn Writer Threads.

        //START: Spawn Verifier Threads.
        //       These traverse the linked lists and check for holes corresponding to data loss.
        for(int i=0; i<10; ++i){
            ctx.addThread(new ListVerifier(ctx, conn, tableName, (numLists/10)*i,(numLists/10), listLength, numLists));
        }
        ctx.startThreads();
        ctx.stop();
        //END:   Spawn Verifier Threads.


        LOG.info("Finished test.");
    }
}
