package org.apache.phoenix.end2end;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * Test case that uses multiple threads to read and write rows
 * from/into a table, verifying that reads never see partially-complete writes.
 * Meant to be a port of HBase's atomicity test.
 * Source: https://github.com/apache/hbase/blob/master/hbase-server/src/test/java/org/apache/hadoop/hbase/TestAcidGuarantees.java
 */
public class PhoenixHBaseSuiteAtomicityIT extends BaseHBaseManagedTimeIT {
    protected static final Log LOG = LogFactory.getLog(PhoenixHBaseSuiteAtomicityIT.class);

    //Two arbitrary values for writers to randomly alternate between.
    final int DataVal1 = 1023; //2^10 - 1
    final int DataVal2 = 33;   //2^5 - 1

    final String TableName = "TestAcidGuarantees";
    final String TestTable = "CREATE TABLE IF NOT EXISTS " + TableName +
            "(a_id INTEGER NOT NULL, " +
            "a_data INTEGER, " +
            "CONSTRAINT my_pk PRIMARY KEY (a_id))";

    /**
     * Thread that does random full-row writes into a table.
     */
    private class RandomWriter extends MultithreadedTestUtil.RepeatingTestThread {
        Random rand = new Random();
        Connection conn;
        String tableName;
        int data;
        AtomicLong numWritten = new AtomicLong();

        private RandomWriter(MultithreadedTestUtil.TestContext ctx, Connection conn, String tableName) throws IOException {
            super(ctx);
            this.conn = conn;
            this.tableName = tableName;
        }

        public void doAnAction() throws Exception {
            if(rand.nextBoolean()){
                data = DataVal1; //2^10 - 1
            } else {
                data = DataVal2; //2^5 + 1
            }

            // Pick a random row to write into
            String randomID = Integer.toString(rand.nextInt(50));
            synchronized (conn){
                conn.createStatement().execute("UPSERT INTO " + tableName +
                        " VALUES ("+randomID+","+Integer.toString(data)+")");
                conn.commit();
            }
            numWritten.getAndIncrement();
        }

    }

    /**
     * Thread that does scans of a table, looking for partially
     * completed rows.
     */
    private class RandomReader extends MultithreadedTestUtil.RepeatingTestThread {
        Connection conn;
        String tableName;
        int idOffset;
        int idRange;
        AtomicLong numRead = new AtomicLong();

        private RandomReader(MultithreadedTestUtil.TestContext ctx, Connection conn, String tableName, int idOffset, int idRange) throws IOException {
            super(ctx);
            this.conn = conn;
            this.tableName = tableName;
            this.idOffset = idOffset;
            this.idRange = idRange;
        }

        public void doAnAction() throws Exception {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT a_data FROM " + tableName +
                            " WHERE a_id >= " + Integer.toString(idOffset) +
                            " AND a_id < " + Integer.toString(idOffset + idRange));

            while (rs.next()) {
                int thisValue = rs.getInt(1);
                assertTrue(thisValue == DataVal1 || thisValue == DataVal2);
                numRead.getAndIncrement();
            }
        }

    }

    @Test
    public void testScanAtomicity() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String sql = TestTable;

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.execute();
        synchronized (conn){
            conn.commit();
        }

        MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(getTestClusterConfig());
        List<RandomWriter> testWriters = new ArrayList<RandomWriter>();
        List<RandomReader> testReaders = new ArrayList<RandomReader>();
        for(int i = 0; i<5; ++i){
            testWriters.add(new RandomWriter(ctx, conn, TableName));
            ctx.addThread(testWriters.get(i));
        }
        for(int i=0; i<4; ++i){
            testReaders.add(new RandomReader(ctx, conn, TableName, (25*i)%50, 25));
            ctx.addThread(testReaders.get(i));
        }
        ctx.startThreads();
        ctx.waitFor(3000);
        ctx.stop();

        LOG.info("Finished test.");
        LOG.info("Finished test. Writers:");
        for (RandomWriter writer : testWriters) {
            LOG.info("  wrote " + writer.numWritten.get());
        }
        LOG.info("Readers:");
        for (RandomReader reader : testReaders) {
            LOG.info("  read " + reader.numRead.get());
        }

    }
}
