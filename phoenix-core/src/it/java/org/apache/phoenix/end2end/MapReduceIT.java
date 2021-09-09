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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.iterate.TestingMapReduceParallelScanGrouper;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.PhoenixTestingInputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test that our MapReduce basic tools work as expected
 */
@Category(ParallelStatsDisabledTest.class)
public class MapReduceIT extends ParallelStatsDisabledIT {

    private static final String STOCK_NAME = "STOCK_NAME";
    private static final String RECORDING_YEAR = "RECORDING_YEAR";
    private static final String RECORDINGS_QUARTER = "RECORDINGS_QUARTER";

    // We pre-split the table to ensure that we have multiple mappers.
    // This is used to test scenarios with more than 1 mapper
    private static final String CREATE_STOCK_TABLE = "CREATE TABLE IF NOT EXISTS %s ( " +
            " STOCK_NAME VARCHAR NOT NULL , RECORDING_YEAR  INTEGER NOT  NULL,  RECORDINGS_QUARTER " +
            " DOUBLE array[] CONSTRAINT pk PRIMARY KEY ( STOCK_NAME, RECORDING_YEAR )) "
            + "SPLIT ON ('AA')";

    private static final String CREATE_STOCK_VIEW = "CREATE VIEW IF NOT EXISTS %s (v1 VARCHAR) AS "
        + " SELECT * FROM %s WHERE RECORDING_YEAR = 2008";

    private static final String MAX_RECORDING = "MAX_RECORDING";
    private static final String CREATE_STOCK_STATS_TABLE =
            "CREATE TABLE IF NOT EXISTS %s(STOCK_NAME VARCHAR NOT NULL , "
                    + " MAX_RECORDING DOUBLE CONSTRAINT pk PRIMARY KEY (STOCK_NAME ))";


    private static final String UPSERT = "UPSERT into %s values (?, ?, ?)";

    private static final String TENANT_ID = "1234567890";

    @Before
    public void setupTables() throws Exception {

    }

    @After
    public void clearCountersForScanGrouper() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        TestingMapReduceParallelScanGrouper.clearNumCallsToGetRegionBoundaries();
        assertFalse("refCount leaked", refCountLeaked);
    }

    @Test
    public void testNoConditionsOnSelect() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createAndTestJob(conn, null, 91.04, null);
        }
    }

    @Test
    public void testConditionsOnSelect() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createAndTestJob(conn, RECORDING_YEAR + "  < 2009", 81.04, null);
        }
    }

    @Test
    public void testWithTenantId() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())){
            //tenant view will perform the same filter as the select conditions do in testConditionsOnSelect
            createAndTestJob(conn, null, 81.04, TENANT_ID);
        }

    }

    private void createAndTestJob(Connection conn, String s, double v, String tenantId) throws
            SQLException, IOException, InterruptedException, ClassNotFoundException {
        String stockTableName = generateUniqueName();
        String stockStatsTableName = generateUniqueName();
        conn.createStatement().execute(String.format(CREATE_STOCK_TABLE, stockTableName));
        conn.createStatement().execute(String.format(CREATE_STOCK_STATS_TABLE, stockStatsTableName));
        conn.commit();
        final Configuration conf = getUtility().getConfiguration();
        Job job = Job.getInstance(conf);
        if (tenantId != null) {
            setInputForTenant(job, tenantId, stockTableName, s);
        } else {
            PhoenixMapReduceUtil.setInput(job, StockWritable.class, PhoenixTestingInputFormat.class,
                    stockTableName, s, STOCK_NAME, RECORDING_YEAR, "0." + RECORDINGS_QUARTER);
        }
        testJob(conn, job, stockTableName, stockStatsTableName, v);

    }

    private void setInputForTenant(Job job, String tenantId, String stockTableName, String s) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, TENANT_ID);
        try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)){
            PhoenixMapReduceUtil.setTenantId(job, tenantId);
            String stockViewName = generateUniqueName();
            tenantConn.createStatement().execute(String.format(CREATE_STOCK_VIEW, stockViewName, stockTableName));
            tenantConn.commit();
            PhoenixMapReduceUtil.setInput(job, StockWritable.class, PhoenixTestingInputFormat.class,
                    stockViewName, s, STOCK_NAME, RECORDING_YEAR, "0." + RECORDINGS_QUARTER);
        }
    }

    private void testJob(Connection conn, Job job, String stockTableName, String stockStatsTableName, double expectedMax)
            throws SQLException, InterruptedException, IOException, ClassNotFoundException {
        assertEquals("Failed to reset getRegionBoundaries counter for scanGrouper", 0,
                TestingMapReduceParallelScanGrouper.getNumCallsToGetRegionBoundaries());
        upsertData(conn, stockTableName);

        // only run locally, rather than having to spin up a MiniMapReduce cluster and lets us use breakpoints
        job.getConfiguration().set("mapreduce.framework.name", "local");
        setOutput(job, stockStatsTableName);

        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputFormatClass(PhoenixOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(StockWritable.class);

        // run job
        assertTrue("Job didn't complete successfully! Check logs for reason.", job.waitForCompletion(true));

        // verify
        ResultSet stats = DriverManager.getConnection(getUrl()).createStatement()
                .executeQuery("SELECT * FROM " + stockStatsTableName);
        assertTrue("No data stored in stats table!", stats.next());
        String name = stats.getString(1);
        double max = stats.getDouble(2);
        assertEquals("Got the wrong stock name!", "AAPL", name);
        assertEquals("Max value didn't match the expected!", expectedMax, max, 0);
        assertFalse("Should only have stored one row in stats table!", stats.next());
        assertEquals("There should have been only be 1 call to getRegionBoundaries "
                        + "(corresponding to the driver code)", 1,
                TestingMapReduceParallelScanGrouper.getNumCallsToGetRegionBoundaries());
    }

    /**
     * Custom output setting because output upsert statement setting is broken (PHOENIX-2677)
     *
     * @param job to update
     */
    private void setOutput(Job job, String stockStatsTableName) {
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setOutputTableName(configuration, stockStatsTableName);
        configuration.set(PhoenixConfigurationUtil.UPSERT_STATEMENT, "UPSERT into " + stockStatsTableName +
                " (" + STOCK_NAME + ", " + MAX_RECORDING + ") values (?,?)");
        job.setOutputFormatClass(PhoenixOutputFormat.class);
    }

    private void upsertData(Connection conn, String stockTableName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(String.format(UPSERT, stockTableName));
        upsertData(stmt, "AAPL", 2009, new Double[]{85.88, 91.04, 88.5, 90.3});
        upsertData(stmt, "AAPL", 2008, new Double[]{75.88, 81.04, 78.5, 80.3});
        conn.commit();
    }

    private void upsertData(PreparedStatement stmt, String name, int year, Double[] data) throws SQLException {
        int i = 1;
        stmt.setString(i++, name);
        stmt.setInt(i++, year);
        Array recordings = new PhoenixArray.PrimitiveDoublePhoenixArray(PDouble.INSTANCE, data);
        stmt.setArray(i++, recordings);
        stmt.execute();
    }

    public static class StockWritable implements DBWritable {

        private String stockName;
        private double[] recordings;
        private double maxPrice;

        @Override
        public void readFields(ResultSet rs) throws SQLException {
            stockName = rs.getString(STOCK_NAME);
            final Array recordingsArray = rs.getArray(RECORDINGS_QUARTER);
            recordings = (double[]) recordingsArray.getArray();
        }

        @Override
        public void write(PreparedStatement pstmt) throws SQLException {
            pstmt.setString(1, stockName);
            pstmt.setDouble(2, maxPrice);
        }

        public double[] getRecordings() {
            return recordings;
        }

        public String getStockName() {
            return stockName;
        }

        public void setStockName(String stockName) {
            this.stockName = stockName;
        }

        public void setMaxPrice(double maxPrice) {
            this.maxPrice = maxPrice;
        }
    }

    /**
     * Extract the max price for each stock recording
     */
    public static class StockMapper extends Mapper<NullWritable, StockWritable, Text, DoubleWritable> {

        private Text stock = new Text();
        private DoubleWritable price = new DoubleWritable();

        @Override
        protected void map(NullWritable key, StockWritable stockWritable, Context context)
                throws IOException, InterruptedException {
            double[] recordings = stockWritable.getRecordings();
            final String stockName = stockWritable.getStockName();
            double maxPrice = Double.MIN_VALUE;
            for (double recording : recordings) {
                if (maxPrice < recording) {
                    maxPrice = recording;
                }
            }
            stock.set(stockName);
            price.set(maxPrice);
            context.write(stock, price);
        }
    }

    /**
     * Store the max price seen for each stock
     */
    public static class StockReducer extends Reducer<Text, DoubleWritable, NullWritable, StockWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> recordings, Context context)
                throws IOException, InterruptedException {
            double maxPrice = Double.MIN_VALUE;
            for (DoubleWritable recording : recordings) {
                if (maxPrice < recording.get()) {
                    maxPrice = recording.get();
                }
            }
            final StockWritable stock = new StockWritable();
            stock.setStockName(key.toString());
            stock.setMaxPrice(maxPrice);
            context.write(NullWritable.get(), stock);
        }

    }
}