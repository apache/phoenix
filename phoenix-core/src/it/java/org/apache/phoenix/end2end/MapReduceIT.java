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
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;

import static org.junit.Assert.*;

/**
 * Test that our MapReduce basic tools work as expected
 */
public class MapReduceIT extends ParallelStatsDisabledIT {

    private static final String STOCK_NAME = "STOCK_NAME";
    private static final String RECORDING_YEAR = "RECORDING_YEAR";
    private static final String RECORDINGS_QUARTER = "RECORDINGS_QUARTER";
    private  String CREATE_STOCK_TABLE = "CREATE TABLE IF NOT EXISTS %s ( " +
            " STOCK_NAME VARCHAR NOT NULL , RECORDING_YEAR  INTEGER NOT  NULL,  RECORDINGS_QUARTER " +
            " DOUBLE array[] CONSTRAINT pk PRIMARY KEY ( STOCK_NAME, RECORDING_YEAR ))";

    private static final String MAX_RECORDING = "MAX_RECORDING";
    private  String CREATE_STOCK_STATS_TABLE =
            "CREATE TABLE IF NOT EXISTS %s(STOCK_NAME VARCHAR NOT NULL , "
                    + " MAX_RECORDING DOUBLE CONSTRAINT pk PRIMARY KEY (STOCK_NAME ))";
    private String UPSERT = "UPSERT into %s values (?, ?, ?)";

    @Before
    public void setupTables() throws Exception {

    }

    @Test
    public void testNoConditionsOnSelect() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String stockTableName = generateUniqueName();
        String stockStatsTableName = generateUniqueName();
        conn.createStatement().execute(String.format(CREATE_STOCK_TABLE, stockTableName));
        conn.createStatement().execute(String.format(CREATE_STOCK_STATS_TABLE, stockStatsTableName));
        conn.commit();
        final Configuration conf = getUtility().getConfiguration();
        Job job = Job.getInstance(conf);
        PhoenixMapReduceUtil.setInput(job, StockWritable.class, stockTableName, null,
                STOCK_NAME, RECORDING_YEAR, "0." + RECORDINGS_QUARTER);
        testJob(job, stockTableName, stockStatsTableName, 91.04);
    }

    @Test
    public void testConditionsOnSelect() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String stockTableName = generateUniqueName();
        String stockStatsTableName = generateUniqueName();
        conn.createStatement().execute(String.format(CREATE_STOCK_TABLE, stockTableName));
        conn.createStatement().execute(String.format(CREATE_STOCK_STATS_TABLE, stockStatsTableName));
        conn.commit();
        final Configuration conf = getUtility().getConfiguration();
        Job job = Job.getInstance(conf);
        PhoenixMapReduceUtil.setInput(job, StockWritable.class, stockTableName, RECORDING_YEAR+"  < 2009",
                STOCK_NAME, RECORDING_YEAR, "0." + RECORDINGS_QUARTER);
        testJob(job, stockTableName, stockStatsTableName, 81.04);
    }

    private void testJob(Job job, String stockTableName, String stockStatsTableName, double expectedMax)
            throws SQLException, InterruptedException, IOException, ClassNotFoundException {
        upsertData(stockTableName);

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

    private void upsertData(String stockTableName) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
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