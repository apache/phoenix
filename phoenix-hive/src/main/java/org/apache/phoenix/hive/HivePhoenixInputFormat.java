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
package org.apache.phoenix.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.hive.util.HiveConnectionUtil;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.*;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
* HivePhoenixInputFormat
* Need to extend the standard PhoenixInputFormat but also implement the mapred inputFormat for Hive compliance
*
* @version 1.0
* @since   2015-02-08 
*/

public class HivePhoenixInputFormat<T extends DBWritable> extends org.apache.phoenix.mapreduce.PhoenixInputFormat<T> implements org.apache.hadoop.mapred.InputFormat<NullWritable, T>{
    private static final Log LOG = LogFactory.getLog(HivePhoenixInputFormat.class);
    private Configuration configuration;
    private Connection connection;
    private QueryPlan queryPlan;
    
    
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        setConf(job);
        QueryPlan queryPlan = getQueryPlan();
        List allSplits = queryPlan.getSplits();
        Path path = new Path(job.get("location"));
        List splits = generateSplits(queryPlan, allSplits, path);
        FileSplit[] asplits = new FileSplit[splits.size()];
        LOG.debug("Splits size " + splits.size());
        splits.toArray(asplits);
        return asplits;
    }

    public RecordReader<NullWritable, T> getRecordReader(InputSplit split, JobConf job,
            Reporter reporter) throws IOException {
        setConf(job);
        QueryPlan queryPlan = getQueryPlan();

        Class inputClass = PhoenixConfigurationUtil.getInputClass(this.configuration);
        HivePhoenixRecordReader r = new HivePhoenixRecordReader(inputClass, this.configuration, queryPlan);
        try {
            r.init(split);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return r;
    }
    
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return this.configuration;
    }
    
    
    private QueryPlan getQueryPlan() throws IOException {
        try {
            LOG.debug("PhoenixInputFormat getQueryPlan statement "
                    + this.configuration.get("phoenix.select.stmt"));
            Connection connection = getConnection();
            String selectStatement =
                    PhoenixConfigurationUtil.getSelectStatement(this.configuration);
            Preconditions.checkNotNull(selectStatement);
            Statement statement = connection.createStatement();
            PhoenixStatement pstmt = (PhoenixStatement) statement.unwrap(PhoenixStatement.class);
            this.queryPlan = pstmt.compileQuery(selectStatement);
            this.queryPlan.iterator();
        } catch (Exception exception) {
            LOG.error(String.format("Failed to get the query plan with error [%s]",
                new Object[] { exception.getMessage() }));
            throw new RuntimeException(exception);
        }
        return this.queryPlan;
    }
    
    private List<FileSplit> generateSplits(QueryPlan qplan, List<KeyRange> splits, Path path)
            throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        List psplits = Lists.newArrayListWithExpectedSize(splits.size());
        StatementContext context = qplan.getContext();
        TableRef tableRef = qplan.getTableRef();
        for (KeyRange split : splits) {
            Scan splitScan = new Scan(context.getScan());

            if (tableRef.getTable().getBucketNum() != null) {
                LOG.error("Salted/bucketed Tables not yet supported");
                throw new IOException("Salted/bucketed Tables not yet supported");
            }

            if (ScanUtil.intersectScanRange(splitScan, split.getLowerRange(),
                split.getUpperRange(), context.getScanRanges().useSkipScanFilter())) {
                HivePhoenixInputSplit inputSplit =
                        new HivePhoenixInputSplit(KeyRange.getKeyRange(splitScan.getStartRow(),
                            splitScan.getStopRow()), path);
                psplits.add(inputSplit);
            }
        }
        return psplits;
    }
    
    private Connection getConnection() {
        try {
            if (this.connection == null) this.connection =
                    HiveConnectionUtil.getConnection(this.configuration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this.connection;
    }
    

}
