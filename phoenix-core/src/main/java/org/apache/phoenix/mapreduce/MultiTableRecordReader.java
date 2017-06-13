/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Reads records from two tables simultaneously.
 */
public class MultiTableRecordReader
        extends RecordReader<ImmutableBytesWritable, MultiTableResults> {
    private static final Log LOG = LogFactory.getLog(MultiTableRecordReader.class);

    private Configuration conf;
    private PhoenixRecordReader<PhoenixRecordWritable> sourceReader;
    private QueryPlan targetQueryPlan;
    private ResultIterator targetResultIterator = null;
    private PhoenixResultSet targetResultSet;
    private ImmutableBytesWritable key;
    private MultiTableResults value;

    public MultiTableRecordReader(Configuration conf,
            QueryPlan sourceQueryPlan) {
        this.conf = conf;
        this.sourceReader =
                new PhoenixRecordReader<>(PhoenixRecordWritable.class, conf, sourceQueryPlan);
        try {
            this.targetQueryPlan = PhoenixMapReduceUtil.getQueryPlan(this.conf, true);
        } catch (SQLException e) {
            LOG.error("Could not create target QueryPlan", e);
            Throwables.propagate(e);
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        sourceReader.initialize(split, context);
        try {
            List<Scan> targetScans = ((MultiTableInputSplit) split).getTargetScans();
            this.targetResultIterator =
                    PhoenixMapReduceUtil
                            .initializeIterator(this.conf, targetScans, targetQueryPlan);
            this.targetResultSet =
                    PhoenixMapReduceUtil
                            .initializeResultSet(this.targetResultIterator, this.targetQueryPlan);
        } catch (SQLException e) {
            LOG.error("Could not initialize target ResultSet", e);
            Throwables.propagate(e);
        }
    }

    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public MultiTableResults getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return sourceReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        sourceReader.close();
        if (targetResultIterator != null) {
            try {
                targetResultIterator.close();
            } catch (SQLException e) {
                LOG.error("Could not close target ResultIterator", e);
                Throwables.propagate(e);
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        Preconditions.checkNotNull(this.targetResultSet);
        try {
            // result sets will be automatically closed if a previous
            // invocation of next() did not have a result
            boolean sourceHasNext =
                    !sourceReader.getResultSet().isClosed() && sourceReader.nextKeyValue();
            boolean targetHasNext = !targetResultSet.isClosed() && targetResultSet.next();
            if(!sourceHasNext && !targetHasNext) {
                return false;
            }
            PhoenixResultSet sourceResult = sourceReader.getResultSet();
            key = null;
            if (sourceResult.getCurrentRow() != null) {
                key = new ImmutableBytesWritable();
                sourceResult.getCurrentRow().getKey(key);
            }
            PhoenixRecordWritable targetResults = new PhoenixRecordWritable();
            if (targetHasNext) {
                targetResults.readFields(targetResultSet);
            }
            Map<String, Object> sourceValues = null;
            if (sourceReader.getCurrentValue() != null) {
                sourceValues = sourceReader.getCurrentValue().getResultMap();
            }
            ImmutableBytesWritable targetKey = null;
            if (targetResultSet.getCurrentRow() != null) {
                targetKey = new ImmutableBytesWritable();
                targetResultSet.getCurrentRow().getKey(targetKey);
            }
            Map<String, Object> targetValues = null;
            if (targetResults.getResultMap() != null) {
                targetValues = targetResults.getResultMap();
            }
            value = new MultiTableResults(key, sourceValues, targetKey, targetValues);
            return true;
        } catch (SQLException e) {
            LOG.error("Could not read results", e);
            Throwables.propagate(e);
        }
        return false;
    }
}
