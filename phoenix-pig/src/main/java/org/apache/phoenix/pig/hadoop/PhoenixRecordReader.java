/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
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
package org.apache.phoenix.pig.hadoop;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.pig.PhoenixPigConfiguration;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.ColumnInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * RecordReader that process the scan and returns PhoenixRecord
 * 
 */
public final class PhoenixRecordReader extends RecordReader<NullWritable,PhoenixRecord>{
    
    private static final Log LOG = LogFactory.getLog(PhoenixRecordReader.class);
    private final PhoenixPigConfiguration phoenixConfiguration;
    private final QueryPlan queryPlan;
    private final List<ColumnInfo> columnInfos;
    private NullWritable key =  NullWritable.get();
    private PhoenixRecord value = null;
    private ResultIterator resultIterator = null;
    private PhoenixResultSet resultSet;
    
    public PhoenixRecordReader(final PhoenixPigConfiguration pConfiguration,final QueryPlan qPlan) throws SQLException {
        
        Preconditions.checkNotNull(pConfiguration);
        Preconditions.checkNotNull(qPlan);
        this.phoenixConfiguration = pConfiguration;
        this.queryPlan = qPlan;
        this.columnInfos = phoenixConfiguration.getSelectColumnMetadataList();
     }

    @Override
    public void close() throws IOException {
       if(resultIterator != null) {
           try {
               resultIterator.close();
        } catch (SQLException e) {
           LOG.error(" Error closing resultset.");
        }
       }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public PhoenixRecord getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        final PhoenixInputSplit pSplit = (PhoenixInputSplit)split;
        final KeyRange keyRange = pSplit.getKeyRange();
        final Scan splitScan = queryPlan.getContext().getScan();
        final Scan scan = new Scan(splitScan);
        scan.setStartRow(keyRange.getLowerRange());
        scan.setStopRow(keyRange.getUpperRange());
         try {
            this.resultIterator = new TableResultIterator(queryPlan.getContext(), queryPlan.getTableRef(),scan);
            this.resultSet = new PhoenixResultSet(this.resultIterator, queryPlan.getProjector(),queryPlan.getContext().getStatement());
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] initializing PhoenixRecordReader. ",e.getMessage()));
            Throwables.propagate(e);
        }
        
   }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = NullWritable.get();
        }
        if (value == null) {
            value =  new PhoenixRecord();
        }
        Preconditions.checkNotNull(this.resultSet);
        try {
            if(!resultSet.next()) {
                return false;
            }
            value.read(resultSet,columnInfos.size());
            return true;
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] occurred while iterating over the resultset. ",e.getMessage()));
            Throwables.propagate(e);
        }
        return false;
    }
}
