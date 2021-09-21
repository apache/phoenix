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
package org.apache.phoenix.mapreduce.index;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.query.QueryServices;

/**
 * Mapper that does not do much as regions servers actually build the index from the data table regions directly
 */
public class PhoenixServerBuildIndexMapper extends
        Mapper<NullWritable, PhoenixServerBuildIndexDBWritable, ImmutableBytesWritable, IntWritable> {

    private long rebuildPageRowSize;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        String rebuildPageRowSizeConf =
                context.getConfiguration().get(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS);
        if (rebuildPageRowSizeConf != null) {
            this.rebuildPageRowSize = Long.parseLong(rebuildPageRowSizeConf);
        } else {
            this.rebuildPageRowSize = -1L;
        }
    }

    @Override
    protected void map(NullWritable key, PhoenixServerBuildIndexDBWritable record, Context context)
            throws IOException, InterruptedException {
        context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(record.getRowCount());
        if (this.rebuildPageRowSize != -1) {
            if (record.getRowCount() > this.rebuildPageRowSize) {
                throw new IOException("Rebuilt/Verified rows greater than page size. Rebuilt rows: "
                        + record.getRowCount() + " Page size: " + this.rebuildPageRowSize);
            }
        }
        // Make sure progress is reported to Application Master.
        context.progress();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new ImmutableBytesWritable(
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
                new IntWritable(0));
        super.cleanup(context);
    }
}
