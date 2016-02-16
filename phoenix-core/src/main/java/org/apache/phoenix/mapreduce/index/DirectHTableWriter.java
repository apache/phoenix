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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes mutations directly to HBase using HBase front-door APIs.
 */
public class DirectHTableWriter {
    private static final Logger LOG = LoggerFactory.getLogger(DirectHTableWriter.class);

    private Configuration conf = null;

    private HTable table;

    public DirectHTableWriter(Configuration otherConf) {
        setConf(otherConf);
    }

    protected void setConf(Configuration otherConf) {
        this.conf = HBaseConfiguration.create(otherConf);

        String tableName = this.conf.get(TableOutputFormat.OUTPUT_TABLE);
        if (tableName == null || tableName.length() <= 0) {
            throw new IllegalArgumentException("Must specify table name");
        }

        try {
            this.table = new HTable(this.conf, tableName);
            this.table.setAutoFlush(false, true);
            LOG.info("Created table instance for " + tableName);
        } catch (IOException e) {
            LOG.error("IOException : ", e);
            throw new RuntimeException(e);
        }
    }

    public void write(List<Mutation> mutations) throws IOException, InterruptedException {
        Object[] results = new Object[mutations.size()];
        table.batch(mutations, results);
    }

    protected Configuration getConf() {
        return conf;
    }

    protected HTable getTable() {
        return table;
    }

    public void close() throws IOException {
        table.close();
    }
}
