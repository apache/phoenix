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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.TransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Writes mutations directly to HBase using HBase front-door APIs.
 */
public class DirectHTableWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectHTableWriter.class);

    private Configuration conf = null;
    private Table table;
    private Connection conn;

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
            this.conn = ConnectionFactory.createConnection(this.conf);
            this.table = conn.getTable(TableName.valueOf(tableName));
            LOGGER.info("Created table instance for " + tableName);
        } catch (IOException e) {
            LOGGER.error("IOException : ", e);
            tryClosingResourceSilently(this.conn);
            throw new RuntimeException(e);
        }
    }

    public void write(List<Mutation> mutations) throws IOException, InterruptedException {
        Object[] results = new Object[mutations.size()];
        String txnIdStr = conf.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
        if (txnIdStr == null) {
            table.batch(mutations, results);
        } else {
            long ts = Long.parseLong(txnIdStr);
            PhoenixTransactionProvider provider = TransactionFactory.Provider.getDefault().getTransactionProvider();
            String txnProviderStr = conf.get(PhoenixConfigurationUtil.TX_PROVIDER);
            if (txnProviderStr != null) {
                provider = TransactionFactory.Provider.valueOf(txnProviderStr).getTransactionProvider();
            }
            List<Mutation> shadowedMutations = Lists.newArrayListWithExpectedSize(mutations.size());
            for (Mutation m : mutations) {
                if (m instanceof Put) {
                    shadowedMutations.add(provider.markPutAsCommitted((Put)m, ts, ts));
                }
            }
            table.batch(shadowedMutations, results);
        }
    }

    protected Configuration getConf() {
        return conf;
    }

    protected Table getTable() {
        return table;
    }

    private void tryClosingResourceSilently(Closeable res) {
        if (res != null) {
            try {
                res.close();
            } catch (IOException e) {
                LOGGER.error("Closing resource: " + res + " failed with error: ", e);
            }
        }
    }

    public void close() throws IOException {
        tryClosingResourceSilently(this.table);
        tryClosingResourceSilently(this.conn);
    }
}
