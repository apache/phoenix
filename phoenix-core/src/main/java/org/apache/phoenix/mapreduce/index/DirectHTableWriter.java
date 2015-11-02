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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes mutations directly to HBase using HBase front-door APIs.
 */
public class DirectHTableWriter {
    private static final Logger LOG = LoggerFactory.getLogger(DirectHTableWriter.class);

    private Configuration conf = null;

    private HTable table;

    /** Job parameter that specifies the output table. */
    public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";

    /**
     * Optional job parameter to specify a peer cluster. Used specifying remote cluster when copying
     * between hbase clusters (the source is picked up from <code>hbase-site.xml</code>).
     * @see TableMapReduceUtil#initTableReducerJob(String, Class, org.apache.hadoop.mapreduce.Job,
     *      Class, String, String, String)
     */
    public static final String QUORUM_ADDRESS = "hbase.mapred.output.quorum";

    /** Optional job parameter to specify peer cluster's ZK client port */
    public static final String QUORUM_PORT = "hbase.mapred.output.quorum.port";

    /** Optional specification of the rs class name of the peer cluster */
    public static final String REGION_SERVER_CLASS = "hbase.mapred.output.rs.class";
    /** Optional specification of the rs impl name of the peer cluster */
    public static final String REGION_SERVER_IMPL = "hbase.mapred.output.rs.impl";

    public DirectHTableWriter(Configuration otherConf) {
        setConf(otherConf);
    }

    protected void setConf(Configuration otherConf) {
        this.conf = HBaseConfiguration.create(otherConf);

        String tableName = this.conf.get(OUTPUT_TABLE);
        if (tableName == null || tableName.length() <= 0) {
            throw new IllegalArgumentException("Must specify table name");
        }

        String address = this.conf.get(QUORUM_ADDRESS);
        int zkClientPort = this.conf.getInt(QUORUM_PORT, 0);
        String serverClass = this.conf.get(REGION_SERVER_CLASS);
        String serverImpl = this.conf.get(REGION_SERVER_IMPL);

        try {
            if (address != null) {
                ZKUtil.applyClusterKeyToConf(this.conf, address);
            }
            if (serverClass != null) {
                this.conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
            }
            if (zkClientPort != 0) {
                this.conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
            }
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
