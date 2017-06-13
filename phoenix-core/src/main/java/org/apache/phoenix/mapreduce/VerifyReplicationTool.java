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
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Map only job that compares data across a source and target table. The target table can be on the
 * same cluster or on a remote cluster. SQL conditions may be specified to compare only a subset of
 * both tables.
 */
public class VerifyReplicationTool implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyReplicationTool.class);

    static final Option
            ZK_QUORUM_OPT =
            new Option("z", "zookeeper", true, "ZooKeeper connection details (optional)");
    static final Option
            TABLE_NAME_OPT =
            new Option("t", "table", true, "Phoenix table name (required)");
    static final Option
            TARGET_TABLE_NAME_OPT =
            new Option("tt", "target-table", true, "Target Phoenix table name (optional)");
    static final Option
            TARGET_ZK_QUORUM_OPT =
            new Option("tz", "target-zookeeper", true,
                    "Target ZooKeeper connection details (optional)");
    static final Option
            CONDITIONS_OPT =
            new Option("c", "conditions", true,
                    "Conditions for select query WHERE clause (optional)");
    static final Option HELP_OPT = new Option("h", "help", false, "Show this help and quit");

    private Configuration conf;

    private String zkQuorum;
    private String tableName;
    private String targetTableName;
    private String targetZkQuorum;
    private String sqlConditions;

    VerifyReplicationTool(Configuration conf) {
        this.conf = Preconditions.checkNotNull(conf, "Configuration cannot be null");
    }

    public static Builder newBuilder(Configuration conf) {
        return new Builder(conf);
    }

    public static class Verifier
            extends Mapper<ImmutableBytesWritable, MultiTableResults, NullWritable, NullWritable> {

        public enum Counter {
            GOODROWS, BADROWS, ONLY_IN_SOURCE_TABLE_ROWS, ONLY_IN_TARGET_TABLE_ROWS,
            CONTENT_DIFFERENT_ROWS
        }

        @Override
        protected void map(ImmutableBytesWritable key, MultiTableResults value, Context context)
                throws IOException, InterruptedException {
            ImmutableBytesWritable targetKey = value.getTargetKey();
            if (value.getTargetKey() == null) {
                logFailRowAndIncrementCounter(context, Counter.ONLY_IN_SOURCE_TABLE_ROWS, key);
                return;
            }
            int keyCompare;
            if (key == null) {
                keyCompare = 1;
            } else {
                keyCompare = Bytes.compareTo(key.get(), targetKey.get());
            }
            if (keyCompare == 0) {
                // row keys match
                Map<String, Object> sourceResults = value.getSourceResults();
                Map<String, Object> targetResults = value.getTargetResults();
                boolean valuesMatch = true;
                if (sourceResults == null) {
                    if (targetResults != null) {
                        valuesMatch = false;
                    }
                } else if (!sourceResults.equals(targetResults)) {
                    valuesMatch = false;
                }
                if (!valuesMatch) {
                    logFailRowAndIncrementCounter(context, Counter.CONTENT_DIFFERENT_ROWS, key);
                    return;
                }
                context.getCounter(Counter.GOODROWS).increment(1);
            } else if (keyCompare < 0) { // row only exists in source table
                logFailRowAndIncrementCounter(context, Counter.ONLY_IN_SOURCE_TABLE_ROWS, key);
            } else { // row only exists in target table
                logFailRowAndIncrementCounter(context, Counter.ONLY_IN_TARGET_TABLE_ROWS,
                        targetKey);
            }
        }

        private void logFailRowAndIncrementCounter(Context context, Counter counter,
                ImmutableBytesWritable row) {
            context.getCounter(counter).increment(1);
            context.getCounter(Counter.BADROWS).increment(1);
            LOG.error(counter + ", row=" + Bytes.toStringBinary(row.get()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (!parseCommandLine(args)) {
            return 1;
        }
        return verify() ? 0 : 1;
    }

    public boolean verify() throws IOException, ClassNotFoundException, InterruptedException {
        checkState();
        Job job = createSubmittableJob();
        if (!job.waitForCompletion(true)) {
            LOG.info("Map-reduce job failed!");
            return false;
        }
        return true;
    }

    private void checkState() {
        // input table is required
        if (Strings.isNullOrEmpty(this.tableName)) {
            throw new IllegalStateException("A table name must be specified");
        }
        // either a target table or cluster must be specified
        if (Strings.isNullOrEmpty(this.targetTableName) && Strings
                .isNullOrEmpty(this.targetZkQuorum)) {
            throw new IllegalStateException(
                    "A target table name or ZooKeeper quorum must be specified");
        }
    }

    @VisibleForTesting
    Job createSubmittableJob() throws IOException {
        Job job = Job.getInstance(conf, "Phoenix VerifyReplication for " + tableName);
        job.setInputFormatClass(MultiTableInputFormat.class);
        job.setMapperClass(VerifyReplicationTool.Verifier.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        final Configuration conf = job.getConfiguration();
        PhoenixConfigurationUtil.setInputCluster(conf, zkQuorum);
        PhoenixConfigurationUtil.setInputTableName(conf, tableName);
        // add target table or cluster if specified
        if (!Strings.isNullOrEmpty(targetTableName)) {
            PhoenixConfigurationUtil.setInputTargetTableName(conf, targetTableName);
        } else if (!Strings.isNullOrEmpty(targetZkQuorum)) {
            PhoenixConfigurationUtil.setInputTargetCluster(conf, targetZkQuorum);
            Configuration peerConf = new Configuration(conf);
            peerConf.set(HConstants.ZOOKEEPER_QUORUM, targetZkQuorum);
            TableMapReduceUtil.initCredentialsForCluster(job, peerConf);
        }
        if (!Strings.isNullOrEmpty(sqlConditions)) {
            PhoenixConfigurationUtil.setInputTableConditions(conf, sqlConditions);
        }
        PhoenixConfigurationUtil.setSchemaType(conf, PhoenixConfigurationUtil.SchemaType.QUERY);
        TableMapReduceUtil.initCredentials(job);
        TableMapReduceUtil.addDependencyJars(job);
        return job;
    }

    @VisibleForTesting
    boolean parseCommandLine(String[] args) throws IOException {
        CommandLineParser parser = new PosixParser();
        Options options = getOptions();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPT.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!cmdLine.hasOption(TABLE_NAME_OPT.getOpt())) {
            throw new IllegalStateException(
                TABLE_NAME_OPT.getLongOpt() + " is a required parameter");
        }

        if (!cmdLine.getArgList().isEmpty()) {
            throw new IllegalArgumentException(
                    "Unexpected extra parameters: " + cmdLine.getArgList());
        }

        this.tableName = cmdLine.getOptionValue(TABLE_NAME_OPT.getOpt());
        this.zkQuorum =
                cmdLine.getOptionValue(ZK_QUORUM_OPT.getOpt(),
                        this.conf.get(HConstants.ZOOKEEPER_QUORUM));
        if (cmdLine.hasOption(TARGET_TABLE_NAME_OPT.getOpt())) {
            this.targetTableName = cmdLine.getOptionValue(TARGET_TABLE_NAME_OPT.getOpt());
        } else if (cmdLine.hasOption(TARGET_ZK_QUORUM_OPT.getOpt())) {
            this.targetZkQuorum = cmdLine.getOptionValue(TARGET_ZK_QUORUM_OPT.getOpt());
        } else {
            throw new IllegalStateException("Target table or target ZK quorum required");
        }
        this.sqlConditions = cmdLine.getOptionValue(CONDITIONS_OPT.getOpt());
        return true;
    }

    private void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    private Options getOptions() {
        Options options = new Options();
        options.addOption(ZK_QUORUM_OPT);
        options.addOption(TABLE_NAME_OPT);
        options.addOption(TARGET_TABLE_NAME_OPT);
        options.addOption(TARGET_ZK_QUORUM_OPT);
        options.addOption(CONDITIONS_OPT);
        options.addOption(HELP_OPT);
        return options;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new VerifyReplicationTool(HBaseConfiguration.create()), args);
        System.exit(ret);
    }

    public static class Builder {

        private VerifyReplicationTool tool;

        public Builder(Configuration conf) {
            this.tool = new VerifyReplicationTool(conf);
        }

        public Builder zkQuorum(String zkQuorum) {
            this.tool.zkQuorum = zkQuorum;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tool.tableName = tableName;
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            this.tool.targetTableName = targetTableName;
            return this;
        }

        public Builder targetZkQuorum(String targetZkQuorum) {
            this.tool.targetZkQuorum = targetZkQuorum;
            return this;
        }

        public Builder sqlConditions(String sqlConditions) {
            this.tool.sqlConditions = sqlConditions;
            return this;
        }

        public VerifyReplicationTool build() {
            tool.checkState();
            if (Strings.isNullOrEmpty(tool.zkQuorum)) {
                this.tool.zkQuorum = tool.conf.get(HConstants.ZOOKEEPER_QUORUM);
            }
            return tool;
        }
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public String getTargetZkQuorum() {
        return targetZkQuorum;
    }

    public String getSqlConditions() {
        return sqlConditions;
    }
}
