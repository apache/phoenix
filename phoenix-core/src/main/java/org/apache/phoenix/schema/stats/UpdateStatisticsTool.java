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
package org.apache.phoenix.schema.stats;

import org.antlr.runtime.CharStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.impl.MetricRegistriesImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.SpanReceiver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.SchemaType;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.apache.thrift.transport.TTransportException;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClient;
import org.joda.time.Chronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateStatisticsTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateStatisticsTool.class);

    private static final Option TABLE_NAME_OPTION = new Option("t", "table", true,
            "Phoenix Table Name");
    private static final Option SNAPSHOT_NAME_OPTION = new Option("s", "snapshot", true,
            "HBase Snapshot Name");
    private static final Option RESTORE_DIR_OPTION = new Option("d", "restore-dir", true,
            "Restore Directory for HBase snapshot");
    private static final Option GPW_OPTION = new Option("w", "guide-posts-width", true,
            "Guide Posts width for stats collection");
    private static final Option RUN_FOREGROUND_OPTION =
            new Option("runfg", "run-foreground", false, "Applicable on top of -direct option."
                    + "If specified, runs index scrutiny in Foreground. Default - Runs the build in background.");
    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

    private Configuration conf;
    private String tableName;
    private String snapshotName;
    private Path restoreDir;
    private String guidePostWidth;
    private boolean isForeground;

    @Override
    public int run(String[] args) throws Exception {

        parseArgs(args);
        Job job = configureJob(conf, tableName, snapshotName, restoreDir, guidePostWidth);
        TableMapReduceUtil.initCredentials(job);
        return runJob(job, isForeground);
    }

    private void parseArgs(String[] args) {
        CommandLine cmdLine = null;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }

        conf = HBaseConfiguration.create();
        tableName = cmdLine.getOptionValue(TABLE_NAME_OPTION.getOpt());
        snapshotName = cmdLine.getOptionValue(SNAPSHOT_NAME_OPTION.getOpt());
        restoreDir = new Path(cmdLine.getOptionValue(RESTORE_DIR_OPTION.getOpt()));
        guidePostWidth = cmdLine.getOptionValue(GPW_OPTION.getOpt());
        isForeground = cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt());
    }

    Job configureJob(Configuration conf, String tableName,
                     String snapshotName, Path restoreDir, String guidePostWidth) throws Exception {
        if (guidePostWidth != null) {
            conf.set(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, guidePostWidth);
        }
        return configureJob(conf, tableName, snapshotName, restoreDir);
    }

    private Job configureJob(Configuration conf, String tableName,
                     String snapshotName, Path restoreDir) throws Exception {
        Job job = Job.getInstance(conf);
        PhoenixMapReduceUtil.setInput(job, PhoenixStatsCollectorWritable.class,
                snapshotName, tableName, restoreDir, SchemaType.UPDATE_STATS);
        // DO NOT allow mapper splits using statistics since it may result into many smaller chunks
        PhoenixConfigurationUtil.setSplitByStats(conf, false);

        job.setJarByClass(UpdateStatisticsTool.class);
        job.setMapperClass(TableSnapshotMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(), PhoenixConnection.class, Chronology.class,
                CharStream.class, TransactionSystemClient.class, TransactionNotInProgressException.class,
                ZKClient.class, DiscoveryServiceClient.class, ZKDiscoveryService.class,
                Cancellable.class, TTransportException.class, SpanReceiver.class, TransactionProcessor.class, Gauge.class, MetricRegistriesImpl.class);
        LOG.info("UpdateStatisticsTool running for: " + tableName
                + " on snapshot: " + snapshotName + " with restore dir: " + restoreDir + " GPW: " + guidePostWidth);

        return job;
    }

    int runJob(Job job, boolean isForeground) throws Exception {
        if (isForeground) {
            LOG.info("Running UpdateStatisticsTool in Foreground. " +
                    "Runs full table scans. This may take a long time!.");
            return (job.waitForCompletion(true)) ? 0 : 1;
        } else {
            LOG.info("Running UpdateStatisticsTool in Background - Submit async and exit");
            job.submit();
            return 0;
        }
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

    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing.
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    private CommandLine parseOptions(String[] args) {

        final Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!cmdLine.hasOption(TABLE_NAME_OPTION.getOpt())) {
            throw new IllegalStateException(TABLE_NAME_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

        if (!cmdLine.hasOption(SNAPSHOT_NAME_OPTION.getOpt())) {
            throw new IllegalStateException(SNAPSHOT_NAME_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

        return cmdLine;
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(TABLE_NAME_OPTION);
        options.addOption(SNAPSHOT_NAME_OPTION);
        options.addOption(HELP_OPTION);
        options.addOption(RESTORE_DIR_OPTION);
        options.addOption(GPW_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);
        return options;
    }

    public static class TableSnapshotMapper
            extends Mapper<NullWritable, PhoenixStatsCollectorWritable, NullWritable, NullWritable> {

        @Override
        protected void map(NullWritable key, PhoenixStatsCollectorWritable value,
                           Context context) {
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UpdateStatisticsTool(), args);
    }
}
