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
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.impl.MetricRegistriesImpl;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.SpanReceiver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MRJobType;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.joda.time.Chronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;

import static org.apache.phoenix.query.QueryServices.IS_NAMESPACE_MAPPING_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_IS_NAMESPACE_MAPPING_ENABLED;

/**
 * Tool to collect table level statistics on HBase snapshot
 */
public class UpdateStatisticsTool extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateStatisticsTool.class);

    private static final Option TABLE_NAME_OPTION = new Option("t", "table", true,
            "Phoenix Table Name");
    private static final Option SNAPSHOT_NAME_OPTION = new Option("s", "snapshot", true,
            "HBase Snapshot Name");
    private static final Option RESTORE_DIR_OPTION = new Option("d", "restore-dir", true,
            "Restore Directory for HBase snapshot");
    private static final Option JOB_PRIORITY_OPTION = new Option("p", "job-priority", true,
            "Define job priority from 0(highest) to 4");
    private static final Option RUN_FOREGROUND_OPTION =
            new Option("runfg", "run-foreground", false,
                    "If specified, runs UpdateStatisticsTool in Foreground. Default - Runs the build in background");
    private static final Option MANAGE_SNAPSHOT_OPTION =
            new Option("ms", "manage-snapshot", false,
                    "Creates a new snapshot, runs the tool and deletes it");

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

    private String tableName;
    private String snapshotName;
    private Path restoreDir;
    private JobPriority jobPriority;
    private boolean manageSnapshot;
    private boolean isForeground;

    private Job job;

    @Override
    public int run(String[] args) throws Exception {
        try {
            parseArgs(args);
            preJobTask();
            configureJob();
            TableMapReduceUtil.initCredentials(job);
            int ret = runJob();
            postJobTask();
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Run any tasks before the MR job is launched
     * Currently being used for snapshot creation
     */
    private void preJobTask() throws Exception {
        if (!manageSnapshot) {
            return;
        }

        try (final Connection conn = ConnectionUtil.getInputConnection(getConf())) {
            Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            boolean namespaceMapping = getConf().getBoolean(IS_NAMESPACE_MAPPING_ENABLED,
                    DEFAULT_IS_NAMESPACE_MAPPING_ENABLED);
            String physicalTableName =  SchemaUtil.getPhysicalTableName(
                    tableName.getBytes(StandardCharsets.UTF_8),
                    namespaceMapping).getNameAsString();
            admin.snapshot(snapshotName, TableName.valueOf(physicalTableName));
            LOGGER.info("Successfully created snapshot " + snapshotName + " for " + physicalTableName);
        }
    }

    /**
     * Run any tasks before the MR job is completed successfully
     * Currently being used for snapshot deletion
     */
    private void postJobTask() throws Exception {
        if (!manageSnapshot) {
            return;
        }

        try (final Connection conn = ConnectionUtil.getInputConnection(getConf())) {
            Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            admin.deleteSnapshot(snapshotName);
            LOGGER.info("Successfully deleted snapshot " + snapshotName);
        }
    }

    void parseArgs(String[] args) {
        CommandLine cmdLine = null;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }

        if (getConf() == null) {
            setConf(HBaseConfiguration.create());
        }

        tableName = cmdLine.getOptionValue(TABLE_NAME_OPTION.getOpt());
        snapshotName = cmdLine.getOptionValue(SNAPSHOT_NAME_OPTION.getOpt());
        if (snapshotName == null) {
            snapshotName = "UpdateStatisticsTool_" + tableName + "_" + System.currentTimeMillis();
        }

        String restoreDirOptionValue = cmdLine.getOptionValue(RESTORE_DIR_OPTION.getOpt());
        if (restoreDirOptionValue == null) {
            restoreDirOptionValue = getConf().get(FS_DEFAULT_NAME_KEY) + "/tmp";
        }

        jobPriority = getJobPriority(cmdLine);

        restoreDir = new Path(restoreDirOptionValue);
        manageSnapshot = cmdLine.hasOption(MANAGE_SNAPSHOT_OPTION.getOpt());
        isForeground = cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt());
    }

    public String getJobPriority() {
        return this.jobPriority.toString();
    }

    private JobPriority getJobPriority(CommandLine cmdLine) {
        String jobPriorityOption = cmdLine.getOptionValue(JOB_PRIORITY_OPTION.getOpt());
         if (jobPriorityOption == null) {
             return JobPriority.NORMAL;
         }

         switch (jobPriorityOption) {
             case "0" : return JobPriority.VERY_HIGH;
             case "1" : return JobPriority.HIGH;
             case "2" : return JobPriority.NORMAL;
             case "3" : return JobPriority.LOW;
             case "4" : return JobPriority.VERY_LOW;
             default:
                 return JobPriority.NORMAL;
         }
    }

    private void configureJob() throws Exception {
        job = Job.getInstance(getConf(),
                "UpdateStatistics-" + tableName + "-" + snapshotName);
        PhoenixMapReduceUtil.setInput(job, NullDBWritable.class,
                snapshotName, tableName, restoreDir);

        PhoenixConfigurationUtil.setMRJobType(job.getConfiguration(), MRJobType.UPDATE_STATS);

        // DO NOT allow mapper splits using statistics since it may result into many smaller chunks
        PhoenixConfigurationUtil.setSplitByStats(job.getConfiguration(), false);

        job.setJarByClass(UpdateStatisticsTool.class);
        job.setMapperClass(TableSnapshotMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setPriority(this.jobPriority);

        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
                PhoenixConnection.class, Chronology.class, CharStream.class,
                SpanReceiver.class, Gauge.class, MetricRegistriesImpl.class);

        LOGGER.info("UpdateStatisticsTool running for: " + tableName
                + " on snapshot: " + snapshotName + " with restore dir: " + restoreDir);
    }

    private int runJob() {
        try {
            if (isForeground) {
                LOGGER.info("Running UpdateStatisticsTool in Foreground. " +
                        "Runs full table scans. This may take a long time!");
                return (job.waitForCompletion(true)) ? 0 : 1;
            } else {
                LOGGER.info("Running UpdateStatisticsTool in Background - Submit async and exit");
                job.submit();
                return 0;
            }
        } catch (Exception e) {
            LOGGER.error("Caught exception " + e + " trying to update statistics.");
            return 1;
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
    CommandLine parseOptions(String[] args) {

        final Options options = getOptions();

        CommandLineParser parser = DefaultParser.builder().
                setAllowPartialMatching(false).
                setStripLeadingAndTrailingQuotes(false).
                build();
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

        if (cmdLine.hasOption(MANAGE_SNAPSHOT_OPTION.getOpt())
                && !cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt())) {
            throw new IllegalStateException("Snapshot cannot be managed if job is running in background");
        }

        return cmdLine;
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(TABLE_NAME_OPTION);
        options.addOption(SNAPSHOT_NAME_OPTION);
        options.addOption(HELP_OPTION);
        options.addOption(RESTORE_DIR_OPTION);
        options.addOption(JOB_PRIORITY_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);
        options.addOption(MANAGE_SNAPSHOT_OPTION);
        return options;
    }

    public Job getJob() {
        return job;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public Path getRestoreDir() {
        return restoreDir;
    }

    /**
     * Empty Mapper class since stats collection happens as part of scanner object
     */
    public static class TableSnapshotMapper
            extends Mapper<NullWritable, NullDBWritable, NullWritable, NullWritable> {

        @Override
        protected void map(NullWritable key, NullDBWritable value,
                           Context context) {
        }
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new UpdateStatisticsTool(), args);
        System.exit(result);
    }
}
