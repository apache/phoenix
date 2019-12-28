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
package org.apache.phoenix.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Properties;


public class ViewTtlTool extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewTtlTool.class);

    public static final String RUNNING_FOR_DELETE_ALL_VIEWS_STRING = "RUNNING_FOR_DELETE_ALL_VIEWS";

    public static final int DEFAULT_MAPPER_SPLIT_SIZE = 10;

    private static final Option DELETE_ALL_VIEW_OPTION = new Option("a", "all", false,
            "Delete all views from all tables.");
    private static final Option TABLE_NAME_OPTION = new Option("t", "table", true,
            "Delete all children views from the Phoenix Table");
    private static final Option VIEW_NAME_OPTION = new Option("v", "view", true,
            "Delete Phoenix View Name");
    private static final Option TENANT_ID_OPTION = new Option("i", "id", true,
            "Delete an view based on the tenant id.");
    private static final Option JOB_PRIORITY_OPTION = new Option("p", "job-priority", true,
            "Define job priority from 0(highest) to 4");
    private static final Option SPLIT_SIZE_OPTION = new Option("s", "split-size-per-mapper", true,
            "Define split size for each mapper.");
    private static final Option RUN_FOREGROUND_OPTION = new Option("runfg",
            "run-foreground", false, "If specified, runs ViewTTLTool " +
            "in Foreground. Default - Runs the build in background");

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

    Configuration configuration;
    Connection connection;

    private String baseTableName;
    private String viewName;
    private String tenantId;
    private String jobName;
    private boolean isDeletingAllViews;
    private JobPriority jobPriority;
    private boolean isForeground;
    private int splitSize;
    private Job job;

    public void parseArgs(String[] args) {
        CommandLine cmdLine;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
            throw e;
        }

        if (getConf() == null) {
            setConf(HBaseConfiguration.create());
        }

        if (cmdLine.hasOption(DELETE_ALL_VIEW_OPTION.getOpt())) {
            this.isDeletingAllViews = true;
        } else if (cmdLine.hasOption(TABLE_NAME_OPTION.getOpt())) {
            baseTableName = cmdLine.getOptionValue(TABLE_NAME_OPTION.getOpt());
            this.isDeletingAllViews = false;
        } else if (cmdLine.hasOption(VIEW_NAME_OPTION.getOpt())) {
            viewName = cmdLine.getOptionValue(VIEW_NAME_OPTION.getOpt());
            this.isDeletingAllViews = false;
        }

        if (cmdLine.hasOption(TENANT_ID_OPTION.getOpt())) {
            tenantId = cmdLine.getOptionValue((TENANT_ID_OPTION.getOpt()));
        }

        jobPriority = getJobPriority(cmdLine);
        if (cmdLine.hasOption(SPLIT_SIZE_OPTION.getOpt())) {
            splitSize = Integer.valueOf(cmdLine.getOptionValue(SPLIT_SIZE_OPTION.getOpt()));
        } else {
            splitSize = DEFAULT_MAPPER_SPLIT_SIZE;
        }
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

    public boolean isDeletingAllViews() {
        return this.isDeletingAllViews;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public String getBaseTableName() {
        return this.baseTableName;
    }

    public String getViewName() {
        return this.viewName;
    }

    public int getSplitSize() {
        return this.splitSize;
    }

    public CommandLine parseOptions(String[] args) {
        final Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (!cmdLine.hasOption(DELETE_ALL_VIEW_OPTION.getOpt()) && !cmdLine.hasOption(TABLE_NAME_OPTION.getOpt())
                && !cmdLine.hasOption(VIEW_NAME_OPTION.getOpt()) && !cmdLine.hasOption(TENANT_ID_OPTION.getOpt())) {
            throw new IllegalStateException("No deletion job is specified, " +
                    "please indicate deletion job for ALL/TABLE/VIEW/TENANT level");
        }

        if (cmdLine.hasOption(TABLE_NAME_OPTION.getOpt()) && cmdLine.hasOption(VIEW_NAME_OPTION.getOpt())) {
            throw new IllegalStateException("Table and View name options cannot be set at the same time");
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

        this.jobPriority = getJobPriority(cmdLine);

        return cmdLine;
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(DELETE_ALL_VIEW_OPTION);
        options.addOption(TABLE_NAME_OPTION);
        options.addOption(VIEW_NAME_OPTION);
        options.addOption(TENANT_ID_OPTION);
        options.addOption(HELP_OPTION);
        options.addOption(JOB_PRIORITY_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);
        options.addOption(SPLIT_SIZE_OPTION);

        return options;
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

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobName() {
        if (this.jobName == null) {
            String jobName;
            if (this.isDeletingAllViews) {
                jobName = RUNNING_FOR_DELETE_ALL_VIEWS_STRING;
            } else if (this.getBaseTableName() != null) {
                jobName = this.getBaseTableName();
            } else if (this.getViewName() != null) {
                jobName = this.getViewName();
            } else  {
                jobName = this.tenantId;
            }
            this.jobName =  "ViewTTLTool-" + jobName + "-";
        }

        return this.jobName;
    }

    public void setViewTTLJobInputConfig(Configuration configuration) {
        if (this.isDeletingAllViews) {
            configuration.set(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_DELETE_JOB_ALL_VIEWS,
                    RUNNING_FOR_DELETE_ALL_VIEWS_STRING);
        } else if (this.getBaseTableName() != null) {
            configuration.set(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_DELETE_JOB_PER_TABLE,
                    this.baseTableName);
        } else if (this.getViewName() != null) {
            configuration.set(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_DELETE_JOB_PER_VIEW,
                    this.viewName);
        }

        if (this.tenantId != null) {
            configuration.set(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID, this.tenantId);
        }
    }

    public void configureJob() throws Exception {
        this.job = Job.getInstance(getConf(),getJobName() +  System.currentTimeMillis());
        PhoenixMapReduceUtil.setInput(job, this);

        job.setJarByClass(ViewTtlTool.class);
        job.setMapperClass(ViewTTLDeleteJobMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setPriority(this.jobPriority);

        TableMapReduceUtil.addDependencyJars(job);
        LOGGER.info("ViewTTLTool is running for " + job.getJobName());
    }

    public int runJob() {
        try {
            if (isForeground) {
                LOGGER.info("Running ViewTTLTool in Foreground. " +
                        "Runs full table scans. This may take a long time!");
                return (job.waitForCompletion(true)) ? 0 : 1;
            } else {
                LOGGER.info("Running ViewTTLTool in Background - Submit async and exit");
                job.submit();
                return 0;
            }
        } catch (Exception e) {
            LOGGER.error("Caught exception " + e + " trying to run ViewTTLTool.");
            return 1;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        connection = null;
        int ret;
        try {
            parseArgs(args);
            configuration = HBaseConfiguration.addHbaseResources(getConf());
            connection = ConnectionUtil.getInputConnection(configuration, new Properties());
            configureJob();
            TableMapReduceUtil.initCredentials(job);
            ret = runJob();
        } catch (Exception e) {
            printHelpAndExit(e.getMessage(), getOptions());
            return -1;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return ret;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new ViewTtlTool(), args);
        System.exit(result);
    }
}
