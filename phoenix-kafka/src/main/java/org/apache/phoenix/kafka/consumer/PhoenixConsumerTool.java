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
package org.apache.phoenix.kafka.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixConsumerTool extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixConsumerTool.class);
    static final Option FILE_PATH_OPT = new Option("f", "file", true, "input file path");
    static final Option HELP_OPT = new Option("h", "help", false, "Show this help and quit");
    
    public static Options getOptions() {
        Options options = new Options();
        options.addOption(FILE_PATH_OPT);
        options.addOption(HELP_OPT);
        return options;
    }

    public static CommandLine parseOptions(String[] args) {

        Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPT.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!cmdLine.hasOption(FILE_PATH_OPT.getOpt())) {
            throw new IllegalStateException(FILE_PATH_OPT.getLongOpt() + " is a mandatory " + "parameter");
        }

        if (!cmdLine.getArgList().isEmpty()) {
            throw new IllegalStateException("Got unexpected extra parameters: " + cmdLine.getArgList());
        }

        return cmdLine;
    }

    public static void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    public static void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create(getConf());

        CommandLine cmdLine = null;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
        
        String path = cmdLine.getOptionValue(FILE_PATH_OPT.getOpt());
        conf.set("kafka.consumer.file", path);
        new PhoenixConsumer(conf);
        
        return 1;
    }
    
    public static void main(String[] args) throws Exception {
        int exitStatus = ToolRunner.run(new PhoenixConsumerTool(), args);
        System.exit(exitStatus);
    }
}
