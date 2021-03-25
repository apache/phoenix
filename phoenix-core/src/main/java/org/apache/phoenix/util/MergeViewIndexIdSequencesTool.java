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
package org.apache.phoenix.util;

import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeViewIndexIdSequencesTool extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergeViewIndexIdSequencesTool.class);


    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");
    private static final Option RUN_OPTION = new Option("r", "run", false,
            "Run MergeViewIndexIdSequencesTool to avoid view index id collision.");


    private Options getOptions() {
        final Options options = new Options();
        options.addOption(RUN_OPTION);
        options.addOption(HELP_OPTION);
        return options;
    }

    private void parseOptions(String[] args) throws Exception {

        final Options options = getOptions();

        CommandLineParser parser = new DefaultParser(false, false);
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!cmdLine.hasOption(RUN_OPTION.getOpt())) {
            printHelpAndExit("Please give at least one param", options);
        }

    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    private void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }


    @Override
    public int run(String[] args) throws Exception {
        int status = 0;
        PhoenixConnection conn = null;
        try {
            parseOptions(args);

            final Configuration config = HBaseConfiguration.addHbaseResources(getConf());

            conn = ConnectionUtil.getInputConnection(config).
                    unwrap(PhoenixConnection.class);

            UpgradeUtil.mergeViewIndexIdSequences(conn);

        } catch (Exception e) {
            LOGGER.error("Get an error while running MergeViewIndexIdSequencesTool, "
                    + e.getMessage());
            status = 1;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return status;
    }
    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new MergeViewIndexIdSequencesTool(), args);
        System.exit(result);
    }
}