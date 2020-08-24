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
package org.apache.phoenix.schema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.logging.Logger;

public class SchemaExtractionTool extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(SchemaExtractionTool.class.getName());
    private static final Option HELP_OPTION = new Option("h", "help",
            false, "Help");
    private static final Option TABLE_OPTION = new Option("tb", "table", true,
            "[Required] Table name ex. table1");
    private static final Option SCHEMA_OPTION = new Option("s", "schema", true,
            "[Optional] Schema name ex. schema");
    private static final Option TENANT_OPTION = new Option("t", "tenant", true,
            "[Optional] Tenant Id ex. abc");

    private String pTableName;
    private String pSchemaName;
    private String tenantId;

    public static Configuration conf;
    private String output;

    @Override
    public int run(String[] args) throws Exception {
        populateToolAttributes(args);
        conf = HBaseConfiguration.addHbaseResources(getConf());
        SchemaExtractionProcessor processor = new SchemaExtractionProcessor(tenantId,
                conf, pSchemaName, pTableName);
        output = processor.process();
        return 0;
    }

    public String getOutput() {
        return output;
    }

    private void populateToolAttributes(String[] args) {
        try {
            CommandLine cmdLine = parseOptions(args);
            pTableName = cmdLine.getOptionValue(TABLE_OPTION.getOpt());
            pSchemaName = cmdLine.getOptionValue(SCHEMA_OPTION.getOpt());
            tenantId = cmdLine.getOptionValue(TENANT_OPTION.getOpt());
            LOGGER.info("Schema Extraction Tool initiated: " + StringUtils.join( args, ","));
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
    }

    private CommandLine parseOptions(String[] args) {
        final Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("severe parsing command line options: " + e.getMessage(),
                    options);
        }
        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }
        if (!(cmdLine.hasOption(TABLE_OPTION.getOpt()))) {
            throw new IllegalStateException("Table name should be passed "
                    +TABLE_OPTION.getLongOpt());
        }
        return cmdLine;
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(TABLE_OPTION);
        SCHEMA_OPTION.setOptionalArg(true);
        options.addOption(SCHEMA_OPTION);
        TENANT_OPTION.setOptionalArg(true);
        options.addOption(TENANT_OPTION);
        return options;
    }

    private void printHelpAndExit(String severeMessage, Options options) {
        System.err.println(severeMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    public static void main (String[] args) throws Exception {
        int result = ToolRunner.run(new SchemaExtractionTool(), args);
        System.exit(result);
    }
}
