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
package org.apache.phoenix.schema.tool;

import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public class SchemaTool extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaTool.class);
    private static final Option HELP_OPTION = new Option("h", "help",
            false, "Help");
    private static final Option MODE_OPTION = new Option("m", "mode", true,
            "[Required] Takes either synth or extract value");
    private static final Option DDL_OPTION = new Option("d", "ddl", true,
            "[Required with synth mode] SQL file that has one or more ddl statements"
                    + " for the same entity");
    private static final Option TABLE_OPTION = new Option("tb", "table", true,
            "[Required with extract mode] Table name ex. table1");
    private static final Option SCHEMA_OPTION = new Option("s", "schema", true,
            "[Optional] Schema name ex. schema");
    private static final Option TENANT_OPTION = new Option("t", "tenant", true,
            "[Optional] Tenant Id ex. abc");

    private String pTableName;
    private String pSchemaName;
    private String tenantId;
    private Enum mode;

    protected static Configuration conf;
    private String output;
    private String ddlFile;
    private String alterDDLFile;

    @Override
    public int run(String[] args) throws Exception {
        try {
            populateToolAttributes(args);
            SchemaProcessor processor=null;
            if(Mode.SYNTH.equals(mode)) {
                processor = new SchemaSynthesisProcessor(ddlFile);
            } else if(Mode.EXTRACT.equals(mode)) {
                conf = HBaseConfiguration.addHbaseResources(getConf());
                processor = new SchemaExtractionProcessor(tenantId, conf, pSchemaName, pTableName);
            } else {
                throw new Exception(mode+" is not accepted, provide [synth or extract]");
            }
            output = processor.process();
            LOGGER.info("Effective DDL with " + mode.toString() +": " + output);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public String getOutput() {
        return output;
    }

    private void populateToolAttributes(String[] args) {
        try {
            CommandLine cmdLine = parseOptions(args);
            mode = Mode.valueOf(cmdLine.getOptionValue(MODE_OPTION.getOpt()));
            ddlFile = cmdLine.getOptionValue(DDL_OPTION.getOpt());
            pTableName = cmdLine.getOptionValue(TABLE_OPTION.getOpt());
            pSchemaName = cmdLine.getOptionValue(SCHEMA_OPTION.getOpt());
            tenantId = cmdLine.getOptionValue(TENANT_OPTION.getOpt());
            LOGGER.info("Schema Tool initiated: " + StringUtils.join( args, ","));
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
    }

    @SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
            justification="null path call calls System.exit()")
    private CommandLine parseOptions(String[] args) {
        final Options options = getOptions();
        CommandLineParser parser = DefaultParser.builder().
                setAllowPartialMatching(false).
                setStripLeadingAndTrailingQuotes(false).
                build();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("severe parsing command line options: " + e.getMessage(),
                    options);
        }
        if(cmdLine == null) {
            printHelpAndExit("parsed command line object is null", options);
        }
        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }
        if (!(cmdLine.hasOption(TABLE_OPTION.getOpt()))
                && cmdLine.getOptionValue(MODE_OPTION.getOpt()).equalsIgnoreCase(Mode.EXTRACT.toString())) {
            throw new IllegalStateException("Table name should be passed with EXTRACT mode"
                    +TABLE_OPTION.getLongOpt());
        }
        if ((!(cmdLine.hasOption(DDL_OPTION.getOpt())))
                && cmdLine.getOptionValue(MODE_OPTION.getOpt()).equalsIgnoreCase(Mode.SYNTH.toString())) {
            throw new IllegalStateException("ddl option should be passed with SYNTH mode"
                    + DDL_OPTION.getLongOpt());
        }
        return cmdLine;
    }

    enum Mode {
        SYNTH,
        EXTRACT
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(TABLE_OPTION);
        options.addOption(MODE_OPTION);
        options.addOption(DDL_OPTION);
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
        int result = ToolRunner.run(new SchemaTool(), args);
        System.exit(result);
    }
}
