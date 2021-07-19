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
package org.apache.phoenix.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.phoenix.tools.util.PhckSupportedFeatureDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.phoenix.tools.util.PhckUtil;

import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.FIX_ALL_CORRUPTED_METADATA;
import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.HELP_USAGE_STRING;
import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.MISS_FIX_OR_MONITOR_OPTION_MSG;
import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.SYSTEM_LEVEL_TABLE;
import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.USER_LEVEL_TABLE;


public class Phck extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(Phck.class);
    private static final int SUCCEED_CODE = 0;
    private static final int FAILED_CODE = 1;

    private Configuration conf;

    public static void showUsage(Options options){
        HelpFormatter formatter = new HelpFormatter();

        formatter.printHelp("phck [OPTIONS] COMMAND <ARGS>",
                "Options:", options, PhckSupportedFeatureDocs.getCommandUsage());
    }

    private int doCommandLine(CommandLine commandLine) throws Exception {
        String[] commands = commandLine.getArgs();
        String command = commands[0];
        if(commands.length < 2){
            showErrorMessage(command + MISS_FIX_OR_MONITOR_OPTION_MSG);
            return FAILED_CODE;
        }
        PhckSystemLevelTool phckSystemLevelTool = new PhckSystemLevelTool(conf);
        PhckUserLevelTool phckUserLevelTool = new PhckUserLevelTool(conf);

        switch (command) {
            case SYSTEM_LEVEL_TABLE:
                phckSystemLevelTool.parserParam(PhckUtil.purgeFirst(commands));
                phckSystemLevelTool.run();
                break;
            case USER_LEVEL_TABLE:
                phckUserLevelTool.parserParam(PhckUtil.purgeFirst(commands));
                break;
            case FIX_ALL_CORRUPTED_METADATA:
                phckSystemLevelTool.parserParam(PhckUtil.purgeFirst(commands));
                phckUserLevelTool.parserParam(PhckUtil.purgeFirst(commands));

                break;
            default:
                showErrorMessage("Unsupported command: " + command);
                return FAILED_CODE;
        }

        return SUCCEED_CODE;
    }

    private static void showErrorMessage(String error) {
        if (error != null) {
            System.out.println(HELP_USAGE_STRING);
            System.out.println("ERROR: " + error);
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public int run(String[] args) {
        final Options options = new Options();

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine;

        try {
            commandLine = parser.parse(options, args, true);
            return doCommandLine(commandLine);
        } catch (Exception e) {
            showErrorMessage(e.getMessage());
            return FAILED_CODE;
        }
    }

    public Phck() {
    }

    public Phck(Configuration conf) {
//        super(conf);
        this.conf = conf;
    }

    public static void main(String [] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int errCode = org.apache.hadoop.util.ToolRunner.run(new Phck(conf), args);
        if (errCode != 0) {
            System.exit(errCode);
        }
    }
}
