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
package org.apache.phoenix.jdbc;

import java.io.FileReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixHAAdmin.HighAvailibilityCuratorProvider;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The command line tool to manage high availability (HA) groups and their cluster roles.
 */
@Deprecated
public class PhoenixHAAdminTool extends Configured implements Tool {
    // Following are return value of this tool. We need this to be very explicit because external
    // system calling this tool may need to retry, alert or audit the operations of cluster roles.
    public static final int RET_SUCCESS = 0; // Saul Goodman
    public static final int RET_ARGUMENT_ERROR = 1; // arguments are invalid
    public static final int RET_SYNC_ERROR = 2; //  error to sync from manifest to ZK
    public static final int RET_REPAIR_FOUND_INCONSISTENCIES = 3; // error to repair current ZK

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixHAAdminTool.class);

    private static final Option HELP_OPT = new Option("h", "help", false, "Show this help");
    private static final Option FORCEFUL_OPT =
            new Option("F", "forceful", false,
                    "Forceful writing cluster role records ignoring errors on other clusters");
    private static final Option MANIFEST_OPT =
            new Option("m", "manifest", true, "Manifest file containing cluster role records");
    private static final Option LIST_OPT =
            new Option("l", "list", false, "List all HA groups stored on this ZK cluster");
    private static final Option REPAIR_OPT = new Option("r", "repair", false,
            "Verify all HA groups stored on this ZK cluster and repair if inconsistency found");
    @VisibleForTesting
    static final Options OPTIONS = new Options()
            .addOption(HELP_OPT)
            .addOption(FORCEFUL_OPT)
            .addOption(MANIFEST_OPT)
            .addOption(LIST_OPT)
            .addOption(REPAIR_OPT);

    @Override
    public int run(String[] args) throws Exception {
        CommandLine commandLine;
        try {
            commandLine = parseOptions(args);
        } catch (Exception e) {
            System.err.println(
                    "ERROR: Unable to parse command-line arguments " + Arrays.toString(args) + " due to: " + e);
            printUsageMessage();
            return RET_ARGUMENT_ERROR;
        }

        try {
            if (commandLine.hasOption(HELP_OPT.getOpt())) {
                printUsageMessage();
                return RET_SUCCESS;
            }
            String zkUrl = PhoenixHAAdmin.getLocalZkUrl(getConf()); // Admin is created against local ZK cluster
            if (commandLine.hasOption(LIST_OPT.getOpt())) { // list
                try (PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, getConf(), HighAvailibilityCuratorProvider.INSTANCE)) {
                    List<ClusterRoleRecord> records = admin.listAllClusterRoleRecordsOnZookeeper();
                    JacksonUtil.getObjectWriterPretty().writeValue(System.out, records);
                }
            } else if (commandLine.hasOption(MANIFEST_OPT.getOpt())) { // create or update
                String fileName = commandLine.getOptionValue(MANIFEST_OPT.getOpt());
                List<ClusterRoleRecord> records = readRecordsFromFile(fileName);
                boolean forceful = commandLine.hasOption(FORCEFUL_OPT.getOpt());
                try (PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, getConf(), HighAvailibilityCuratorProvider.INSTANCE)) {
                    Map<String, List<String>> failedHaGroups = admin.syncClusterRoleRecords(records, forceful);
                    if (!failedHaGroups.isEmpty()) {
                        System.out.println("Found following HA groups are failing to write the clusters:");
                        failedHaGroups.forEach((k, v) ->
                                System.out.printf("%s -> [%s]\n", k, String.join(",", v)));
                        return RET_SYNC_ERROR;
                    }
                }
            } else if (commandLine.hasOption(REPAIR_OPT.getOpt()))  { // verify and repair
                try (PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, getConf(), HighAvailibilityCuratorProvider.INSTANCE)) {
                    List<String> inconsistentRecord = admin.verifyAndRepairWithRemoteZnode();
                    if (!inconsistentRecord.isEmpty()) {
                        System.out.println("Found following inconsistent cluster role records: ");
                        System.out.print(String.join(",", inconsistentRecord));
                        return RET_REPAIR_FOUND_INCONSISTENCIES;
                    }
                }
            }
            return RET_SUCCESS;
        } catch(Exception e ) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Read cluster role records defined in the file, given file name.
     *
     * @param file The local manifest file name to read from
     * @return list of cluster role records defined in the manifest file
     * @throws Exception when parsing or reading from the input file
     */
    @VisibleForTesting
    List<ClusterRoleRecord> readRecordsFromFile(String file) throws Exception {
        Preconditions.checkArgument(!StringUtils.isEmpty(file));
        String fileType = FilenameUtils.getExtension(file);
        switch (fileType) {
        case "json":
            // TODO: use jackson or standard JSON library according to PHOENIX-5789
            try (Reader reader = new FileReader(file)) {
                ClusterRoleRecord[] records =
                        JacksonUtil.getObjectReader(ClusterRoleRecord[].class).readValue(reader);
                return Arrays.asList(records);
            }
        case "yaml":
            LOG.error("YAML file is not yet supported. See W-8274533");
        default:
            throw new Exception("Can not read cluster role records from file '" + file + "' " +
                    "reason: unsupported file type");
        }
    }

    /**
     * Parses the commandline arguments, throw exception if validation fails.
     *
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    @VisibleForTesting
    CommandLine parseOptions(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = parser.parse(OPTIONS, args);
        assert cmdLine != null;

        if ((cmdLine.hasOption(REPAIR_OPT.getOpt()) && cmdLine.hasOption(MANIFEST_OPT.getOpt()))
                || (cmdLine.hasOption(LIST_OPT.getOpt()) && cmdLine.hasOption(REPAIR_OPT.getOpt()))
                || (cmdLine.hasOption(LIST_OPT.getOpt()) && cmdLine.hasOption(MANIFEST_OPT.getOpt()))) {
            String msg = "--list, --manifest and --repair options are mutually exclusive";
            LOG.error(msg + " User provided args: {}", (Object[]) args);
            throw new IllegalArgumentException(msg);
        }

        if (cmdLine.hasOption(FORCEFUL_OPT.getOpt()) && !cmdLine.hasOption(MANIFEST_OPT.getOpt())) {
            String msg = "--forceful option only works with --manifest option";
            LOG.error(msg + " User provided args: {}", (Object[]) args);
            throw new IllegalArgumentException(msg);
        }

        return cmdLine;
    }

    /**
     * Print the usage message.
     */
    private void printUsageMessage() {
        GenericOptionsParser.printGenericCommandUsage(System.out);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", OPTIONS);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int retCode = ToolRunner.run(conf, new PhoenixHAAdminTool(), args);
        System.exit(retCode);
    }
}