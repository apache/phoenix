/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce.index.fsck;

import java.io.PrintStream;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.fsck.RowKeyFormatter.KeyFormat;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;

/**
 * Command line utility for secondary index verification, consistency checking (fsck), repair, and
 * introspection.
 * <p>
 * Dispatches operations to the appropriate {@link IndexFsckProvider} for the target index type.
 * Returns process exit code 0 when clean or warnings only, 1 when findings contain errors, and -1
 * on execution failure.
 */
public class IndexFsckTool extends Configured implements Tool {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexFsckTool.class);

  public static final String CMD_VERIFY = "verify";
  public static final String CMD_FSCK = "fsck";
  public static final String CMD_REPAIR = "repair";
  public static final String CMD_INSPECT = "inspect";
  private static final List<String> COMMANDS =
    Arrays.asList(CMD_VERIFY, CMD_FSCK, CMD_REPAIR, CMD_INSPECT);

  private static final Option SCHEMA_NAME_OPTION =
    new Option("s", "schema", true, "Phoenix schema name (optional)");
  private static final Option DATA_TABLE_OPTION =
    new Option("dt", "data-table", true, "Data table name (mandatory)");
  private static final Option INDEX_TABLE_OPTION =
    new Option("it", "index-table", true, "Index table name (mandatory)");
  private static final Option TENANT_ID_OPTION =
    new Option("tenant", "tenant-id", true, "Tenant id, for a tenant view index (optional)");
  private static final Option START_TIME_OPTION =
    new Option("st", "start-time", true, "Start time of row verification in millis (optional)");
  private static final Option END_TIME_OPTION =
    new Option("et", "end-time", true, "End time of row verification in millis (optional)");
  private static final Option OUTPUT_FORMAT_OPTION =
    new Option("of", "output-format", true, "TEXT or JSON (default TEXT)");
  private static final Option KEYS_OPTION =
    new Option("k", "keys", true, "Row key rendering, DECODED or HEX (default DECODED)");
  private static final Option CONFIRM_OPTION = new Option("confirm", "confirm", false,
    "Apply the repair plan; without it repair is a dry run");
  private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

  private Report lastReport;
  private PrintStream out = System.out;

  public Report getLastReport() {
    return lastReport;
  }

  public void setOutStream(PrintStream out) {
    this.out = out;
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption(SCHEMA_NAME_OPTION);
    options.addOption(DATA_TABLE_OPTION);
    options.addOption(INDEX_TABLE_OPTION);
    options.addOption(TENANT_ID_OPTION);
    options.addOption(START_TIME_OPTION);
    options.addOption(END_TIME_OPTION);
    options.addOption(OUTPUT_FORMAT_OPTION);
    options.addOption(KEYS_OPTION);
    options.addOption(CONFIRM_OPTION);
    options.addOption(HELP_OPTION);
    return options;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = getOptions();
    CommandLine cmdLine;
    try {
      cmdLine = DefaultParser.builder().setAllowPartialMatching(false).build().parse(options, args);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printHelp(options);
      return -1;
    }
    List<String> positional = cmdLine.getArgList();
    if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
      printHelp(options);
      return 0;
    }
    if (positional.isEmpty() || !COMMANDS.contains(positional.get(0))) {
      System.err.println("The first argument must be one of " + COMMANDS);
      printHelp(options);
      return -1;
    }
    String command = positional.get(0);
    String inspectCommand = null;
    List<String> inspectArgs = positional.subList(1, positional.size());
    if (CMD_INSPECT.equals(command)) {
      if (inspectArgs.isEmpty()) {
        System.err.println("inspect requires a command");
        return -1;
      }
      inspectCommand = inspectArgs.get(0);
      inspectArgs = inspectArgs.subList(1, inspectArgs.size());
    } else if (!inspectArgs.isEmpty()) {
      System.err.println("Unexpected arguments: " + inspectArgs);
      return -1;
    }
    if (
      !cmdLine.hasOption(DATA_TABLE_OPTION.getOpt())
        || !cmdLine.hasOption(INDEX_TABLE_OPTION.getOpt())
    ) {
      System.err.println("-dt and -it are mandatory");
      printHelp(options);
      return -1;
    }

    String schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPTION.getOpt());
    String dataTable = cmdLine.getOptionValue(DATA_TABLE_OPTION.getOpt());
    String indexTable = cmdLine.getOptionValue(INDEX_TABLE_OPTION.getOpt());
    String tenantId = cmdLine.getOptionValue(TENANT_ID_OPTION.getOpt());
    Long startTime = cmdLine.hasOption(START_TIME_OPTION.getOpt())
      ? Long.valueOf(cmdLine.getOptionValue(START_TIME_OPTION.getOpt()))
      : null;
    Long endTime = cmdLine.hasOption(END_TIME_OPTION.getOpt())
      ? Long.valueOf(cmdLine.getOptionValue(END_TIME_OPTION.getOpt()))
      : null;
    OutputFormat outputFormat = cmdLine.hasOption(OUTPUT_FORMAT_OPTION.getOpt())
      ? OutputFormat.fromString(cmdLine.getOptionValue(OUTPUT_FORMAT_OPTION.getOpt()))
      : OutputFormat.TEXT;
    KeyFormat keyFormat = cmdLine.hasOption(KEYS_OPTION.getOpt())
      ? KeyFormat.fromString(cmdLine.getOptionValue(KEYS_OPTION.getOpt()))
      : KeyFormat.DECODED;

    Configuration conf = HBaseConfiguration.create(getConf());
    if (tenantId != null) {
      conf.set(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    }
    try (Connection connection = ConnectionUtil.getInputConnection(conf)) {
      PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
      String qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
      String qIndexTable = SchemaUtil.getQualifiedTableName(schemaName, indexTable);
      if (!IndexTool.isValidIndexTable(connection, qDataTable, indexTable, tenantId)) {
        throw new IllegalArgumentException(indexTable + " is not an index of " + qDataTable);
      }
      PTable pdataTable = pconn.getTableNoCache(qDataTable);
      PTable pindexTable = pconn.getTableNoCache(qIndexTable);
      IndexFsckProvider provider = IndexFsckProviders.forIndex(pindexTable);
      IndexFsckContext context = new IndexFsckContext(connection, conf, pdataTable, pindexTable,
        tenantId, startTime, endTime, keyFormat, cmdLine.hasOption(CONFIRM_OPTION.getOpt()));
      Report report;
      switch (command) {
        case CMD_VERIFY:
          report = provider.verify(context);
          break;
        case CMD_FSCK:
          report = provider.fsck(context);
          break;
        case CMD_REPAIR:
          report = provider.repair(context);
          break;
        default:
          report = provider.inspect(context, inspectCommand, inspectArgs);
      }
      lastReport = report;
      out.print(outputFormat == OutputFormat.JSON ? report.toJson() + "\n" : report.toText());
      out.flush();
      return report.hasErrors() ? 1 : 0;
    } catch (Exception e) {
      LOGGER.error("IndexFsckTool failed", e);
      System.err.println("Error: " + e.getMessage());
      return -1;
    }
  }

  private static void printHelp(Options options) {
    new HelpFormatter().printHelp(
      "indexfsck.py <verify|fsck|repair|inspect <command> [args]> -dt <table> -it <index>",
      options);
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new IndexFsckTool(), args));
  }
}
