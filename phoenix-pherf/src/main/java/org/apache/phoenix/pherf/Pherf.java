/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf;

import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.schema.SchemaReader;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.util.ResourceList;
import org.apache.phoenix.pherf.workload.WorkloadExecutor;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Properties;

public class Pherf {
    private static final Logger logger = LoggerFactory.getLogger(Pherf.class);
    private static final Options options = new Options();

    static {
        options.addOption("m", "monitor", false, "Launch the stats profilers");
        options.addOption("monitorFrequency", true, "Override for frequency in Ms for which monitor should log stats. " +
                "\n See pherf.default.monitorFrequency in pherf.properties");
        options.addOption("d", "debug", false, "Put tool in debug mode");
        options.addOption("z", "zookeeper", true, "HBase Zookeeper address for connection. Default: localhost");
        options.addOption("l", "load", false, "Loads data according to specified configuration values.");
        options.addOption("scenarioFile", true, "Regex or file name for the Test Scenario configuration .xml file to use.");
        options.addOption("drop", true, "Regex drop all tables with schema name as PHERF. " +
                "\nExample drop Event tables: -drop .*(EVENT).* Drop all: -drop .* or -drop all");
        options.addOption("schemaFile", true, "Regex or file name for the Test phoenix table schema .sql to use.");
        options.addOption("rowCountOverride", true, "Row count override to use instead of one specified in scenario.");
        options.addOption("hint", true, "Executes all queries with specified hint. Example SMALL");
        options.addOption("diff", false, "Run pherf in verification mode and diff with exported results");
        options.addOption("export", false, "Exports query results to CSV files in " + PherfConstants.EXPORT_DIR + " directory");
        options.addOption("listFiles", false, "List available resource files");
        options.addOption("writerThreadSize", true, "Override the default number of writer threads. " +
                "See pherf.default.dataloader.threadpool in Pherf.properties.");
        options.addOption("q", "query", false, "Executes multi-threaded query sets");
        options.addOption("h", "help", false, "Get help on using this utility.");
    }

    private final String zookeeper;
    private final String scenarioFile;
    private final String schemaFile;
    private final String queryHint;
    private final Properties properties;
    private final boolean loadData;
    private final String dropPherfTablesRegEx;
    private final boolean executeQuerySets;
    private final boolean exportCSV;
    private final boolean diff;
    private final boolean monitor;
    private final int rowCountOverride;
    private  final boolean listFiles;

    public Pherf(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        CommandLine command = null;
        HelpFormatter hf = new HelpFormatter();

        try {
            command = parser.parse(options, args);
        } catch (ParseException e) {
            hf.printHelp("Pherf", options);
            System.exit(1);
        }

        properties = getProperties();
        dropPherfTablesRegEx = command.getOptionValue("drop", null);
        monitor = command.hasOption("m");
        String monitorFrequency = (command.hasOption("m") && command.hasOption("monitorFrequency"))
                ? command.getOptionValue("monitorFrequency")
                : properties.getProperty("pherf.default.monitorFrequency");
        properties.setProperty("pherf.default.monitorFrequency", monitorFrequency);

        logger.debug("Using Monitor: " + monitor);
        logger.debug("Monitor Frequency Ms:" + monitorFrequency);
        loadData = command.hasOption("l");
        executeQuerySets = command.hasOption("q");
        zookeeper = command.getOptionValue("z", "localhost");
        queryHint = command.getOptionValue("hint", null);
        exportCSV = command.hasOption("export");
        diff = command.hasOption("diff");
        listFiles = command.hasOption("listFiles");
        scenarioFile = command.hasOption("scenarioFile") ? command.getOptionValue("scenarioFile") : null;
        schemaFile = command.hasOption("schemaFile") ? command.getOptionValue("schemaFile") : null;
        rowCountOverride = Integer.parseInt(command.getOptionValue("rowCountOverride", "0"));
        String writerThreadPoolSize = command.getOptionValue("writerThreadSize",
                properties.getProperty("pherf.default.dataloader.threadpool"));
        properties.setProperty("pherf. default.dataloader.threadpool", writerThreadPoolSize);


        if ((command.hasOption("h") || (args == null || args.length == 0))
                && !command.hasOption("listFiles")) {
            hf.printHelp("Pherf", options);
            System.exit(1);
        }
        PhoenixUtil.setZookeeper(zookeeper);
        PhoenixUtil.setRowCountOverride(rowCountOverride);
        PhoenixUtil.writeSfdcClientProperty();
    }

    public static void main(String[] args) {
        try {
            new Pherf(args).run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void run() throws Exception {
        WorkloadExecutor workloadExec = null;
        try {
            if (listFiles) {
                ResourceList list = new ResourceList(PherfConstants.RESOURCE_DATAMODEL);
                Collection<Path> schemaFiles = list.getResourceList(PherfConstants.SCHEMA_ROOT_PATTERN + ".sql");
                System.out.println("Schema Files:");
                for (Path path : schemaFiles) {
                    System.out.println(path);
                }
                list = new ResourceList(PherfConstants.RESOURCE_SCENARIO);
                Collection<Path> scenarioFiles =
                        list.getResourceList(PherfConstants.SCENARIO_ROOT_PATTERN + ".xml");
                System.out.println("Scenario Files:");
                for (Path path : scenarioFiles) {
                    System.out.println(path);
                }
                return;
            }
            workloadExec = (scenarioFile == null)
                    ? new WorkloadExecutor(properties,
                    new XMLConfigParser(PherfConstants.DEFAULT_FILE_PATTERN),
                    monitor)
                    : new WorkloadExecutor(properties,
                    new XMLConfigParser(scenarioFile),
                    monitor);

            // Drop tables with PHERF schema and regex comparison
            if (null != dropPherfTablesRegEx) {
                logger.info("\nDropping existing table with PHERF namename and "
                        + dropPherfTablesRegEx + " regex expression.");
                new PhoenixUtil().deleteTables(dropPherfTablesRegEx);
            }

            // Schema and Data Load
            if (loadData) {
                logger.info("\nStarting to apply schema...");
                SchemaReader reader = (schemaFile == null)
                        ? new SchemaReader(".*.sql")
                        : new SchemaReader(schemaFile);
                reader.applySchema();

                logger.info("\nStarting Data Load...");
                workloadExec.executeDataLoad();

                logger.info("\nGenerate query gold files after data load");
                workloadExec.executeMultithreadedQueryExecutor(queryHint, true, PherfConstants.RunMode.FUNCTIONAL);
            } else {
                logger.info("\nSKIPPED: Data Load and schema creation as -l argument not specified");
            }

            // Execute multi-threaded query sets
            if (executeQuerySets) {
                logger.info("\nStarting to apply schema...");
                workloadExec.executeMultithreadedQueryExecutor(queryHint, exportCSV, diff ? PherfConstants.RunMode.FUNCTIONAL : PherfConstants.RunMode.PERFORMANCE);
            } else {
                logger.info("\nSKIPPED: Multithreaded query set execution as -q argument not specified");
            }
        } finally {
            if (workloadExec != null) {
                logger.info("Run completed. Shutting down Monitor if it was running.");
                workloadExec.shutdown();
            }
        }
    }

    private static Properties getProperties() throws Exception {
        ResourceList list = new ResourceList();
        return list.getProperties();
    }
}