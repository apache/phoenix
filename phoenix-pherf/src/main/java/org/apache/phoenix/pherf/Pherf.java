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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.phoenix.pherf.PherfConstants.CompareType;
import org.apache.phoenix.pherf.PherfConstants.GeneratePhoenixStats;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.jmx.MonitorManager;
import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.schema.SchemaReader;
import org.apache.phoenix.pherf.util.GoogleChartGenerator;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.util.ResourceList;
import org.apache.phoenix.pherf.workload.QueryExecutor;
import org.apache.phoenix.pherf.workload.Workload;
import org.apache.phoenix.pherf.workload.WorkloadExecutor;
import org.apache.phoenix.pherf.workload.WriteWorkload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pherf {
    private static final Logger logger = LoggerFactory.getLogger(Pherf.class);
    private static final Options options = new Options();
    private final PhoenixUtil phoenixUtil = PhoenixUtil.create();

    static {
        options.addOption("disableSchemaApply", false, "Set to disable schema from being applied.");
		options.addOption("disableRuntimeResult", false,
				"Set to disable writing detailed CSV file during query execution. Those will eventually get written at the end of query execution.");
        options.addOption("z", "zookeeper", true,
                "HBase Zookeeper address for connection. Default: localhost");
        options.addOption("q", "query", false, "Executes multi-threaded query sets");
        options.addOption("listFiles", false, "List available resource files");
        options.addOption("l", "load", false,
                "Pre-loads data according to specified configuration values.");
        options.addOption("scenarioFile", true,
                "Regex or file name for the Test Scenario configuration .xml file to use.");
        options.addOption("drop", true, "Regex drop all tables with schema name as PHERF. "
                + "\nExample drop Event tables: -drop .*(EVENT).* Drop all: -drop .* or -drop all");
        options.addOption("schemaFile", true,
                "Regex or file name for the Test phoenix table schema .sql to use.");
        options.addOption("m", "monitor", false, "Launch the stats profilers");
        options.addOption("monitorFrequency", true,
                "Override for frequency in Ms for which monitor should log stats. "
                        + "\n See pherf.default.monitorFrequency in pherf.properties");
        options.addOption("rowCountOverride", true,
                "Row count override to use instead of one specified in scenario.");
        options.addOption("hint", true, "Executes all queries with specified hint. Example SMALL");
        options.addOption("diff", false,
                "Run pherf in verification mode and diff with exported results");
        options.addOption("export", false,
                "Exports query results to CSV files in " + PherfConstants.EXPORT_DIR
                        + " directory");
        options.addOption("writerThreadSize", true,
                "Override the default number of writer threads. "
                        + "See pherf.default.dataloader.threadpool in Pherf.properties.");
        options.addOption("h", "help", false, "Get help on using this utility.");
        options.addOption("d", "debug", false, "Put tool in debug mode");
        options.addOption("stats", false,
                "Update Phoenix Statistics after data is loaded with -l argument");
		options.addOption("label", true, "Label a run. Result file name will be suffixed with specified label");
		options.addOption("compare", true, "Specify labeled run(s) to compare");
		options.addOption("useAverageCompareType", false, "Compare results with Average query time instead of default is Minimum query time.");
		    options.addOption("t", "thin", false, "Use the Phoenix Thin Driver");
		    options.addOption("s", "server", true, "The URL for the Phoenix QueryServer");
		    options.addOption("b", "batchApi", false, "Use JDBC Batch API for writes");
    }

    private final String zookeeper;
    private final String scenarioFile;
    private final String schemaFile;
    private final String queryHint;
    private final Properties properties;
    private final boolean preLoadData;
    private final String dropPherfTablesRegEx;
    private final boolean executeQuerySets;
    private final boolean isFunctional;
    private final boolean monitor;
    private final int rowCountOverride;
    private final boolean listFiles;
    private final boolean applySchema;
    private final boolean writeRuntimeResults;
    private final GeneratePhoenixStats generateStatistics;
    private final String label;
    private final String compareResults;
    private final CompareType compareType;
    private final boolean thinDriver;
    private final String queryServerUrl;

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

        properties = PherfConstants.create().getProperties(PherfConstants.PHERF_PROPERTIES, false);
        dropPherfTablesRegEx = command.getOptionValue("drop", null);
        monitor = command.hasOption("m");
        String
                monitorFrequency =
                (command.hasOption("m") && command.hasOption("monitorFrequency")) ?
                        command.getOptionValue("monitorFrequency") :
                        properties.getProperty("pherf.default.monitorFrequency");
        properties.setProperty("pherf.default.monitorFrequency", monitorFrequency);

        logger.debug("Using Monitor: " + monitor);
        logger.debug("Monitor Frequency Ms:" + monitorFrequency);
        preLoadData = command.hasOption("l");
        executeQuerySets = command.hasOption("q");
        zookeeper = command.getOptionValue("z", "localhost");
        queryHint = command.getOptionValue("hint", null);
        isFunctional = command.hasOption("diff");
        listFiles = command.hasOption("listFiles");
        applySchema = !command.hasOption("disableSchemaApply");
        writeRuntimeResults = !command.hasOption("disableRuntimeResult");
        scenarioFile =
                command.hasOption("scenarioFile") ? command.getOptionValue("scenarioFile") : null;
        schemaFile = command.hasOption("schemaFile") ? command.getOptionValue("schemaFile") : null;
        rowCountOverride = Integer.parseInt(command.getOptionValue("rowCountOverride", "0"));
        generateStatistics = command.hasOption("stats") ? GeneratePhoenixStats.YES : GeneratePhoenixStats.NO;
        String
                writerThreadPoolSize =
                command.getOptionValue("writerThreadSize",
                        properties.getProperty("pherf.default.dataloader.threadpool"));
        properties.setProperty("pherf. default.dataloader.threadpool", writerThreadPoolSize);
        label = command.getOptionValue("label", null);
        compareResults = command.getOptionValue("compare", null);
        compareType = command.hasOption("useAverageCompareType") ? CompareType.AVERAGE : CompareType.MINIMUM;
        thinDriver = command.hasOption("thin");
        if (thinDriver) {
            queryServerUrl = command.getOptionValue("server", "http://localhost:8765");
        } else {
            queryServerUrl = null;
        }

        if (command.hasOption('b')) {
          // If the '-b' option was provided, set the system property for WriteWorkload to pick up.
          System.setProperty(WriteWorkload.USE_BATCH_API_PROPERTY, Boolean.TRUE.toString());
        }

        if ((command.hasOption("h") || (args == null || args.length == 0)) && !command
                .hasOption("listFiles")) {
            hf.printHelp("Pherf", options);
            System.exit(1);
        }
        PhoenixUtil.setRowCountOverride(rowCountOverride);
        if (!thinDriver) {
            logger.info("Using thick driver with ZooKeepers '{}'", zookeeper);
            PhoenixUtil.setZookeeper(zookeeper);
        } else {
            logger.info("Using thin driver with PQS '{}'", queryServerUrl);
            // Enables the thin-driver and sets the PQS URL
            PhoenixUtil.useThinDriver(queryServerUrl);
        }
        ResultUtil.setFileSuffix(label);
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
        MonitorManager monitorManager = null;
        List<Workload> workloads = new ArrayList<>();
        WorkloadExecutor workloadExecutor = new WorkloadExecutor(properties, workloads, !isFunctional);
        try {
            if (listFiles) {
                ResourceList list = new ResourceList(PherfConstants.RESOURCE_DATAMODEL);
                Collection<Path>
                        schemaFiles =
                        list.getResourceList(PherfConstants.SCHEMA_ROOT_PATTERN + ".sql");
                System.out.println("Schema Files:");
                for (Path path : schemaFiles) {
                    System.out.println(path);
                }
                list = new ResourceList(PherfConstants.RESOURCE_SCENARIO);
                Collection<Path>
                        scenarioFiles =
                        list.getResourceList(PherfConstants.SCENARIO_ROOT_PATTERN + ".xml");
                System.out.println("Scenario Files:");
                for (Path path : scenarioFiles) {
                    System.out.println(path);
                }
                return;
            }
            
            // Compare results and exit  
			if (null != compareResults) {
				logger.info("\nStarting to compare results and exiting for " + compareResults);
				new GoogleChartGenerator(compareResults, compareType).readAndRender();
				return;
            }
            
            XMLConfigParser parser = new XMLConfigParser(scenarioFile);

            // Drop tables with PHERF schema and regex comparison
            if (null != dropPherfTablesRegEx) {
                logger.info(
                        "\nDropping existing table with PHERF namename and " + dropPherfTablesRegEx
                                + " regex expression.");
                phoenixUtil.deleteTables(dropPherfTablesRegEx);
            }

            if (monitor) {
                monitorManager =
                        new MonitorManager(Integer.parseInt(
                                properties.getProperty("pherf.default.monitorFrequency")));
                workloadExecutor.add(monitorManager);
            }

            if (applySchema) {
                logger.info("\nStarting to apply schema...");
                SchemaReader
                        reader =
                        (schemaFile == null) ?
                                new SchemaReader(".*.sql") :
                                new SchemaReader(schemaFile);
                reader.applySchema();
            }

            // Schema and Data Load
            if (preLoadData) {
                logger.info("\nStarting Data Load...");
                Workload workload = new WriteWorkload(parser, generateStatistics);
                try {
                    workloadExecutor.add(workload);

                    // Wait for dataLoad to complete
                    workloadExecutor.get(workload);
                } finally {
                    if (null != workload) {
                        workload.complete();
                    }
                }
            } else {
                logger.info(
                        "\nSKIPPED: Data Load and schema creation as -l argument not specified");
            }

            // Execute multi-threaded query sets
            if (executeQuerySets) {
                logger.info("\nStarting to apply Execute Queries...");

                workloadExecutor
                        .add(new QueryExecutor(parser, phoenixUtil, workloadExecutor, parser.getDataModels(), queryHint,
                                isFunctional, writeRuntimeResults));

            } else {
                logger.info(
                        "\nSKIPPED: Multithreaded query set execution as -q argument not specified");
            }

            // Clean up the monitor explicitly
            if (monitorManager != null) {
                logger.info("Run completed. Shutting down Monitor.");
                monitorManager.complete();
            }

            // Collect any final jobs
            workloadExecutor.get();

        } finally {
            if (workloadExecutor != null) {
                logger.info("Run completed. Shutting down thread pool.");
                workloadExecutor.shutdown();
            }
        }
    }
}