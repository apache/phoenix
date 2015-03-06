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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.job.JobManager;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.CSVCommonsLoader;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * Base tool for running MapReduce-based ingests of data.
 */
@SuppressWarnings("deprecation")
public class CsvBulkLoadTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(CsvBulkLoadTool.class);

    static final Option ZK_QUORUM_OPT = new Option("z", "zookeeper", true, "Zookeeper quorum to connect to (optional)");
    static final Option INPUT_PATH_OPT = new Option("i", "input", true, "Input CSV path (mandatory)");
    static final Option OUTPUT_PATH_OPT = new Option("o", "output", true, "Output path for temporary HFiles (optional)");
    static final Option SCHEMA_NAME_OPT = new Option("s", "schema", true, "Phoenix schema name (optional)");
    static final Option TABLE_NAME_OPT = new Option("t", "table", true, "Phoenix table name (mandatory)");
    static final Option INDEX_TABLE_NAME_OPT = new Option("it", "index-table", true, "Phoenix index table name when just loading this particualar index table");
    static final Option DELIMITER_OPT = new Option("d", "delimiter", true, "Input delimiter, defaults to comma");
    static final Option QUOTE_OPT = new Option("q", "quote", true, "Supply a custom phrase delimiter, defaults to double quote character");
    static final Option ESCAPE_OPT = new Option("e", "escape", true, "Supply a custom escape character, default is a backslash");
    static final Option ARRAY_DELIMITER_OPT = new Option("a", "array-delimiter", true, "Array element delimiter (optional)");
    static final Option IMPORT_COLUMNS_OPT = new Option("c", "import-columns", true, "Comma-separated list of columns to be imported");
    static final Option IGNORE_ERRORS_OPT = new Option("g", "ignore-errors", false, "Ignore input errors");
    static final Option HELP_OPT = new Option("h", "help", false, "Show this help and quit");

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CsvBulkLoadTool(), args);
    }

    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing.
     *
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    CommandLine parseOptions(String[] args) {

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

        if (!cmdLine.hasOption(TABLE_NAME_OPT.getOpt())) {
            throw new IllegalStateException(TABLE_NAME_OPT.getLongOpt() + " is a mandatory " +
                    "parameter");
        }

        if (!cmdLine.getArgList().isEmpty()) {
            throw new IllegalStateException("Got unexpected extra parameters: "
                    + cmdLine.getArgList());
        }

        if (!cmdLine.hasOption(INPUT_PATH_OPT.getOpt())) {
            throw new IllegalStateException(INPUT_PATH_OPT.getLongOpt() + " is a mandatory " +
                    "parameter");
        }

        return cmdLine;
    }

    private Options getOptions() {
        Options options = new Options();
        options.addOption(INPUT_PATH_OPT);
        options.addOption(TABLE_NAME_OPT);
        options.addOption(INDEX_TABLE_NAME_OPT);
        options.addOption(ZK_QUORUM_OPT);
        options.addOption(OUTPUT_PATH_OPT);
        options.addOption(SCHEMA_NAME_OPT);
        options.addOption(DELIMITER_OPT);
        options.addOption(QUOTE_OPT);
        options.addOption(ESCAPE_OPT);
        options.addOption(ARRAY_DELIMITER_OPT);
        options.addOption(IMPORT_COLUMNS_OPT);
        options.addOption(IGNORE_ERRORS_OPT);
        options.addOption(HELP_OPT);
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

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.addHbaseResources(getConf());

        CommandLine cmdLine = null;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
        Class.forName(DriverManager.class.getName());
        Connection conn = DriverManager.getConnection(
                getJdbcUrl(cmdLine.getOptionValue(ZK_QUORUM_OPT.getOpt())));
        
        return loadData(conf, cmdLine, conn);
    }

	private int loadData(Configuration conf, CommandLine cmdLine,
			Connection conn) throws SQLException, InterruptedException,
			ExecutionException {
		    String tableName = cmdLine.getOptionValue(TABLE_NAME_OPT.getOpt());
        String schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPT.getOpt());
        String indexTableName = cmdLine.getOptionValue(INDEX_TABLE_NAME_OPT.getOpt());
        String qualifiedTableName = getQualifiedTableName(schemaName, tableName);
        String qualifedIndexTableName = null;
        if(indexTableName != null){
        	qualifedIndexTableName = getQualifiedTableName(schemaName, indexTableName);
        }
        List<ColumnInfo> importColumns = buildImportColumns(conn, cmdLine, qualifiedTableName);
        configureOptions(cmdLine, importColumns, conf);

        try {
            validateTable(conn, schemaName, tableName);
        } finally {
            conn.close();
        }

        Path inputPath = new Path(cmdLine.getOptionValue(INPUT_PATH_OPT.getOpt()));
        Path outputPath = null;
        if (cmdLine.hasOption(OUTPUT_PATH_OPT.getOpt())) {
            outputPath = new Path(cmdLine.getOptionValue(OUTPUT_PATH_OPT.getOpt()));
        } else {
            outputPath = new Path("/tmp/" + UUID.randomUUID());
        }
        
        List<TargetTableRef> tablesToBeLoaded = new ArrayList<TargetTableRef>();
        tablesToBeLoaded.add(new TargetTableRef(qualifiedTableName));
        tablesToBeLoaded.addAll(getIndexTables(conn, schemaName, qualifiedTableName));
        
        // When loading a single index table, check index table name is correct
        if(qualifedIndexTableName != null){
            TargetTableRef targetIndexRef = null;
        	for (TargetTableRef tmpTable : tablesToBeLoaded){
        		if(tmpTable.getLogicalName().compareToIgnoreCase(qualifedIndexTableName) == 0) {
                    targetIndexRef = tmpTable;
        			break;
        		}
        	}
        	if(targetIndexRef == null){
                throw new IllegalStateException("CSV Bulk Loader error: index table " +
                    qualifedIndexTableName + " doesn't exist");
        	}
        	tablesToBeLoaded.clear();
        	tablesToBeLoaded.add(targetIndexRef);
        }
        
        List<Future<Boolean>> runningJobs = new ArrayList<Future<Boolean>>();
        boolean useInstrumentedPool = conn
                .unwrap(PhoenixConnection.class)
                .getQueryServices()
                .getProps()
                .getBoolean(QueryServices.METRICS_ENABLED,
                        QueryServicesOptions.DEFAULT_IS_METRICS_ENABLED);
        ExecutorService executor =  JobManager.createThreadPoolExec(Integer.MAX_VALUE, 5, 20, useInstrumentedPool);
        try{
	        for (TargetTableRef table : tablesToBeLoaded) {
	        	Path tablePath = new Path(outputPath, table.getPhysicalName());
	        	Configuration jobConf = new Configuration(conf);
	        	jobConf.set(CsvToKeyValueMapper.TABLE_NAME_CONFKEY, qualifiedTableName);
	        	if(qualifiedTableName.compareToIgnoreCase(table.getLogicalName()) != 0) {
                    jobConf.set(CsvToKeyValueMapper.INDEX_TABLE_NAME_CONFKEY, table.getPhysicalName());
	        	}
	        	TableLoader tableLoader = new TableLoader(
                        jobConf, table.getPhysicalName(), inputPath, tablePath);
	        	runningJobs.add(executor.submit(tableLoader));
	        }
        } finally {
        	executor.shutdown();
        }
        
        // wait for all jobs to complete
        int retCode = 0;
        for(Future<Boolean> task : runningJobs){
        	if(!task.get() && (retCode==0)){
        		retCode = -1;
        	}
        }
		return retCode;
	}

    String getJdbcUrl(String zkQuorum) {
        if (zkQuorum == null) {
            LOG.warn("Defaulting to localhost for ZooKeeper quorum");
            zkQuorum = "localhost:2181";
        }
        return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
    }

    /**
     * Build up the list of columns to be imported. The list is taken from the command line if
     * present, otherwise it is taken from the table description.
     *
     * @param conn connection to Phoenix
     * @param cmdLine supplied command line options
     * @param qualifiedTableName table name (possibly with schema) of the table to be imported
     * @return the list of columns to be imported
     */
    List<ColumnInfo> buildImportColumns(Connection conn, CommandLine cmdLine,
            String qualifiedTableName) throws SQLException {
        List<String> userSuppliedColumnNames = null;
        if (cmdLine.hasOption(IMPORT_COLUMNS_OPT.getOpt())) {
            userSuppliedColumnNames = Lists.newArrayList(
                    Splitter.on(",").trimResults().split
                            (cmdLine.getOptionValue(IMPORT_COLUMNS_OPT.getOpt())));
        }
        return CSVCommonsLoader.generateColumnInfo(
                conn, qualifiedTableName, userSuppliedColumnNames, true);
    }

    /**
     * Calculate the HBase HTable name for which the import is to be done.
     *
     * @param schemaName import schema name, can be null
     * @param tableName import table name
     * @return the byte representation of the import HTable
     */
    @VisibleForTesting
    static String getQualifiedTableName(String schemaName, String tableName) {
        if (schemaName != null) {
            return String.format("%s.%s", SchemaUtil.normalizeIdentifier(schemaName),
                    SchemaUtil.normalizeIdentifier(tableName));
        } else {
            return SchemaUtil.normalizeIdentifier(tableName);
        }
    }

    /**
     * Set configuration values based on parsed command line options.
     *
     * @param cmdLine supplied command line options
     * @param importColumns descriptors of columns to be imported
     * @param conf job configuration
     */
    @VisibleForTesting
    static void configureOptions(CommandLine cmdLine, List<ColumnInfo> importColumns,
            Configuration conf) {

        char delimiterChar = ',';
        if (cmdLine.hasOption(DELIMITER_OPT.getOpt())) {
            String delimString = cmdLine.getOptionValue(DELIMITER_OPT.getOpt());
            if (delimString.length() != 1) {
                throw new IllegalArgumentException("Illegal delimiter character: " + delimString);
            }
            delimiterChar = delimString.charAt(0);
        }

        char quoteChar = '"';
        if (cmdLine.hasOption(QUOTE_OPT.getOpt())) {
            String quoteString = cmdLine.getOptionValue(QUOTE_OPT.getOpt());
            if (quoteString.length() != 1) {
                throw new IllegalArgumentException("Illegal quote character: " + quoteString);
            }
            quoteChar = quoteString.charAt(0);
        }

        char escapeChar = '\\';
        if (cmdLine.hasOption(ESCAPE_OPT.getOpt())) {
            String escapeString = cmdLine.getOptionValue(ESCAPE_OPT.getOpt());
            if (escapeString.length() != 1) {
                throw new IllegalArgumentException("Illegal escape character: " + escapeString);
            }
            escapeChar = escapeString.charAt(0);
        }

        if (cmdLine.hasOption(ZK_QUORUM_OPT.getOpt())) {
            String zkQuorum = cmdLine.getOptionValue(ZK_QUORUM_OPT.getOpt());
            LOG.info("Configuring ZK quorum to {}", zkQuorum);
            conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        }

        CsvBulkImportUtil.initCsvImportJob(
                conf,
                getQualifiedTableName(
                        cmdLine.getOptionValue(SCHEMA_NAME_OPT.getOpt()),
                        cmdLine.getOptionValue(TABLE_NAME_OPT.getOpt())),
                delimiterChar,
                quoteChar,
                escapeChar,
                cmdLine.getOptionValue(ARRAY_DELIMITER_OPT.getOpt()),
                importColumns,
                cmdLine.hasOption(IGNORE_ERRORS_OPT.getOpt()));
    }

    /**
     * Perform any required validation on the table being bulk loaded into:
     * - ensure no column family names start with '_', as they'd be ignored leading to problems.
     * @throws java.sql.SQLException
     */
    private void validateTable(Connection conn, String schemaName,
            String tableName) throws SQLException {

        ResultSet rs = conn.getMetaData().getColumns(
                null, StringUtil.escapeLike(schemaName),
                StringUtil.escapeLike(tableName), null);
        while (rs.next()) {
            String familyName = rs.getString(PhoenixDatabaseMetaData.COLUMN_FAMILY);
            if (familyName != null && familyName.startsWith("_")) {
                if (QueryConstants.DEFAULT_COLUMN_FAMILY.equals(familyName)) {
                    throw new IllegalStateException(
                            "CSV Bulk Loader error: All column names that are not part of the " +
                                    "primary key constraint must be prefixed with a column family " +
                                    "name (i.e. f.my_column VARCHAR)");
                } else {
                    throw new IllegalStateException("CSV Bulk Loader error: Column family name " +
                            "must not start with '_': " + familyName);
                }
            }
        }
        rs.close();
    }
    
    /**
     * Get the index tables of current data table
     * @throws java.sql.SQLException
     */
    private List<TargetTableRef> getIndexTables(Connection conn, String schemaName, String qualifiedTableName)
        throws SQLException {
        PTable table = PhoenixRuntime.getTable(conn, qualifiedTableName);
        List<TargetTableRef> indexTables = new ArrayList<TargetTableRef>();
        for(PTable indexTable : table.getIndexes()){
            if (indexTable.getIndexType() == IndexType.LOCAL) {
                indexTables.add(
                        new TargetTableRef(getQualifiedTableName(schemaName,
                                indexTable.getTableName().getString()),
                                MetaDataUtil.getLocalIndexTableName(qualifiedTableName)));
            } else {
                indexTables.add(new TargetTableRef(getQualifiedTableName(schemaName,
                        indexTable.getTableName().getString())));
            }
        }
        return indexTables;
    }

    /**
     * Represents the logical and physical name of a single table to which data is to be loaded.
     *
     * This class exists to allow for the difference between HBase physical table names and
     * Phoenix logical table names.
     */
    private static class TargetTableRef {

        private final String logicalName;
        private final String physicalName;

        private TargetTableRef(String name) {
            this(name, name);
        }

        private TargetTableRef(String logicalName, String physicalName) {
            this.logicalName = logicalName;
            this.physicalName = physicalName;
        }

        public String getLogicalName() {
            return logicalName;
        }

        public String getPhysicalName() {
            return physicalName;
        }
    }

    /**
     * A runnable to load data into a single table
     *
     */
    private static class TableLoader implements Callable<Boolean> {
    	 
    	private Configuration conf;
        private String tableName;
        private Path inputPath;
        private Path outputPath;
         
        public TableLoader(Configuration conf, String qualifiedTableName, Path inputPath, 
        		Path outputPath){
        	this.conf = conf;
            this.tableName = qualifiedTableName;
            this.inputPath = inputPath;
            this.outputPath = outputPath;
        }
        
        @Override
        public Boolean call() {
            LOG.info("Configuring HFile output path to {}", outputPath);
            try{
	            Job job = new Job(conf, "Phoenix MapReduce import for " + tableName);
	
	            // Allow overriding the job jar setting by using a -D system property at startup
	            if (job.getJar() == null) {
	                job.setJarByClass(CsvToKeyValueMapper.class);
	            }
	            job.setInputFormatClass(TextInputFormat.class);
	            FileInputFormat.addInputPath(job, inputPath);
	            FileOutputFormat.setOutputPath(job, outputPath);
	
	            job.setMapperClass(CsvToKeyValueMapper.class);
	            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	            job.setMapOutputValueClass(KeyValue.class);

	            // initialize credentials to possibily run in a secure env
	            TableMapReduceUtil.initCredentials(job);

                HTable htable = new HTable(conf, tableName);

	            // Auto configure partitioner and reducer according to the Main Data table
	            HFileOutputFormat.configureIncrementalLoad(job, htable);
	
	            LOG.info("Running MapReduce import job from {} to {}", inputPath, outputPath);
	            boolean success = job.waitForCompletion(true);
	            if (!success) {
	                LOG.error("Import job failed, check JobTracker for details");
	                htable.close();
	                return false;
	            }
	
	            LOG.info("Loading HFiles from {}", outputPath);
	            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
	            loader.doBulkLoad(outputPath, htable);
	            htable.close();
	
	            LOG.info("Incremental load complete for table=" + tableName);
	
	            LOG.info("Removing output directory {}", outputPath);
	            if (!FileSystem.get(conf).delete(outputPath, true)) {
	                LOG.error("Removing output directory {} failed", outputPath);
	            }
	            
	            return true;
            } catch(Exception ex) {
            	LOG.error("Import job on table=" + tableName + " failed due to exception:" + ex);
            	return false;
            }
        }
     
    }
    
}
