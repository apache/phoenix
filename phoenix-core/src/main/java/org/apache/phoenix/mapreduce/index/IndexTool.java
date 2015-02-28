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
package org.apache.phoenix.mapreduce.index;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ColumnInfoToStringEncoderDecoder;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * An MR job to populate the index table from the data table.
 *
 */
public class IndexTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(IndexTool.class);

    private static final Option SCHEMA_NAME_OPTION = new Option("s", "schema", true, "Phoenix schema name (optional)");
    private static final Option DATA_TABLE_OPTION = new Option("dt", "data-table", true, "Data table name (mandatory)");
    private static final Option INDEX_TABLE_OPTION = new Option("it", "index-table", true, "Index table name(mandatory)");
    private static final Option OUTPUT_PATH_OPTION = new Option("op", "output-path", true, "Output path where the files are written(mandatory)");
    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");
    
    private static final String ALTER_INDEX_QUERY_TEMPLATE = "ALTER INDEX IF EXISTS %s ON %s %s";  
    private static final String INDEX_JOB_NAME_TEMPLATE = "PHOENIX_%s_INDX_%s";
    
    
    private Options getOptions() {
        final Options options = new Options();
        options.addOption(SCHEMA_NAME_OPTION);
        options.addOption(DATA_TABLE_OPTION);
        options.addOption(INDEX_TABLE_OPTION);
        options.addOption(OUTPUT_PATH_OPTION);
        options.addOption(HELP_OPTION);
        return options;
    }
    
    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing.
     *
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    private CommandLine parseOptions(String[] args) {

        final Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: "+ e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!cmdLine.hasOption(DATA_TABLE_OPTION.getOpt())) {
            throw new IllegalStateException(DATA_TABLE_OPTION.getLongOpt() + " is a mandatory " 
                   + "parameter");
        }
        
        if (!cmdLine.hasOption(INDEX_TABLE_OPTION.getOpt())) {
            throw new IllegalStateException(INDEX_TABLE_OPTION.getLongOpt() + " is a mandatory " 
                   + "parameter");
        }

        if (!cmdLine.hasOption(OUTPUT_PATH_OPTION.getOpt())) {
            throw new IllegalStateException(OUTPUT_PATH_OPTION.getLongOpt() + " is a mandatory " 
                   + "parameter");
        }
        return cmdLine;
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
        Connection connection = null;
        try {
            CommandLine cmdLine = null;
            try {
                cmdLine = parseOptions(args);
            } catch (IllegalStateException e) {
                printHelpAndExit(e.getMessage(), getOptions());
            }
            final Configuration configuration = HBaseConfiguration.addHbaseResources(getConf());
            final String schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPTION.getOpt());
            final String dataTable = cmdLine.getOptionValue(DATA_TABLE_OPTION.getOpt());
            final String indexTable = cmdLine.getOptionValue(INDEX_TABLE_OPTION.getOpt());
            final String qDataTable = SchemaUtil.getTableName(schemaName, dataTable);
            final String qIndexTable = SchemaUtil.getTableName(schemaName, indexTable);
         
            connection = ConnectionUtil.getInputConnection(configuration);
            if(!isValidIndexTable(connection, dataTable, indexTable)) {
                throw new IllegalArgumentException(String.format(" %s is not an index table for %s ",qIndexTable,qDataTable));
            }
            
            final PTable pdataTable = PhoenixRuntime.getTable(connection, dataTable);
            final PTable pindexTable = PhoenixRuntime.getTable(connection, indexTable);
            
            // this is set to ensure index tables remains consistent post population.
            long indxTimestamp = pindexTable.getTimeStamp();
            configuration.set(PhoenixConfigurationUtil.CURRENT_SCN_VALUE,Long.toString(indxTimestamp + 1));
            
            // check if the index type is LOCAL, if so, set the logicalIndexName that is computed from the dataTable name.
            String logicalIndexTable = qIndexTable;
            if(IndexType.LOCAL.equals(pindexTable.getIndexType())) {
                logicalIndexTable  = MetaDataUtil.getLocalIndexTableName(dataTable);
            }
            
            final PhoenixConnection pConnection = connection.unwrap(PhoenixConnection.class);
            final PostIndexDDLCompiler ddlCompiler = new PostIndexDDLCompiler(pConnection,new TableRef(pdataTable));
            ddlCompiler.compile(pindexTable);
            
            final List<String> indexColumns = ddlCompiler.getIndexColumnNames();
            final String selectQuery = ddlCompiler.getSelectQuery();
            final String upsertQuery = QueryUtil.constructUpsertStatement(indexTable, indexColumns, Hint.NO_INDEX);
       
            configuration.set(PhoenixConfigurationUtil.UPSERT_STATEMENT, upsertQuery);
            PhoenixConfigurationUtil.setOutputTableName(configuration, logicalIndexTable);
            PhoenixConfigurationUtil.setUpsertColumnNames(configuration,indexColumns.toArray(new String[indexColumns.size()]));
            final List<ColumnInfo> columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, qIndexTable, indexColumns);
            final String encodedColumnInfos = ColumnInfoToStringEncoderDecoder.encode(columnMetadataList);
            configuration.set(PhoenixConfigurationUtil.UPSERT_COLUMN_INFO_KEY, encodedColumnInfos);
            
            final Path outputPath =  new Path(cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt()),logicalIndexTable);
            
            final String jobName = String.format(INDEX_JOB_NAME_TEMPLATE,dataTable,indexTable);
            final Job job = Job.getInstance(configuration, jobName);
            job.setJarByClass(IndexTool.class);
           
            job.setMapperClass(PhoenixIndexImportMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class); 
            job.setMapOutputValueClass(KeyValue.class);
            PhoenixMapReduceUtil.setInput(job,PhoenixIndexDBWritable.class,dataTable,selectQuery);
     
            TableMapReduceUtil.initCredentials(job);
            FileOutputFormat.setOutputPath(job, outputPath);
            
            final HTable htable = new HTable(configuration, logicalIndexTable);
            HFileOutputFormat.configureIncrementalLoad(job, htable);

            boolean status = job.waitForCompletion(true);
            if (!status) {
                LOG.error("Failed to run the IndexTool job. ");
                htable.close();
                return -1;
            }
            
            LOG.info("Loading HFiles from {}", outputPath);
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
            loader.doBulkLoad(outputPath, htable);
            htable.close();
            
            LOG.info("Removing output directory {}", outputPath);
            if (!FileSystem.get(configuration).delete(outputPath, true)) {
                LOG.error("Removing output directory {} failed", outputPath);
            }
            
            // finally update the index state to ACTIVE.
            updateIndexState(connection,dataTable,indexTable,PIndexState.ACTIVE);
            return 0;
            
        } catch (Exception ex) {
           LOG.error(" An exception occured while performing the indexing job , error message {} ",ex.getMessage());
           return -1;
        } finally {
            try {
                if(connection != null) {
                    connection.close();
                }
            } catch(SQLException sqle) {
                LOG.error(" Failed to close connection ",sqle.getMessage());
                throw new RuntimeException("Failed to close connection");
            }
        }
    }

    /**
     * Checks for the validity of the index table passed to the job.
     * @param connection
     * @param masterTable
     * @param indexTable
     * @return
     * @throws SQLException
     */
    private boolean isValidIndexTable(final Connection connection, final String masterTable, final String indexTable) throws SQLException {
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        final String schemaName = SchemaUtil.getSchemaNameFromFullName(masterTable);
        final String tableName = SchemaUtil.getTableNameFromFullName(masterTable);
        
        ResultSet rs = null;
        try {
            rs = dbMetaData.getIndexInfo(null, schemaName, tableName, false, false);
            while(rs.next()) {
                final String indexName = rs.getString(6);
                if(indexTable.equalsIgnoreCase(indexName)) {
                    return true;
                }
            }
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
        return false;
    }
    
    /**
     * Updates the index state.
     * @param connection
     * @param masterTable
     * @param indexTable
     * @param state
     * @throws SQLException
     */
    private void updateIndexState(Connection connection, final String masterTable , final String indexTable, PIndexState state) throws SQLException {
        Preconditions.checkNotNull(connection);
        final String alterQuery = String.format(ALTER_INDEX_QUERY_TEMPLATE,indexTable,masterTable,state.name());
        connection.createStatement().execute(alterQuery);
        LOG.info(" Updated the status of the index {} to {} " , indexTable , state.name());
    }
    
    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new IndexTool(), args);
        System.exit(result);
    }

}
