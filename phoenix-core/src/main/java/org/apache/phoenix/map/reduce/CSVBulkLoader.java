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
package org.apache.phoenix.map.reduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.io.Closeables;
import org.apache.phoenix.map.reduce.util.ConfigReader;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;

public class CSVBulkLoader {
	private static final String UNDERSCORE = "_";
	
	static FileWriter wr = null;
	static BufferedWriter bw = null;
	static boolean isDebug = false; //Set to true, if you need to log the bulk-import time.
	static ConfigReader systemConfig = null;

	static String schemaName = "";
	static String tableName = "";
	static String idxTable = "";
	static String createPSQL[] = null;
	static String skipErrors = null;
	static String zookeeperIP = null;
	static String mapredIP = null;
	static String hdfsNameNode = null;

	static{
		/** load the log-file writer, if debug is true **/
		if(isDebug){
			try {
			    wr = new FileWriter("phoenix-bulk-import.log", false);
			    bw = new BufferedWriter(wr);
			} catch (IOException e) {
			    System.err.println("Error preparing writer for log file :: " + e.getMessage());
			}
		}

		/** load the Map-Reduce configs **/
		try {
			systemConfig = new ConfigReader("csv-bulk-load-config.properties");
		} catch (Exception e) {
			System.err.println("Exception occurred while reading config properties");
			System.err.println("The bulk loader will run slower than estimated");
		}
	}
	
	/**
	 * -i		CSV data file path in hdfs
	 * -s		Phoenix schema name
	 * -t		Phoenix table name
	 * -sql  	Phoenix create table sql path (1 SQL statement per line)
	 * -zk		Zookeeper IP:<port>
	 * -mr		MapReduce Job Tracker IP:<port>
	 * -hd		HDFS NameNode IP:<port>
	 * -o		Output directory path in hdfs (Optional)
	 * -idx  	Phoenix index table name (Optional)
	 * -error    	Ignore error while reading rows from CSV ? (1 - YES/0 - NO, defaults to 1) (OPtional)
	 * -help	Print all options (Optional)
	 */

	@SuppressWarnings("deprecation")
    	public static void main(String[] args) throws Exception{
		
		String inputFile = null;
		String outFile = null;

		Options options = new Options();
		options.addOption("i", true, "CSV data file path");
		options.addOption("o", true, "Output directory path");
		options.addOption("s", true, "Phoenix schema name");
		options.addOption("t", true, "Phoenix table name");
		options.addOption("idx", true, "Phoenix index table name");
		options.addOption("zk", true, "Zookeeper IP:<port>");
		options.addOption("mr", true, "MapReduce Job Tracker IP:<port>");
		options.addOption("hd", true, "HDFS NameNode IP:<port>");
		options.addOption("sql", true, "Phoenix create table sql path");
		options.addOption("error", true, "Ignore error while reading rows from CSV ? (1 - YES/0 - NO, defaults to 1)");
		options.addOption("help", false, "All options");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse( options, args);
		
		if(cmd.hasOption("help")){
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "help", options );
			System.exit(0);
		}
		
		String parser_error = "ERROR while parsing arguments. ";
		//CSV input, table name, sql and zookeeper IP  are mandatory fields
		if(cmd.hasOption("i")){
			inputFile = cmd.getOptionValue("i");
		}else{
			System.err.println(parser_error + "Please provide CSV file input path");
			System.exit(0);
		}
		if(cmd.hasOption("t")){
			tableName = cmd.getOptionValue("t");
		}else{
			System.err.println(parser_error + "Please provide Phoenix table name");
			System.exit(0);
		}
		if(cmd.hasOption("sql")){
			String sqlPath = cmd.getOptionValue("sql");
			createPSQL = getCreatePSQLstmts(sqlPath);
		}
		if(cmd.hasOption("zk")){
			zookeeperIP = cmd.getOptionValue("zk");
		}else{
			System.err.println(parser_error + "Please provide Zookeeper address");
			System.exit(0);
		}
		if(cmd.hasOption("mr")){
			mapredIP = cmd.getOptionValue("mr");
		}else{
			System.err.println(parser_error + "Please provide MapReduce address");
			System.exit(0);
		}
		if(cmd.hasOption("hd")){
			hdfsNameNode = cmd.getOptionValue("hd");
		}else{
			System.err.println(parser_error + "Please provide HDFS NameNode address");
			System.exit(0);
		}
		
		if(cmd.hasOption("o")){
			outFile = cmd.getOptionValue("o");
		}else{
			outFile = "phoenix-output-dir";
		}
		if(cmd.hasOption("s")){
			schemaName = cmd.getOptionValue("s");
		}
		if(cmd.hasOption("idx")){
			idxTable = cmd.getOptionValue("idx");
		}
		if(cmd.hasOption("error")){
			skipErrors = cmd.getOptionValue("error");
		}else{
			skipErrors = "1";
		}
		
		log("[TS - START] :: " + new Date() + "\n");

		Path inputPath = new Path(inputFile);
		Path outPath = new Path(outFile);
		
		//Create the Phoenix table in HBase
		if (createPSQL != null) {
    		for(String s : createPSQL){
    			if(s == null || s.trim().length() == 0) {
    				continue;
    			}
				createTable(s);
    		}
    		
    		log("[TS - Table created] :: " + new Date() + "\n");
		}

        String dataTable = ""; 
        if(schemaName != null && schemaName.trim().length() > 0)
            dataTable = SchemaUtil.normalizeIdentifier(schemaName) + "." + SchemaUtil.normalizeIdentifier(tableName);
        else
            dataTable = SchemaUtil.normalizeIdentifier(tableName);
        
        try {
            validateTable();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.exit(0);
        }

        Configuration conf = new Configuration();
		loadMapRedConfigs(conf);
		
		Job job = new Job(conf, "MapReduce - Phoenix bulk import");
		job.setJarByClass(MapReduceJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.setMapperClass(MapReduceJob.PhoenixMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		
		SchemaMetrics.configureGlobally(conf);

		HTable hDataTable = new HTable(conf, dataTable);
		
		// Auto configure partitioner and reducer according to the Main Data table
    	HFileOutputFormat.configureIncrementalLoad(job, hDataTable);

		job.waitForCompletion(true);
	    
		log("[TS - M-R HFile generated..Now dumping to HBase] :: " + new Date() + "\n");
		
    		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    		loader.doBulkLoad(new Path(outFile), hDataTable);
	    
		log("[TS - FINISH] :: " + new Date() + "\n");
		if(isDebug) bw.close();
		
	}
	
	private static void createTable(String stmt) {
		
		Connection conn = null;
		PreparedStatement statement = null;

		try {
			conn = DriverManager.getConnection(getUrl(), "", "");
			try {
    			statement = conn.prepareStatement(stmt);
    			statement.execute();
    			conn.commit();
			} finally {
			    if(statement != null) {
			        statement.close();
			    }
			}
		} catch (Exception e) {
			System.err.println("Error creating the table :: " + e.getMessage());
		} finally{
			try {
			    if(conn != null) {
			        conn.close();
			    }
			} catch (Exception e) {
				System.err.println("Failed to close connection :: " + e.getMessage());
			}
		}
	}

	/**
	 * Perform any required validation on the table being bulk loaded into:
	 * - ensure no column family names start with '_', as they'd be ignored leading to problems.
	 * @throws SQLException
	 */
    private static void validateTable() throws SQLException {
        
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            ResultSet rs = conn.getMetaData().getColumns(null, StringUtil.escapeLike(schemaName), StringUtil.escapeLike(tableName), null);
            while (rs.next()) {
                String familyName = rs.getString(1);
                if (familyName != null && familyName.startsWith(UNDERSCORE)) {
                    String msg;
                    if (QueryConstants.DEFAULT_COLUMN_FAMILY.equals(familyName)) {
                        msg = "CSV Bulk Loader error: All column names that are not part of the primary key constraint must be prefixed with a column family name (i.e. f.my_column VARCHAR)";
                    } else {
                        msg = "CSV Bulk Loader error: Column family name must not start with '_': " + familyName;
                    }
                    throw new SQLException(msg);
                }
            }
        } finally{
             conn.close();
        }
    }
    
	private static String getUrl() {
        	return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zookeeperIP;
    	}
	
	private static void loadMapRedConfigs(Configuration conf){

		conf.set("IGNORE.INVALID.ROW", skipErrors);
		conf.set("schemaName", schemaName);
		conf.set("tableName", tableName);
		conf.set("zk", zookeeperIP);
		conf.set("hbase.zookeeper.quorum", zookeeperIP);
		conf.set("fs.default.name", hdfsNameNode);
		conf.set("mapred.job.tracker", mapredIP);
		
		//Load the other System-Configs
		try {
			
			Map<String, String> configs = systemConfig.getAllConfigMap();
			
			if(configs.containsKey("mapreduce.map.output.compress")){
				String s = configs.get("mapreduce.map.output.compress");
				if(s != null && s.trim().length() > 0)
					conf.set("mapreduce.map.output.compress", s);
			}
			
			if(configs.containsKey("mapreduce.map.output.compress.codec")){
				String s = configs.get("mapreduce.map.output.compress.codec");
				if(s != null && s.trim().length() > 0)
					conf.set("mapreduce.map.output.compress.codec", s);
			}
			
			if(configs.containsKey("io.sort.record.percent")){
				String s = configs.get("io.sort.record.percent");
				if(s != null && s.trim().length() > 0)
					conf.set("io.sort.record.percent", s);	
			}
				
			if(configs.containsKey("io.sort.factor")){
				String s = configs.get("io.sort.factor");
				if(s != null && s.trim().length() > 0)
					conf.set("io.sort.factor", s);
			}
			
			if(configs.containsKey("mapred.tasktracker.map.tasks.maximum")){
				String s = configs.get("mapred.tasktracker.map.tasks.maximum");
				if(s != null && s.trim().length() > 0)
					conf.set("mapred.tasktracker.map.tasks.maximum", s);
			}
				
		} catch (Exception e) {
			System.err.println("Error loading the configs :: " + e.getMessage());
			System.err.println("The bulk loader will run slower than estimated");
		}
	}
	
	private static String[] getCreatePSQLstmts(String path){
		
	    BufferedReader br = null;
		try {
			FileReader file = new FileReader(path);
			br = new BufferedReader(file);
			//Currently, we can have at-most 2 SQL statements - 1 for create table and 1 for index
			String[] sb = new String[2];
			String line;
			for(int i = 0; i < 2 && (line = br.readLine()) != null ; i++){
				sb[i] = line;
			}
			return sb;
			
		} catch (IOException e) {
			System.err.println("Error reading the file :: " + path + ", " + e.getMessage());
		} finally {
		    if (br != null) Closeables.closeQuietly(br);
		}
		return null;
	}
	
	private static void log(String msg){
		if(isDebug){
			try {
				bw.write(msg);
			} catch (IOException e) {
				System.err.println("Error logging the statement :: " + msg);
			}
		}
	}
}
