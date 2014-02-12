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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

public class MapReduceJob {

	public static class PhoenixMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{
		
		private Connection conn_zk 	= null;
		private PreparedStatement[] stmtCache;
		private String tableName;
		private String schemaName;
		Map<Integer, Integer> colDetails = new LinkedHashMap<Integer, Integer>();
		boolean ignoreUpsertError = true;
		private String zookeeperIP;
		
		/**
		 * Get the phoenix jdbc connection.
		 */
		
		private static String getUrl(String url) {
	        	return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + url;
	  	}
		
		/***
		 * Get the column information from the table metaData.
		 * Cretae a map of col-index and col-data-type.
		 * Create the upsert Prepared Statement based on the map-size.
		 */
		
		@Override
		public void setup(Context context) throws InterruptedException{
			Properties props = new Properties();
			
			try {
				zookeeperIP 		= context.getConfiguration().get("zk");
				
				//ZK connection used to get the table meta-data
				conn_zk				= DriverManager.getConnection(getUrl(zookeeperIP), props);
				
				schemaName			= context.getConfiguration().get("schemaName");
				tableName 			= context.getConfiguration().get("tableName");
				ignoreUpsertError 	= context.getConfiguration().get("IGNORE.INVALID.ROW").equalsIgnoreCase("0") ? false : true;
				
				//Get the resultset from the actual zookeeper connection. Connectionless mode throws "UnSupportedOperation" exception for this
				ResultSet rs 		= conn_zk.getMetaData().getColumns(null, schemaName, tableName, null);
				//This map holds the key-value pair of col-position and its data type
				int i = 1;
				while(rs.next()){
					colDetails.put(i, rs.getInt(QueryUtil.DATA_TYPE_POSITION));
					i++;
				}
				
				stmtCache = new PreparedStatement[colDetails.size()];
				ArrayList<String> cols = new ArrayList<String>();
				for(i = 0 ; i < colDetails.size() ; i++){
					cols.add("?");
					String prepValues = StringUtils.join(cols, ",");
					String upsertStmt = ""; 
					if(schemaName != null && schemaName.trim().length() > 0)
						upsertStmt = "upsert into " + schemaName + "." + tableName + " values (" + prepValues + ")";
					else
						upsertStmt = "upsert into " + tableName + " values (" + prepValues + ")";
					try {
						stmtCache[i] = conn_zk.prepareStatement(upsertStmt);
					} catch (SQLException e) {
						System.err.println("Error preparing the upsert statement" + e.getMessage());
						if(!ignoreUpsertError){
							throw (new InterruptedException(e.getMessage()));
						}
					}
				}
			} catch (SQLException e) {
					System.err.println("Error occurred in connecting to Phoenix HBase" + e.getMessage());
			}
			
	  	}
		
		/* Tokenize the text input line based on the "," delimeter.
		*  TypeCast the token based on the col-data-type using the convertTypeSpecificValue API below.
		*  Upsert the data. DO NOT COMMIT.
		*  Use Phoenix's getUncommittedDataIterator API to parse the uncommited data to KeyValue pairs.
		*  Emit the row-key and KeyValue pairs from Mapper to allow sorting based on row-key.
		*  Finally, do connection.rollback( to preserve table state).
		*/
		
		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
			
			CSVReader reader = new CSVReader(new InputStreamReader(new ByteArrayInputStream(line.toString().getBytes())), ',');			
			try {
				String[] tokens = reader.readNext();
				
				PreparedStatement upsertStatement;
				if(tokens.length >= stmtCache.length){
					//If CVS values are more than the number of cols in the table, apply the col count cap
					upsertStatement = stmtCache[stmtCache.length - 1];
				}else{
					//Else, take the corresponding upsertStmt from cached array 
					upsertStatement = stmtCache[tokens.length - 1];
				}

				for(int i = 0 ; i < tokens.length && i < colDetails.size() ;i++){
					upsertStatement.setObject(i+1, convertTypeSpecificValue(tokens[i], colDetails.get(new Integer(i+1))));
				}
				
				upsertStatement.execute();
			} catch (SQLException e) {
				System.err.println("Failed to upsert data in the Phoenix :: " + e.getMessage());
				if(!ignoreUpsertError){
					throw (new InterruptedException(e.getMessage()));
				}
			} catch (Exception e) {
				System.err.println("Failed to upsert data in the Phoenix :: " + e.getMessage());
			}finally {
				reader.close();
       			} 
			
			Iterator<Pair<byte[],List<KeyValue>>> dataIterator = null;
			try {
				dataIterator = PhoenixRuntime.getUncommittedDataIterator(conn_zk);
			} catch (SQLException e) {
				System.err.println("Failed to retrieve the data iterator for Phoenix table :: " + e.getMessage());
			}
			
			while(dataIterator != null && dataIterator.hasNext()){
				Pair<byte[],List<KeyValue>> row = dataIterator.next();
				for(KeyValue kv : row.getSecond()){
					context.write(new ImmutableBytesWritable(kv.getRow()), kv);
				}
			}
			
			try {
			    conn_zk.rollback();
			} catch (SQLException e) {
				System.err.println("Transaction rollback failed.");
			}
		}
		
		/*
		* Do connection.close()
		*/
		
		@Override
		public void cleanup(Context context) {
	  		try {
	  			conn_zk.close();
			} catch (SQLException e) {
				System.err.println("Failed to close the JDBC connection");
			}
	  	}
		
		private Object convertTypeSpecificValue(String s, Integer sqlType) throws Exception {
			return PDataType.fromTypeId(sqlType).toObject(s);
		}
	}
	
}
