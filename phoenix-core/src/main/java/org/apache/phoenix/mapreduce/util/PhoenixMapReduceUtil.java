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
package org.apache.phoenix.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.SchemaType;

/**
 * Utility class for setting Configuration parameters for the Map Reduce job
 */
public final class PhoenixMapReduceUtil {

    private PhoenixMapReduceUtil() {
        
    }
    
    /**
     * 
     * @param job
     * @param inputClass DBWritable class
     * @param tableName  Input table name
     * @param conditions Condition clause to be added to the WHERE clause.
     * @param fieldNames fields being projected for the SELECT query.
     */
    public static void setInput(final Job job, final Class<? extends DBWritable> inputClass, final String tableName , final String conditions, final String... fieldNames) {
          job.setInputFormatClass(PhoenixInputFormat.class);
          final Configuration configuration = job.getConfiguration();
          PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
          PhoenixConfigurationUtil.setSelectColumnNames(configuration,fieldNames);
          PhoenixConfigurationUtil.setInputClass(configuration,inputClass);
          PhoenixConfigurationUtil.setSchemaType(configuration, SchemaType.TABLE);
    }
       
    /**
     * 
     * @param job         
     * @param inputClass  DBWritable class  
     * @param tableName   Input table name
     * @param inputQuery  Select query.
     */
    public static void setInput(final Job job, final Class<? extends DBWritable> inputClass, final String tableName, final String inputQuery) {
          job.setInputFormatClass(PhoenixInputFormat.class);
          final Configuration configuration = job.getConfiguration();
          PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
          PhoenixConfigurationUtil.setInputQuery(configuration, inputQuery);
          PhoenixConfigurationUtil.setInputClass(configuration,inputClass);
          PhoenixConfigurationUtil.setSchemaType(configuration, SchemaType.QUERY);
          
     }
    
    /**
     * A method to override which HBase cluster for {@link PhoenixInputFormat} to read from
     * @param job MapReduce Job
     * @param quorum an HBase cluster's ZooKeeper quorum
     */
    public static void setInputCluster(final Job job, final String quorum) {
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setInputCluster(configuration, quorum);
    }
    /**
     * 
     * @param job
     * @param outputClass  
     * @param tableName  Output table 
     * @param columns    List of columns separated by ,
     */
    public static void setOutput(final Job job, final String tableName,final String columns) {
        job.setOutputFormatClass(PhoenixOutputFormat.class);
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
        PhoenixConfigurationUtil.setUpsertColumnNames(configuration,columns);
    }
    
    
    /**
     * 
     * @param job
     * @param outputClass
     * @param tableName  Output table 
     * @param fieldNames fields
     */
    public static void setOutput(final Job job, final String tableName , final String... fieldNames) {
          job.setOutputFormatClass(PhoenixOutputFormat.class);
          final Configuration configuration = job.getConfiguration();
          PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
          PhoenixConfigurationUtil.setUpsertColumnNames(configuration,fieldNames);
    }
    
    /**
     * A method to override which HBase cluster for {@link PhoenixOutputFormat} to write to
     * @param job MapReduce Job
     * @param quorum an HBase cluster's ZooKeeper quorum
     */
    public static void setOutputCluster(final Job job, final String quorum) {
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setOutputCluster(configuration, quorum);
    }

    
}
