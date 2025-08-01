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
package org.apache.phoenix.mapreduce.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
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
   * @param inputClass DBWritable class
   * @param tableName  Input table name
   * @param conditions Condition clause to be added to the WHERE clause. Can be <tt>null</tt> if
   *                   there are no conditions.
   * @param fieldNames fields being projected for the SELECT query.
   */
  public static void setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final String tableName, final String conditions, final String... fieldNames) {
    final Configuration configuration = setInput(job, inputClass, tableName);
    if (conditions != null) {
      PhoenixConfigurationUtil.setInputTableConditions(configuration, conditions);
    }
    PhoenixConfigurationUtil.setSelectColumnNames(configuration, fieldNames);
  }

  /**
   * @param job              MR job instance
   * @param inputClass       DBWritable class
   * @param inputFormatClass InputFormat class
   * @param tableName        Input table name
   * @param conditions       Condition clause to be added to the WHERE clause. Can be <tt>null</tt>
   *                         if there are no conditions.
   * @param fieldNames       fields being projected for the SELECT query.
   */
  public static void setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final Class<? extends InputFormat> inputFormatClass, final String tableName,
    final String conditions, final String... fieldNames) {
    final Configuration configuration = setInput(job, inputClass, inputFormatClass, tableName);
    if (conditions != null) {
      PhoenixConfigurationUtil.setInputTableConditions(configuration, conditions);
    }
    PhoenixConfigurationUtil.setSelectColumnNames(configuration, fieldNames);
  }

  /**
   * @param inputClass DBWritable class
   * @param tableName  Input table name
   * @param inputQuery Select query.
   */
  public static void setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final String tableName, final String inputQuery) {
    final Configuration configuration = setInput(job, inputClass, tableName);
    PhoenixConfigurationUtil.setInputQuery(configuration, inputQuery);
    PhoenixConfigurationUtil.setSchemaType(configuration, SchemaType.QUERY);
  }

  /**
   * @param inputClass       DBWritable class
   * @param inputFormatClass InputFormat class
   * @param tableName        Input table name
   * @param inputQuery       Select query
   */

  public static void setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final Class<? extends InputFormat> inputFormatClass, final String tableName,
    final String inputQuery) {
    final Configuration configuration = setInput(job, inputClass, inputFormatClass, tableName);
    PhoenixConfigurationUtil.setInputQuery(configuration, inputQuery);
    PhoenixConfigurationUtil.setSchemaType(configuration, SchemaType.QUERY);
  }

  /**
   * @param inputClass   DBWritable class
   * @param snapshotName The name of a snapshot (of a table) to read from
   * @param tableName    Input table name
   * @param restoreDir   a temporary dir to copy the snapshot files into
   * @param conditions   Condition clause to be added to the WHERE clause. Can be <tt>null</tt> if
   *                     there are no conditions.
   * @param fieldNames   fields being projected for the SELECT query.
   */
  public static void setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final String snapshotName, String tableName, Path restoreDir, final String conditions,
    final String... fieldNames) throws IOException {
    final Configuration configuration =
      setSnapshotInput(job, inputClass, snapshotName, tableName, restoreDir, SchemaType.QUERY);
    if (conditions != null) {
      PhoenixConfigurationUtil.setInputTableConditions(configuration, conditions);
    }
    PhoenixConfigurationUtil.setSelectColumnNames(configuration, fieldNames);
  }

  /**
   * @param inputClass   DBWritable class
   * @param snapshotName The name of a snapshot (of a table) to read from
   * @param tableName    Input table name
   * @param restoreDir   a temporary dir to copy the snapshot files into
   * @param inputQuery   The select query
   */
  public static void setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final String snapshotName, String tableName, Path restoreDir, String inputQuery)
    throws IOException {
    final Configuration configuration =
      setSnapshotInput(job, inputClass, snapshotName, tableName, restoreDir, SchemaType.QUERY);
    if (inputQuery != null) {
      PhoenixConfigurationUtil.setInputQuery(configuration, inputQuery);
    }

  }

  public static void setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final String snapshotName, String tableName, Path restoreDir) {
    setSnapshotInput(job, inputClass, snapshotName, tableName, restoreDir, SchemaType.QUERY);
  }

  /**
   * @param inputClass   DBWritable class
   * @param snapshotName The name of a snapshot (of a table) to read from
   * @param tableName    Input table name
   * @param restoreDir   a temporary dir to copy the snapshot files into
   */
  private static Configuration setSnapshotInput(Job job, Class<? extends DBWritable> inputClass,
    String snapshotName, String tableName, Path restoreDir, SchemaType schemaType) {
    job.setInputFormatClass(PhoenixInputFormat.class);
    final Configuration configuration = job.getConfiguration();
    PhoenixConfigurationUtil.setInputClass(configuration, inputClass);
    PhoenixConfigurationUtil.setSnapshotNameKey(configuration, snapshotName);
    PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
    PhoenixConfigurationUtil.setRestoreDirKey(configuration, restoreDir.toString());
    PhoenixConfigurationUtil.setSchemaType(configuration, schemaType);
    return configuration;
  }

  private static Configuration setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final String tableName) {
    job.setInputFormatClass(PhoenixInputFormat.class);
    final Configuration configuration = job.getConfiguration();
    PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
    PhoenixConfigurationUtil.setInputClass(configuration, inputClass);
    return configuration;
  }

  private static Configuration setInput(final Job job, final Class<? extends DBWritable> inputClass,
    final Class<? extends InputFormat> inputFormatClass, final String tableName) {
    job.setInputFormatClass(inputFormatClass);
    final Configuration configuration = job.getConfiguration();
    PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
    PhoenixConfigurationUtil.setInputClass(configuration, inputClass);
    return configuration;
  }

  /**
   * A method to override which HBase cluster for {@link PhoenixInputFormat} to read from
   * @param job    MapReduce Job
   * @param quorum an HBase cluster's ZooKeeper quorum
   */
  public static void setInputCluster(final Job job, final String quorum) {
    final Configuration configuration = job.getConfiguration();
    PhoenixConfigurationUtil.setInputCluster(configuration, quorum);
  }

  /**
   * @param tableName Output table
   * @param columns   List of columns separated by ,
   */
  public static void setOutput(final Job job, final String tableName, final String columns) {
    job.setOutputFormatClass(PhoenixOutputFormat.class);
    final Configuration configuration = job.getConfiguration();
    PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
    PhoenixConfigurationUtil.setUpsertColumnNames(configuration, columns.split(","));
  }

  /**
   * @param tableName  Output table
   * @param fieldNames fields
   */
  public static void setOutput(final Job job, final String tableName, final String... fieldNames) {
    job.setOutputFormatClass(PhoenixOutputFormat.class);
    final Configuration configuration = job.getConfiguration();
    PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
    PhoenixConfigurationUtil.setUpsertColumnNames(configuration, fieldNames);
  }

  /**
   * A method to override which HBase cluster for {@link PhoenixOutputFormat} to write to
   * @param job    MapReduce Job
   * @param quorum an HBase cluster's ZooKeeper quorum
   */
  public static void setOutputCluster(final Job job, final String quorum) {
    final Configuration configuration = job.getConfiguration();
    PhoenixConfigurationUtil.setOutputCluster(configuration, quorum);
  }

  public static void setTenantId(final Job job, final String tenantId) {
    PhoenixConfigurationUtil.setTenantId(job.getConfiguration(), tenantId);
  }

}
