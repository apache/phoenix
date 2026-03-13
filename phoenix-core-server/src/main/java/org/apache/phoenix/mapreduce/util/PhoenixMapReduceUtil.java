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
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.SchemaType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.EnvironmentEdgeManager;

/**
 * Utility class for setting Configuration parameters for the Map Reduce job
 */
public final class PhoenixMapReduceUtil {

  public static final String INVALID_TIME_RANGE_EXCEPTION_MESSAGE = "Invalid time range for table";

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

  /**
   * Validates that start and end times are in the past and start < end.
   * @param startTime Start timestamp in millis (nullable, defaults to 0)
   * @param endTime   End timestamp in millis (nullable, defaults to current time)
   * @param tableName Table name for error messages
   * @throws IllegalArgumentException if time range is invalid
   */
  public static void validateTimeRange(Long startTime, Long endTime, String tableName) {
    long currentTime = EnvironmentEdgeManager.currentTimeMillis();
    long st = (startTime == null) ? 0L : startTime;
    long et = (endTime == null) ? currentTime : endTime;

    if (et > currentTime || st >= et) {
      throw new IllegalArgumentException(String.format(
        "%s %s: start and end times must be in the past "
          + "and start < end. Start: %d, End: %d, Current: %d",
        INVALID_TIME_RANGE_EXCEPTION_MESSAGE, tableName, st, et, currentTime));
    }
  }

  /**
   * Validates that the end time doesn't exceed the max lookback age configured in Phoenix.
   * @param configuration Hadoop configuration
   * @param endTime       End timestamp in millis
   * @param tableName     Table name for error messages
   * @throws IllegalArgumentException if endTime is before min allowed timestamp
   */
  public static void validateMaxLookbackAge(Configuration configuration, Long endTime,
    String tableName) {
    long maxLookBackAge = BaseScannerRegionObserverConstants.getMaxLookbackInMillis(configuration);
    if (maxLookBackAge > 0) {
      long minTimestamp = EnvironmentEdgeManager.currentTimeMillis() - maxLookBackAge;
      if (endTime < minTimestamp) {
        throw new IllegalArgumentException(String.format(
          "Table %s can't look back past the configured max lookback age: %d ms. "
            + "End time: %d, Min allowed timestamp: %d",
          tableName, maxLookBackAge, endTime, minTimestamp));
      }
    }
  }

  /**
   * Validates that a table is suitable for MR operations. Checks table existence, type, and state.
   * @param connection         Phoenix connection
   * @param qualifiedTableName Qualified table name
   * @param allowViews         Whether to allow VIEW tables
   * @param allowIndexes       Whether to allow INDEX tables
   * @return PTable instance
   * @throws SQLException             if connection fails
   * @throws IllegalArgumentException if validation fails
   */
  public static PTable validateTableForMRJob(Connection connection, String qualifiedTableName,
    boolean allowViews, boolean allowIndexes) throws SQLException {
    PTable pTable = connection.unwrap(PhoenixConnection.class).getTableNoCache(qualifiedTableName);

    if (pTable == null) {
      throw new IllegalArgumentException(
        String.format("Table %s does not exist", qualifiedTableName));
    } else if (!allowViews && pTable.getType() == PTableType.VIEW) {
      throw new IllegalArgumentException(
        String.format("Cannot run MR job on VIEW table %s", qualifiedTableName));
    } else if (!allowIndexes && pTable.getType() == PTableType.INDEX) {
      throw new IllegalArgumentException(
        String.format("Cannot run MR job on INDEX table %s directly", qualifiedTableName));
    }

    return pTable;
  }

  /**
   * Configures a Configuration object with ZooKeeper settings from a ZK quorum string.
   * @param baseConf Base configuration to create from (typically job configuration)
   * @param zkQuorum ZooKeeper quorum string in format: "zk_quorum:port:znode" Example:
   *                 "zk1,zk2,zk3:2181:/hbase"
   * @return New Configuration with ZK settings applied
   * @throws RuntimeException if zkQuorum format is invalid (must have exactly 3 parts)
   */
  public static Configuration createConfigurationForZkQuorum(Configuration baseConf,
    String zkQuorum) {
    Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create(baseConf);
    String[] parts = zkQuorum.split(":");

    if (!(parts.length == 3 || parts.length == 4)) {
      throw new RuntimeException(
        "Invalid ZooKeeper quorum format. Expected: zk_quorum:port:znode OR "
          + "zk_quorum:port:znode:krb_principal. Got: " + zkQuorum);
    }

    conf.set(HConstants.ZOOKEEPER_QUORUM, parts[0]);
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, parts[1]);
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parts[2]);
    if (parts.length == 4) {
      conf.set(HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, parts[3]);
    }
    return conf;
  }
}
