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
package org.apache.phoenix.mapreduce.index;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil.IndexStatusUpdater;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public class PhoenixIndexImportDirectMapper
  extends Mapper<NullWritable, PhoenixIndexDBWritable, ImmutableBytesWritable, IntWritable> {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(PhoenixIndexImportDirectMapper.class);

  private final PhoenixIndexDBWritable indxWritable = new PhoenixIndexDBWritable();

  private List<ColumnInfo> indxTblColumnMetadata;

  private Connection connection;

  private PreparedStatement pStatement;

  private DirectHTableWriter writer;

  private int batchSize;
  private long batchSizeBytes;

  private MutationState mutationState;
  private int currentBatchCount = 0;

  private IndexStatusUpdater indexStatusUpdater;

  private boolean isVectorIndex = false;
  private long vectorCentroidGeneration = 1L;
  private String vectorDistanceMetric = "L2";
  private int vectorIndexInSelected = -1;
  private int nonCentroidColCount = 0;
  private VectorCentroidCache centroidCache;
  private String indexTableName;

  @Override
  protected void setup(final Context context) throws IOException, InterruptedException {
    super.setup(context);
    final Configuration configuration = context.getConfiguration();
    writer = new DirectHTableWriter(configuration);

    try {
      indxTblColumnMetadata = PhoenixConfigurationUtil.getUpsertColumnMetadataList(configuration);
      indxWritable.setColumnMetadata(indxTblColumnMetadata);

      final Properties overrideProps = new Properties();
      String scn = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
      String txScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
      if (txScnValue == null && scn != null) {
        overrideProps.put(PhoenixRuntime.BUILD_INDEX_AT_ATTRIB, scn);
      }
      connection = ConnectionUtil.getOutputConnection(configuration, overrideProps);
      connection.setAutoCommit(false);
      // Get BatchSize, which is in terms of rows
      ConnectionQueryServices services = ((PhoenixConnection) connection).getQueryServices();
      int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,
        QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
      batchSize = Math.min(((PhoenixConnection) connection).getMutateBatchSize(), maxSize);

      // Get batch size in terms of bytes
      batchSizeBytes = ((PhoenixConnection) connection).getMutateBatchSizeBytes();

      LOGGER.info("Mutation Batch Size = " + batchSize);

      final String upsertQuery = PhoenixConfigurationUtil.getUpsertStatement(configuration);
      this.pStatement = connection.prepareStatement(upsertQuery);

      indexTableName = PhoenixConfigurationUtil.getIndexToolIndexTableName(configuration);
      PTable pIndexTable = connection.unwrap(PhoenixConnection.class).getTable(indexTableName);

      indexStatusUpdater = new IndexStatusUpdater(SchemaUtil.getEmptyColumnFamily(pIndexTable),
        EncodedColumnsUtil.getEmptyKeyValueInfo(pIndexTable).getFirst());

      isVectorIndex = PhoenixConfigurationUtil.getIsVectorIndex(configuration)
        || (pIndexTable != null && pIndexTable.isVectorIndex());
      if (isVectorIndex) {
        vectorCentroidGeneration =
          PhoenixConfigurationUtil.getVectorCentroidGeneration(configuration);
        if (
          vectorCentroidGeneration <= 0 && pIndexTable != null
            && pIndexTable.getVectorCentroidGeneration() != null
        ) {
          vectorCentroidGeneration = pIndexTable.getVectorCentroidGeneration();
        }
        if (vectorCentroidGeneration <= 0) {
          vectorCentroidGeneration = 1L;
        }
        vectorDistanceMetric = PhoenixConfigurationUtil.getVectorDistanceMetric(configuration);
        if (vectorDistanceMetric == null && pIndexTable != null) {
          vectorDistanceMetric = pIndexTable.getVectorDistanceMetric() != null
            ? pIndexTable.getVectorDistanceMetric()
            : "L2";
        }
        vectorIndexInSelected = PhoenixConfigurationUtil.getVectorIndexInSelected(configuration);
        nonCentroidColCount =
          configuration.getInt(PhoenixConfigurationUtil.VECTOR_NON_CENTROID_COL_COUNT, 0);
        centroidCache = VectorCentroidCache.getInstance(configuration);
        centroidCache.loadCentroids(indexTableName, vectorCentroidGeneration, connection);
      }

    } catch (Exception e) {
      tryClosingResources();
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void map(NullWritable key, PhoenixIndexDBWritable record, Context context)
    throws IOException, InterruptedException {

    try {
      final List<Object> values = record.getValues();
      if (isVectorIndex) {
        if (vectorIndexInSelected < 0 || vectorIndexInSelected >= values.size()) {
          context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
          return;
        }
        Object vectorObj = values.get(vectorIndexInSelected);
        if (vectorObj == null) {
          // Null vectors are excluded from vector indexes
          context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
          return;
        }
        float[] queryFloats = extractFloatVector(vectorObj);
        if (queryFloats == null || queryFloats.length == 0) {
          context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
          return;
        }
        currentBatchCount++;
        int centroidId = centroidCache.findNearestCentroid(indexTableName, vectorCentroidGeneration,
          queryFloats, vectorDistanceMetric);
        this.pStatement.setInt(1, centroidId);
        for (int k = 0; k < nonCentroidColCount; k++) {
          Object obj = values.get(k);
          int psIdx = k + 2;
          ColumnInfo colInfo = indxTblColumnMetadata.get(psIdx - 1);
          if (obj == null) {
            this.pStatement.setNull(psIdx, colInfo.getSqlType());
          } else if (colInfo.getPDataType() == PVectorFloat.INSTANCE) {
            float[] vec = extractFloatVector(obj);
            this.pStatement.setObject(psIdx, vec, colInfo.getSqlType());
          } else if (colInfo.getPDataType() == PVectorDouble.INSTANCE) {
            double[] vec = extractDoubleVector(obj);
            this.pStatement.setObject(psIdx, vec, colInfo.getSqlType());
          } else if (obj instanceof Array) {
            this.pStatement.setArray(psIdx, (Array) obj);
          } else {
            this.pStatement.setObject(psIdx, obj, colInfo.getSqlType());
          }
        }
        this.pStatement.execute();
      } else {
        currentBatchCount++;
        indxWritable.setValues(values);
        indxWritable.write(this.pStatement);
        this.pStatement.execute();
      }

      final PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
      MutationState currentMutationState = pconn.getMutationState();
      if (mutationState == null) {
        mutationState = currentMutationState;
      }
      // Keep accumulating Mutations till batch size
      mutationState.join(currentMutationState);
      // Write Mutation Batch
      if (currentBatchCount % batchSize == 0) {
        writeBatch(mutationState, context);
        mutationState = null;
      }

      // Make sure progress is reported to Application Master.
      context.progress();
    } catch (SQLException e) {
      LOGGER.error(" Error {}  while read/write of a record ", e.getMessage());
      context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(currentBatchCount);
      throw new RuntimeException(e);
    }
    context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
  }

  private float[] extractFloatVector(Object obj) throws SQLException {
    if (obj == null) {
      return null;
    }
    if (obj instanceof float[]) {
      return (float[]) obj;
    }
    if (obj instanceof double[]) {
      double[] d = (double[]) obj;
      float[] f = new float[d.length];
      for (int i = 0; i < d.length; i++) {
        f[i] = (float) d[i];
      }
      return f;
    }
    if (obj instanceof Float[]) {
      Float[] fArr = (Float[]) obj;
      float[] f = new float[fArr.length];
      for (int i = 0; i < fArr.length; i++) {
        f[i] = fArr[i] != null ? fArr[i] : 0f;
      }
      return f;
    }
    if (obj instanceof Double[]) {
      Double[] dArr = (Double[]) obj;
      float[] f = new float[dArr.length];
      for (int i = 0; i < dArr.length; i++) {
        f[i] = dArr[i] != null ? dArr[i].floatValue() : 0f;
      }
      return f;
    }
    if (obj instanceof Array) {
      Object inner = ((Array) obj).getArray();
      return extractFloatVector(inner);
    }
    if (obj instanceof byte[]) {
      byte[] b = (byte[]) obj;
      return PVectorFloat.readElements(b, 0, b.length);
    }
    return null;
  }

  private double[] extractDoubleVector(Object obj) throws SQLException {
    if (obj == null) {
      return null;
    }
    if (obj instanceof double[]) {
      return (double[]) obj;
    }
    if (obj instanceof float[]) {
      float[] f = (float[]) obj;
      double[] d = new double[f.length];
      for (int i = 0; i < f.length; i++) {
        d[i] = f[i];
      }
      return d;
    }
    if (obj instanceof Double[]) {
      Double[] dArr = (Double[]) obj;
      double[] d = new double[dArr.length];
      for (int i = 0; i < dArr.length; i++) {
        d[i] = dArr[i] != null ? dArr[i] : 0.0;
      }
      return d;
    }
    if (obj instanceof Float[]) {
      Float[] fArr = (Float[]) obj;
      double[] d = new double[fArr.length];
      for (int i = 0; i < fArr.length; i++) {
        d[i] = fArr[i] != null ? fArr[i].doubleValue() : 0.0;
      }
      return d;
    }
    if (obj instanceof Array) {
      Object inner = ((Array) obj).getArray();
      return extractDoubleVector(inner);
    }
    if (obj instanceof byte[]) {
      byte[] b = (byte[]) obj;
      return PVectorDouble.readElements(b, 0, b.length);
    }
    return null;
  }

  private void writeBatch(MutationState mutationState, Context context)
    throws IOException, SQLException, InterruptedException {
    final Iterator<Pair<byte[], List<Mutation>>> iterator = mutationState.toMutations(true, null);
    while (iterator.hasNext()) {
      Pair<byte[], List<Mutation>> mutationPair = iterator.next();
      List<Mutation> batchMutations = mutationPair.getSecond();
      List<List<Mutation>> batchOfBatchMutations =
        MutationState.getMutationBatchList(batchSize, batchSizeBytes, batchMutations);
      for (List<Mutation> mutationList : batchOfBatchMutations) {
        for (Mutation mutation : mutationList) {
          indexStatusUpdater.setVerified(mutation.cellScanner());
        }
        writer.write(mutationList);
      }
      context.getCounter(PhoenixJobCounters.OUTPUT_RECORDS)
        .increment(mutationPair.getSecond().size());
    }
    connection.rollback();
    currentBatchCount = 0;
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    try {
      // Write the last & final Mutation Batch
      if (mutationState != null) {
        writeBatch(mutationState, context);
      }
      // We are writing some dummy key-value as map output here so that we commit only one
      // output to reducer.
      context.write(
        new ImmutableBytesWritable(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
        new IntWritable(0));
      super.cleanup(context);
    } catch (SQLException e) {
      LOGGER.error(" Error {}  while read/write of a record ", e.getMessage());
      context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(currentBatchCount);
      throw new RuntimeException(e);
    } finally {
      tryClosingResources();
    }
  }

  private void tryClosingResources() throws IOException {
    if (this.connection != null) {
      try {
        this.connection.close();
      } catch (SQLException e) {
        LOGGER.error("Error while closing connection in the PhoenixIndexMapper class ", e);
      }
    }
    if (this.writer != null) {
      this.writer.close();
    }
  }
}
