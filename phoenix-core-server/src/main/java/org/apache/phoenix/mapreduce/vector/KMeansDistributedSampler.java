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
package org.apache.phoenix.mapreduce.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed reservoir sampling MapReduce job for k-means training. Mappers perform local
 * reservoir sampling over base table splits, emitting candidate vectors. A single reducer performs
 * a second-level reservoir sample and persists the final sample to HDFS as a SequenceFile.
 */
public class KMeansDistributedSampler {

  private static final Logger LOGGER = LoggerFactory.getLogger(KMeansDistributedSampler.class);

  /**
   * Mapper that performs local Algorithm R reservoir sampling across its input split.
   */
  public static class KMeansSamplingMapper
    extends Mapper<NullWritable, PhoenixRecordWritable, NullWritable, VectorWritable> {

    private String vectorColumn;
    private int sampleSize;
    private int dimension;
    private Random random;
    private List<float[]> reservoir;
    private int count;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      vectorColumn = PhoenixConfigurationUtil.getKMeansVectorColumn(conf);
      sampleSize = PhoenixConfigurationUtil.getKMeansSampleSize(conf);
      dimension = PhoenixConfigurationUtil.getKMeansDimension(conf);
      Long seed = PhoenixConfigurationUtil.getKMeansRandomSeed(conf);
      int taskId = context.getTaskAttemptID().getTaskID().getId();
      random = (seed != null) ? new Random(seed ^ taskId) : new Random();
      reservoir = new ArrayList<>(Math.min(sampleSize, 10000));
      count = 0;
    }

    @Override
    protected void map(NullWritable key, PhoenixRecordWritable record, Context context)
      throws IOException, InterruptedException {
      if (record == null) {
        return;
      }
      Map<String, Object> map = record.getResultMap();
      if (map == null || map.isEmpty()) {
        return;
      }

      Object val = map.get(vectorColumn);
      if (val == null) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          if (vectorColumn != null && vectorColumn.equalsIgnoreCase(entry.getKey())) {
            val = entry.getValue();
            break;
          }
        }
      }
      if (val == null && map.size() == 1) {
        val = map.values().iterator().next();
      }
      if (val == null) {
        return;
      }

      float[] v;
      if (val instanceof float[]) {
        v = (float[]) val;
      } else if (val instanceof byte[]) {
        byte[] b = (byte[]) val;
        v = PVectorFloat.readElements(b, 0, b.length);
      } else {
        Object obj = PVectorFloat.INSTANCE.toObject(val, PVectorFloat.INSTANCE);
        if (obj instanceof float[]) {
          v = (float[]) obj;
        } else {
          return;
        }
      }

      if (dimension > 0 && v.length != dimension) {
        LOGGER.warn("Skipping vector with mismatched dimension: expected {}, got {}", dimension,
          v.length);
        return;
      }

      if (count < sampleSize) {
        reservoir.add(v);
      } else {
        int j = random.nextInt(count + 1);
        if (j < sampleSize) {
          reservoir.set(j, v);
        }
      }
      count++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      NullWritable key = NullWritable.get();
      for (float[] v : reservoir) {
        context.write(key, new VectorWritable(v));
      }
    }
  }

  /**
   * Reducer that merges candidate samples from all mappers via a second-level reservoir sample.
   */
  public static class KMeansSamplingReducer
    extends Reducer<NullWritable, VectorWritable, NullWritable, VectorWritable> {

    private int sampleSize;
    private Random random;
    private List<float[]> reservoir;
    private int count;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      sampleSize = PhoenixConfigurationUtil.getKMeansSampleSize(conf);
      Long seed = PhoenixConfigurationUtil.getKMeansRandomSeed(conf);
      random = (seed != null) ? new Random(seed) : new Random();
      reservoir = new ArrayList<>(Math.min(sampleSize, 10000));
      count = 0;
    }

    @Override
    protected void reduce(NullWritable key, Iterable<VectorWritable> values, Context context)
      throws IOException, InterruptedException {
      for (VectorWritable vw : values) {
        float[] v = vw.getVector();
        if (v == null) {
          continue;
        }
        if (count < sampleSize) {
          reservoir.add(Arrays.copyOf(v, v.length));
        } else {
          int j = random.nextInt(count + 1);
          if (j < sampleSize) {
            reservoir.set(j, Arrays.copyOf(v, v.length));
          }
        }
        count++;
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      NullWritable key = NullWritable.get();
      for (float[] v : reservoir) {
        context.write(key, new VectorWritable(v));
      }
    }
  }

  /**
   * Configures and creates the reservoir sampling MapReduce job.
   * @param conf       Hadoop configuration
   * @param outputPath HDFS output directory for the sampled SequenceFile
   * @return configured Job instance
   * @throws IOException on error
   */
  public static Job createSamplingJob(Configuration conf, Path outputPath) throws IOException {
    Job job = Job.getInstance(conf, "Phoenix KMeans Reservoir Sampling");
    job.setJarByClass(KMeansTool.class);

    String tableName = PhoenixConfigurationUtil.getKMeansTableName(conf);
    String vectorColumn = PhoenixConfigurationUtil.getKMeansVectorColumn(conf);

    PhoenixMapReduceUtil.setInput(job, PhoenixRecordWritable.class, tableName, (String) null,
      vectorColumn);
    PhoenixMapReduceUtil.addPhoenixDependencyJars(job.getConfiguration());

    job.setMapperClass(KMeansSamplingMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);

    job.setReducerClass(KMeansSamplingReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    job.setNumReduceTasks(1);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, outputPath);

    return job;
  }

  /**
   * Reads sampled vectors back from a SequenceFile path or directory on HDFS.
   * @param conf       Hadoop configuration
   * @param samplePath path to SequenceFile or directory containing part files
   * @return list of float vectors
   * @throws IOException on read error
   */
  public static List<float[]> readSampledVectors(Configuration conf, Path samplePath)
    throws IOException {
    List<float[]> vectors = new ArrayList<>();
    FileSystem fs = samplePath.getFileSystem(conf);
    List<Path> files = new ArrayList<>();
    if (fs.isDirectory(samplePath)) {
      FileStatus[] statuses = fs.listStatus(samplePath);
      if (statuses != null) {
        for (FileStatus status : statuses) {
          String name = status.getPath().getName();
          if (name.startsWith("part-") && !name.endsWith(".crc")) {
            files.add(status.getPath());
          }
        }
      }
    } else {
      files.add(samplePath);
    }

    for (Path file : files) {
      try (SequenceFile.Reader reader =
        new SequenceFile.Reader(conf, SequenceFile.Reader.file(file))) {
        NullWritable key = NullWritable.get();
        VectorWritable val = new VectorWritable();
        while (reader.next(key, val)) {
          vectors.add(Arrays.copyOf(val.getVector(), val.getVector().length));
        }
      }
    }
    return vectors;
  }

  /**
   * Writes vectors to a SequenceFile on HDFS.
   * @param conf           Hadoop configuration
   * @param sampleFilePath path to the output SequenceFile
   * @param vectors        vectors to write
   * @throws IOException on write error
   */
  public static void writeSampledVectors(Configuration conf, Path sampleFilePath,
    List<float[]> vectors) throws IOException {
    try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.file(sampleFilePath), SequenceFile.Writer.keyClass(NullWritable.class),
      SequenceFile.Writer.valueClass(VectorWritable.class))) {
      NullWritable key = NullWritable.get();
      for (float[] v : vectors) {
        writer.append(key, new VectorWritable(v));
      }
    }
  }
}
