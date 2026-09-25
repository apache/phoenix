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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.phoenix.expression.function.VectorDistanceUtil;
import org.apache.phoenix.index.vector.ClusterSkewMetrics;
import org.apache.phoenix.index.vector.KMeansConfig;
import org.apache.phoenix.index.vector.KMeansResult;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/** Driver for iterative Lloyd's k-means clustering. */
public class KMeansIterationDriver {

  private static final Logger LOGGER = LoggerFactory.getLogger(KMeansIterationDriver.class);

  public static final String CONF_CURRENT_CENTROID_PATH =
    "phoenix.vector.kmeans.current.centroid.path";
  public static final String CONF_CURRENT_ITER_OUTPUT =
    "phoenix.vector.kmeans.current.iteration.output";

  private static volatile Runnable iterationFailureHook = null;

  @VisibleForTesting
  public static void setIterationFailureHook(Runnable hook) {
    iterationFailureHook = hook;
  }

  /** Computes distance between two vectors using the configured metric. */
  public static double computeDistance(float[] a, float[] b, int dim, String metric) {
    if ("COSINE".equalsIgnoreCase(metric)) {
      return VectorDistanceUtil.scalarCosineDistance(a, b, dim);
    }
    return VectorDistanceUtil.scalarL2DistanceSquared(a, b, dim);
  }

  /** Reads centroid vectors from SequenceFile(s). */
  public static List<float[]> readCentroids(Configuration conf, Path centroidPath)
    throws IOException {
    FileSystem fs = centroidPath.getFileSystem(conf);
    List<Path> files = new ArrayList<>();
    if (fs.isDirectory(centroidPath)) {
      FileStatus[] statuses = fs.listStatus(centroidPath);
      if (statuses != null) {
        for (FileStatus status : statuses) {
          String name = status.getPath().getName();
          if ((name.startsWith("part-") || name.endsWith(".seq")) && !name.endsWith(".crc")) {
            files.add(status.getPath());
          }
        }
      }
    } else {
      files.add(centroidPath);
    }

    Map<Integer, float[]> map = new TreeMap<>();
    for (Path file : files) {
      try (SequenceFile.Reader reader =
        new SequenceFile.Reader(conf, SequenceFile.Reader.file(file))) {
        IntWritable key = new IntWritable();
        VectorWritable val = new VectorWritable();
        while (reader.next(key, val)) {
          map.put(key.get(), Arrays.copyOf(val.getVector(), val.getVector().length));
        }
      }
    }
    return new ArrayList<>(map.values());
  }

  /** Writes centroid vectors to a SequenceFile. */
  public static void writeCentroids(Configuration conf, Path centroidFile, List<float[]> centroids)
    throws IOException {
    FileSystem fs = centroidFile.getFileSystem(conf);
    if (!fs.exists(centroidFile.getParent())) {
      fs.mkdirs(centroidFile.getParent());
    }
    try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.file(centroidFile), SequenceFile.Writer.keyClass(IntWritable.class),
      SequenceFile.Writer.valueClass(VectorWritable.class))) {
      IntWritable key = new IntWritable();
      for (int i = 0; i < centroids.size(); i++) {
        key.set(i);
        writer.append(key, new VectorWritable(centroids.get(i)));
      }
    }
  }

  /** Locates the first valid SequenceFile within a directory or returns the path itself. */
  public static Path findCentroidFile(FileSystem fs, Path dir) throws IOException {
    if (!fs.isDirectory(dir)) {
      return dir;
    }
    FileStatus[] statuses = fs.listStatus(dir);
    if (statuses != null) {
      for (FileStatus st : statuses) {
        String name = st.getPath().getName();
        if ((name.startsWith("part-") || name.endsWith(".seq")) && !name.endsWith(".crc")) {
          return st.getPath();
        }
      }
    }
    return dir;
  }

  /** Loads centroids in mapper setup from DistributedCache or fallback configuration path. */
  private static List<float[]> loadCentroidsInMapper(Mapper<?, ?, ?, ?>.Context context)
    throws IOException {
    List<float[]> loaded = null;
    URI[] cacheFiles = context.getCacheFiles();
    if (cacheFiles != null && cacheFiles.length > 0) {
      for (URI u : cacheFiles) {
        try {
          Path p = new Path(u.getPath());
          loaded = readCentroids(context.getConfiguration(), p);
          if (loaded != null && !loaded.isEmpty()) {
            break;
          }
        } catch (Exception e) {
          LOGGER.debug("Could not read centroids from cache URI: {}", u, e);
        }
      }
    }
    if (loaded == null || loaded.isEmpty()) {
      String pathStr = context.getConfiguration().get(CONF_CURRENT_CENTROID_PATH);
      if (pathStr != null) {
        loaded = readCentroids(context.getConfiguration(), new Path(pathStr));
      }
    }
    if (loaded == null || loaded.isEmpty()) {
      throw new IOException("Failed to load centroids in mapper from cache or configuration");
    }
    return loaded;
  }

  /** Mapper: Assigns sampled vectors to nearest centroid and emits partial sums. */
  public static class KMeansAssignmentMapper
    extends Mapper<NullWritable, VectorWritable, IntWritable, CentroidWritable> {

    private List<float[]> centroids;
    private int dimension;
    private String metric;
    private CentroidWritable[] accumulators;
    private float[] worstFitVector = null;
    private double maxWorstFitDistance = -1.0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      dimension = PhoenixConfigurationUtil.getKMeansDimension(conf);
      metric = PhoenixConfigurationUtil.getKMeansDistanceMetric(conf);
      centroids = loadCentroidsInMapper(context);
      int k = centroids.size();
      accumulators = new CentroidWritable[k];
      for (int i = 0; i < k; i++) {
        accumulators[i] = new CentroidWritable(i, dimension);
      }
      worstFitVector = null;
      maxWorstFitDistance = -1.0;
    }

    @Override
    protected void map(NullWritable key, VectorWritable value, Context context)
      throws IOException, InterruptedException {
      float[] v = value.getVector();
      if (v == null || (dimension > 0 && v.length != dimension)) {
        return;
      }
      double bestDist = Double.MAX_VALUE;
      int bestC = 0;
      int k = centroids.size();
      for (int c = 0; c < k; c++) {
        double d = computeDistance(v, centroids.get(c), dimension, metric);
        if (d < bestDist) {
          bestDist = d;
          bestC = c;
        }
      }
      accumulators[bestC].addVector(v, bestDist);
      if (bestDist > maxWorstFitDistance) {
        maxWorstFitDistance = bestDist;
        worstFitVector = v.clone();
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable outKey = new IntWritable();
      for (int c = 0; c < accumulators.length; c++) {
        if (accumulators[c].getCount() > 0) {
          outKey.set(c);
          context.write(outKey, accumulators[c]);
        }
      }
      if (worstFitVector != null) {
        double[] wfSum = new double[dimension];
        for (int d = 0; d < dimension; d++) {
          wfSum[d] = worstFitVector[d];
        }
        outKey.set(-1);
        context.write(outKey, new CentroidWritable(-1, wfSum, 1, maxWorstFitDistance));
      }
    }
  }

  /** Combiner: Merges partial centroid sums locally to reduce shuffle volume. */
  public static class KMeansAssignmentCombiner
    extends Reducer<IntWritable, CentroidWritable, IntWritable, CentroidWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<CentroidWritable> values, Context context)
      throws IOException, InterruptedException {
      CentroidWritable combined = null;
      for (CentroidWritable cw : values) {
        if (combined == null) {
          combined = new CentroidWritable(cw);
        } else {
          combined.add(cw);
        }
      }
      if (combined != null) {
        context.write(key, combined);
      }
    }
  }

  /** Reducer: Computes updated centroids, handles empty clusters, and writes outputs. */
  public static class KMeansUpdateReducer
    extends Reducer<IntWritable, CentroidWritable, IntWritable, VectorWritable> {

    private String metric;
    private int dimension;
    private int k;
    private List<float[]> oldCentroids;
    private float[] globalWorstFit = null;
    private double globalWorstFitDistance = -1.0;
    private Map<Integer, CentroidWritable> totals = new TreeMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      metric = PhoenixConfigurationUtil.getKMeansDistanceMetric(conf);
      dimension = PhoenixConfigurationUtil.getKMeansDimension(conf);
      k = PhoenixConfigurationUtil.getKMeansK(conf);
      String pathStr = conf.get(CONF_CURRENT_CENTROID_PATH);
      if (pathStr != null) {
        oldCentroids = readCentroids(conf, new Path(pathStr));
      } else {
        oldCentroids = new ArrayList<>();
      }
      totals.clear();
      globalWorstFit = null;
      globalWorstFitDistance = -1.0;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<CentroidWritable> values, Context context)
      throws IOException, InterruptedException {
      int centroidId = key.get();
      if (centroidId == -1) {
        for (CentroidWritable cw : values) {
          if (cw.getDistance() > globalWorstFitDistance) {
            globalWorstFitDistance = cw.getDistance();
            float[] cand = new float[dimension];
            double[] sum = cw.getPartialSum();
            for (int d = 0; d < Math.min(cand.length, sum.length); d++) {
              cand[d] = (float) sum[d];
            }
            globalWorstFit = cand;
          }
        }
      } else {
        CentroidWritable total =
          totals.computeIfAbsent(centroidId, id -> new CentroidWritable(id, dimension));
        for (CentroidWritable cw : values) {
          total.add(cw);
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable outKey = new IntWritable();
      double totalDistortion = 0.0;
      long[] clusterSizes = new long[k];

      for (int c = 0; c < k; c++) {
        CentroidWritable total = totals.get(c);
        float[] updatedCentroid;
        if (total != null && total.getCount() > 0) {
          updatedCentroid = total.computeCentroid(metric);
          totalDistortion += total.getDistance();
          clusterSizes[c] = total.getCount();
        } else {
          // Handle empty clusters by re-seeding with the global worst-fit vector or retaining
          // previous coordinates
          if (globalWorstFit != null) {
            updatedCentroid = Arrays.copyOf(globalWorstFit, globalWorstFit.length);
            if ("COSINE".equalsIgnoreCase(metric)) {
              updatedCentroid = KMeansTrainer.l2Normalize(updatedCentroid);
            }
            LOGGER.warn("Re-seeding empty cluster {} with worst-fit vector", c);
          } else if (c < oldCentroids.size()) {
            updatedCentroid = oldCentroids.get(c);
            LOGGER.warn("Cluster {} is empty, retaining previous centroid", c);
          } else {
            updatedCentroid = new float[dimension];
          }
          clusterSizes[c] = 0;
        }

        outKey.set(c);
        context.write(outKey, new VectorWritable(updatedCentroid));
      }

      // Record iteration distortion and cluster size distributions in a side file for driver
      // monitoring
      Configuration conf = context.getConfiguration();
      String outDirStr = conf.get(CONF_CURRENT_ITER_OUTPUT);
      if (outDirStr != null) {
        Path summaryFile = new Path(new Path(outDirStr), "_summary");
        FileSystem fs = summaryFile.getFileSystem(conf);
        try (BufferedWriter bw = new BufferedWriter(
          new OutputStreamWriter(fs.create(summaryFile, true), StandardCharsets.UTF_8))) {
          bw.write(Double.toString(totalDistortion));
          bw.newLine();
          bw.write(Integer.toString(k));
          bw.newLine();
          for (int c = 0; c < k; c++) {
            if (c > 0) {
              bw.write(" ");
            }
            bw.write(Long.toString(clusterSizes[c]));
          }
          bw.newLine();
        }
      }
    }
  }

  /** Mapper for cluster size calculation. */
  public static class KMeansClusterSizeMapper
    extends Mapper<NullWritable, VectorWritable, IntWritable, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private List<float[]> centroids;
    private int dimension;
    private String metric;
    private IntWritable outKey = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      dimension = PhoenixConfigurationUtil.getKMeansDimension(conf);
      metric = PhoenixConfigurationUtil.getKMeansDistanceMetric(conf);
      centroids = loadCentroidsInMapper(context);
    }

    @Override
    protected void map(NullWritable key, VectorWritable value, Context context)
      throws IOException, InterruptedException {
      float[] v = value.getVector();
      if (v == null) {
        return;
      }
      double bestDist = Double.MAX_VALUE;
      int bestC = 0;
      for (int c = 0; c < centroids.size(); c++) {
        double d = computeDistance(v, centroids.get(c), dimension, metric);
        if (d < bestDist) {
          bestDist = d;
          bestC = c;
        }
      }
      outKey.set(bestC);
      context.write(outKey, ONE);
    }
  }

  /** Reducer for cluster size summation. */
  public static class KMeansClusterSizeReducer
    extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable outVal = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      outVal.set(sum);
      context.write(key, outVal);
    }
  }

  /** Implements k-means|| for large k > parallelInitThreshold. */
  public static List<float[]> initializeKMeansParallel(List<float[]> sampledVectors, int k, int dim,
    String metric, Random random) {
    if (sampledVectors == null || sampledVectors.isEmpty() || k <= 0) {
      throw new IllegalArgumentException("Invalid input to initializeKMeansParallel");
    }
    int n = sampledVectors.size();
    if (k >= n) {
      List<float[]> res = new ArrayList<>();
      for (float[] v : sampledVectors) {
        res.add(Arrays.copyOf(v, v.length));
      }
      return res;
    }

    // K-means|| scalable initialization: sample candidate centroids across multiple rounds
    // proportional to squared distance from already chosen candidates, then reduce to k centroids.
    List<float[]> candidates = new ArrayList<>();
    float[] first = sampledVectors.get(random.nextInt(n)).clone();
    if ("COSINE".equalsIgnoreCase(metric)) {
      first = KMeansTrainer.l2Normalize(first);
    }
    candidates.add(first);

    int rounds = 5;
    for (int r = 0; r < rounds && candidates.size() < 2 * k; r++) {
      double[] minDists = new double[n];
      double totalDist = 0.0;
      for (int i = 0; i < n; i++) {
        float[] v = sampledVectors.get(i);
        double dMin = Double.MAX_VALUE;
        for (float[] c : candidates) {
          double d = computeDistance(v, c, dim, metric);
          if (d < dMin) {
            dMin = d;
          }
        }
        minDists[i] = dMin;
        totalDist += dMin;
      }

      if (totalDist <= 1e-12) {
        break;
      }

      for (int i = 0; i < n; i++) {
        double prob = (2.0 * k * minDists[i]) / totalDist;
        if (random.nextDouble() < prob) {
          float[] cand = sampledVectors.get(i).clone();
          if ("COSINE".equalsIgnoreCase(metric)) {
            cand = KMeansTrainer.l2Normalize(cand);
          }
          candidates.add(cand);
        }
      }
    }

    while (candidates.size() < k) {
      float[] extra = sampledVectors.get(random.nextInt(n)).clone();
      if ("COSINE".equalsIgnoreCase(metric)) {
        extra = KMeansTrainer.l2Normalize(extra);
      }
      candidates.add(extra);
    }

    return KMeansTrainer.initializeKMeansPlusPlus(candidates, k, metric, random);
  }

  /** Executes the full distributed K-Means MapReduce training pipeline. */
  public static KMeansResult run(Configuration callerConf, String tableName, String vectorColumn,
    int dimension, int k, KMeansConfig config)
    throws IOException, InterruptedException, ClassNotFoundException {

    // Defensive copy isolates job configuration, preventing parameters such as temporary HDFS
    // work directories from leaking across runs sharing the same ConnectionQueryServices
    // configuration.
    Configuration conf = new Configuration(callerConf);

    if (tableName == null || tableName.trim().isEmpty()) {
      throw new IllegalArgumentException("tableName must not be null or empty");
    }
    if (vectorColumn == null || vectorColumn.trim().isEmpty()) {
      throw new IllegalArgumentException("vectorColumn must not be null or empty");
    }
    if (k <= 0) {
      throw new IllegalArgumentException("k must be > 0: " + k);
    }
    if (config == null) {
      config = KMeansConfig.defaultConfig();
    }

    String workDirStr = PhoenixConfigurationUtil.getKMeansHdfsWorkDir(conf);
    Path workDir;
    boolean cleanUpWorkDir = false;
    if (workDirStr == null || workDirStr.trim().isEmpty()) {
      workDir = new Path("/tmp/phoenix-kmeans-" + UUID.randomUUID().toString());
      cleanUpWorkDir = true;
    } else {
      workDir = new Path(workDirStr);
    }

    FileSystem fs = workDir.getFileSystem(conf);
    fs.mkdirs(workDir);

    boolean succeeded = false;
    try {
      PhoenixConfigurationUtil.setKMeansTableName(conf, tableName);
      PhoenixConfigurationUtil.setKMeansVectorColumn(conf, vectorColumn);
      PhoenixConfigurationUtil.setKMeansDimension(conf, dimension);
      PhoenixConfigurationUtil.setKMeansK(conf, k);
      PhoenixConfigurationUtil.setKMeansSampleSize(conf, config.getSampleSize());
      PhoenixConfigurationUtil.setKMeansDistanceMetric(conf, config.getDistanceMetric());
      PhoenixConfigurationUtil.setKMeansMaxIterations(conf, config.getMaxIterations());
      PhoenixConfigurationUtil.setKMeansConvergenceThreshold(conf,
        config.getConvergenceThreshold());
      if (config.getRandomSeed() != null) {
        PhoenixConfigurationUtil.setKMeansRandomSeed(conf, config.getRandomSeed());
      }
      PhoenixConfigurationUtil.setKMeansParallelInitThreshold(conf,
        config.getParallelInitThreshold());
      PhoenixConfigurationUtil.setKMeansHdfsWorkDir(conf, workDir.toString());

      Path sampleDir = new Path(workDir, "samples");
      if (!fs.exists(sampleDir)) {
        Job samplingJob = KMeansDistributedSampler.createSamplingJob(conf, sampleDir);
        boolean success = samplingJob.waitForCompletion(true);
        if (!success) {
          throw new IOException("Phase A distributed sampling MapReduce job failed");
        }
      }

      List<float[]> sampledVectors = KMeansDistributedSampler.readSampledVectors(conf, sampleDir);
      if (sampledVectors.isEmpty()) {
        throw new IllegalStateException("No vectors sampled from table " + tableName);
      }

      int n = sampledVectors.size();
      int actualDim = sampledVectors.get(0).length;
      if (dimension <= 0) {
        dimension = actualDim;
        PhoenixConfigurationUtil.setKMeansDimension(conf, dimension);
      }
      int targetK = Math.min(k, n);
      PhoenixConfigurationUtil.setKMeansK(conf, targetK);

      // Check for an existing completion marker to return previously trained centroids
      Path completeMarker = new Path(workDir, "_complete");
      if (fs.exists(completeMarker)) {
        Path finalCentroidsFile = new Path(workDir, "final_centroids.seq");
        if (fs.exists(finalCentroidsFile)) {
          LOGGER.info("K-Means training already complete. Reading final centroids from {}",
            finalCentroidsFile);
          List<float[]> finalCentroids = readCentroids(conf, finalCentroidsFile);
          Path centroidBaseDir = new Path(workDir, "centroids");
          Checkpoint lastCp = findLastCheckpoint(fs, centroidBaseDir, conf, sampledVectors,
            dimension, config.getDistanceMetric());
          int completedIterations = (lastCp != null && lastCp.iteration > 0) ? lastCp.iteration : 1;

          List<Double> distortionHistory = new ArrayList<>();
          for (int i = 1; i <= completedIterations; i++) {
            Path iterDir = new Path(centroidBaseDir, "iter_" + i);
            SummaryMetrics sm = readSummaryMetrics(fs, iterDir, sampledVectors, finalCentroids,
              dimension, config.getDistanceMetric());
            distortionHistory.add(sm.distortion);
          }

          int[] assignments = new int[n];
          int effectiveK = finalCentroids.size();
          int[] clusterSizes = new int[effectiveK];
          double totalDist = 0.0;
          for (int i = 0; i < n; i++) {
            float[] v = sampledVectors.get(i);
            double bestDist = Double.MAX_VALUE;
            int bestC = 0;
            for (int c = 0; c < effectiveK; c++) {
              double d =
                computeDistance(v, finalCentroids.get(c), dimension, config.getDistanceMetric());
              if (d < bestDist) {
                bestDist = d;
                bestC = c;
              }
            }
            assignments[i] = bestC;
            clusterSizes[bestC]++;
            totalDist += bestDist;
          }
          boolean hasSplit = effectiveK > targetK;
          ClusterSkewMetrics skewMetrics = ClusterSkewMetrics.compute(clusterSizes);
          succeeded = true;
          return new KMeansResult(finalCentroids, targetK, completedIterations, true, totalDist,
            clusterSizes, assignments, skewMetrics, skewMetrics, hasSplit ? skewMetrics : null,
            hasSplit, dimension, config.getDistanceMetric(), distortionHistory);
        }
      }

      Random random =
        config.getRandomSeed() != null ? new Random(config.getRandomSeed()) : new Random();

      Path centroidBaseDir = new Path(workDir, "centroids");
      Path convergedMarker = new Path(workDir, "_converged");

      // Resume training from the latest valid HDFS iteration checkpoint if available
      Checkpoint checkpoint = findLastCheckpoint(fs, centroidBaseDir, conf, sampledVectors,
        dimension, config.getDistanceMetric());

      List<float[]> currentCentroids;
      Path currentCentroidFile;
      double prevDistortion = Double.MAX_VALUE;
      double currDistortion = 0.0;
      boolean converged = false;
      int[] clusterSizes = new int[targetK];
      List<Double> distortionHistory = new ArrayList<>();
      int startIteration;

      if (fs.exists(convergedMarker) && checkpoint != null && checkpoint.iteration > 0) {
        LOGGER.info("Resuming K-Means after convergence from checkpoint at iteration {}",
          checkpoint.iteration);
        currentCentroids = checkpoint.centroids;
        currentCentroidFile = checkpoint.centroidFile;
        currDistortion = checkpoint.distortion;
        clusterSizes = checkpoint.clusterSizes;
        converged = true;
        startIteration = checkpoint.iteration + 1;
        for (int i = 1; i <= checkpoint.iteration; i++) {
          Path iterDir = new Path(centroidBaseDir, "iter_" + i);
          SummaryMetrics sm = readSummaryMetrics(fs, iterDir, sampledVectors, currentCentroids,
            dimension, config.getDistanceMetric());
          distortionHistory.add(sm.distortion);
        }
      } else if (checkpoint != null && checkpoint.iteration > 0) {
        LOGGER.info("Resuming K-Means from checkpoint at iteration {}", checkpoint.iteration);
        currentCentroids = checkpoint.centroids;
        currentCentroidFile = checkpoint.centroidFile;
        prevDistortion = checkpoint.distortion;
        clusterSizes = checkpoint.clusterSizes;
        startIteration = checkpoint.iteration + 1;
        for (int i = 1; i <= checkpoint.iteration; i++) {
          Path iterDir = new Path(centroidBaseDir, "iter_" + i);
          SummaryMetrics sm = readSummaryMetrics(fs, iterDir, sampledVectors, currentCentroids,
            dimension, config.getDistanceMetric());
          distortionHistory.add(sm.distortion);
        }
      } else {
        // Phase B: Centroid Initialization
        Path iter0File = new Path(new Path(centroidBaseDir, "iter_0"), "centroids.seq");
        if (checkpoint == null || checkpoint.iteration < 0) {
          List<float[]> initialCentroids;
          if (targetK > config.getParallelInitThreshold()) {
            LOGGER.info("Using k-means|| scalable initialization (k = {} > threshold {})", targetK,
              config.getParallelInitThreshold());
            initialCentroids = initializeKMeansParallel(sampledVectors, targetK, dimension,
              config.getDistanceMetric(), random);
          } else {
            LOGGER.info("Using k-means++ initialization (k = {})", targetK);
            initialCentroids = KMeansTrainer.initializeKMeansPlusPlus(sampledVectors, targetK,
              config.getDistanceMetric(), random);
          }
          writeCentroids(conf, iter0File, initialCentroids);
          currentCentroids = initialCentroids;
          currentCentroidFile = iter0File;
        } else {
          // Resume from initial centroids at iteration 0
          currentCentroids = checkpoint.centroids;
          currentCentroidFile = checkpoint.centroidFile;
        }
        startIteration = 1;
      }

      int maxRetries = PhoenixConfigurationUtil.getKMeansMaxRetries(conf);
      int iteration = (checkpoint != null && checkpoint.iteration > 0) ? checkpoint.iteration : 0;

      if (!converged) {
        for (iteration = startIteration; iteration <= config.getMaxIterations(); iteration++) {
          Path iterOutputDir = new Path(centroidBaseDir, "iter_" + iteration);
          int retries = 0;
          boolean iterSuccess = false;

          while (!iterSuccess && retries <= maxRetries) {
            try {
              if (iterationFailureHook != null) {
                Runnable hook = iterationFailureHook;
                iterationFailureHook = null;
                hook.run();
              }

              if (fs.exists(iterOutputDir)) {
                // Verify whether a prior attempt already wrote valid iteration centroids
                try {
                  List<float[]> existing = readCentroids(conf, iterOutputDir);
                  if (existing != null && !existing.isEmpty()) {
                    LOGGER.info("Iteration {} output already exists with {} centroids, skipping",
                      iteration, existing.size());
                    iterSuccess = true;
                    break;
                  }
                } catch (Exception e) {
                  LOGGER.debug("Could not read existing iteration output, will re-run", e);
                }
                fs.delete(iterOutputDir, true);
              }

              Job iterJob = Job.getInstance(conf, "Phoenix KMeans Iteration " + iteration);
              iterJob.setJarByClass(KMeansTool.class);
              PhoenixMapReduceUtil.addPhoenixDependencyJars(iterJob.getConfiguration());

              Path qualifiedCentroidPath = fs.makeQualified(currentCentroidFile);
              iterJob.getConfiguration().set(CONF_CURRENT_CENTROID_PATH,
                qualifiedCentroidPath.toString());
              iterJob.getConfiguration().set(CONF_CURRENT_ITER_OUTPUT, iterOutputDir.toString());
              iterJob.addCacheFile(qualifiedCentroidPath.toUri());

              iterJob.setInputFormatClass(SequenceFileInputFormat.class);
              SequenceFileInputFormat.setInputPaths(iterJob, sampleDir);

              iterJob.setMapperClass(KMeansAssignmentMapper.class);
              iterJob.setMapOutputKeyClass(IntWritable.class);
              iterJob.setMapOutputValueClass(CentroidWritable.class);

              iterJob.setCombinerClass(KMeansAssignmentCombiner.class);

              iterJob.setReducerClass(KMeansUpdateReducer.class);
              iterJob.setOutputKeyClass(IntWritable.class);
              iterJob.setOutputValueClass(VectorWritable.class);
              iterJob.setNumReduceTasks(1);

              iterJob.setOutputFormatClass(SequenceFileOutputFormat.class);
              SequenceFileOutputFormat.setOutputPath(iterJob, iterOutputDir);

              boolean ok = iterJob.waitForCompletion(true);
              if (!ok) {
                throw new IOException("Iteration " + iteration + " MapReduce job returned false");
              }
              iterSuccess = true;
            } catch (Throwable t) {
              retries++;
              LOGGER.warn("Iteration {} failed on attempt {}/{}", iteration, retries,
                maxRetries + 1, t);
              if (retries > maxRetries) {
                throw new IOException(
                  "Iteration " + iteration + " failed after " + retries + " attempts", t);
              }
            }
          }

          List<float[]> newCentroids = readCentroids(conf, iterOutputDir);
          if (newCentroids == null || newCentroids.isEmpty()) {
            throw new IOException("No centroids produced in iteration " + iteration);
          }
          currentCentroids = newCentroids;

          SummaryMetrics summary = readSummaryMetrics(fs, iterOutputDir, sampledVectors,
            currentCentroids, dimension, config.getDistanceMetric());
          currDistortion = summary.distortion;
          clusterSizes = summary.clusterSizes;
          distortionHistory.add(currDistortion);
          LOGGER.info("K-Means distributed iteration {}: distortion = {}", iteration,
            currDistortion);

          // Evaluate convergence based on relative reduction in total distortion
          if (iteration > 1) {
            if (prevDistortion > 0.0) {
              double relChange = Math.abs(prevDistortion - currDistortion) / prevDistortion;
              if (relChange < config.getConvergenceThreshold()) {
                converged = true;
                LOGGER.info("K-Means converged at iteration {} with relChange {}", iteration,
                  relChange);
                break;
              }
            } else if (currDistortion == 0.0) {
              converged = true;
              LOGGER.info("K-Means converged at iteration {} with 0 distortion", iteration);
              break;
            }
          } else if (currDistortion == 0.0) {
            converged = true;
            LOGGER.info("K-Means converged at iteration 1 with 0 distortion");
            break;
          }

          prevDistortion = currDistortion;
          currentCentroidFile = findCentroidFile(fs, iterOutputDir);
        }

        if (converged) {
          if (!fs.exists(convergedMarker)) {
            fs.createNewFile(convergedMarker);
          }
        }
      }

      int finalIterations;
      if (converged) {
        finalIterations = (iteration > 0 && iteration <= config.getMaxIterations())
          ? iteration
          : (checkpoint != null && checkpoint.iteration > 0
            ? checkpoint.iteration
            : config.getMaxIterations());
      } else {
        finalIterations = Math.min(iteration, config.getMaxIterations());
      }

      // Phase D: Post-Training Split Heuristic

      int[] assignments = new int[n];
      for (int i = 0; i < n; i++) {
        float[] v = sampledVectors.get(i);
        double bestDist = Double.MAX_VALUE;
        int bestC = 0;
        for (int c = 0; c < currentCentroids.size(); c++) {
          double d =
            computeDistance(v, currentCentroids.get(c), dimension, config.getDistanceMetric());
          if (d < bestDist) {
            bestDist = d;
            bestC = c;
          }
        }
        assignments[i] = bestC;
      }

      ClusterSkewMetrics preSplitSkew = ClusterSkewMetrics.compute(clusterSizes);
      ClusterSkewMetrics postSplitSkew = null;
      boolean hasSplit = false;

      if (config.isEnableSplitHeuristic() && n >= 2 * targetK) {
        double threshold = config.getSplitThresholdMultiplier() * ((double) n / targetK);
        Set<Integer> overloaded = new HashSet<>();
        for (int c = 0; c < targetK; c++) {
          if (clusterSizes[c] > threshold) {
            overloaded.add(c);
          }
        }

        if (!overloaded.isEmpty()) {
          hasSplit = true;
          List<float[]> newCentroidList = new ArrayList<>();
          for (int c = 0; c < targetK; c++) {
            if (overloaded.contains(c)) {
              List<float[]> clusterVecs = new ArrayList<>(clusterSizes[c]);
              for (int i = 0; i < n; i++) {
                if (assignments[i] == c) {
                  clusterVecs.add(sampledVectors.get(i));
                }
              }
              List<float[]> subCentroids = KMeansTrainer.initializeKMeansPlusPlus(clusterVecs, 2,
                config.getDistanceMetric(), random);
              newCentroidList.add(subCentroids.get(0));
              newCentroidList.add(subCentroids.get(1));
            } else {
              newCentroidList.add(currentCentroids.get(c));
            }
          }

          currentCentroids = newCentroidList;
          int effectiveK = currentCentroids.size();
          clusterSizes = new int[effectiveK];
          double[][] splitSums = new double[effectiveK][dimension];

          int rebalancePasses = Math.max(1, config.getSplitRebalanceIterations());
          for (int pass = 0; pass < rebalancePasses; pass++) {
            Arrays.fill(clusterSizes, 0);
            for (int c = 0; c < effectiveK; c++) {
              Arrays.fill(splitSums[c], 0.0);
            }
            currDistortion = 0.0;
            for (int i = 0; i < n; i++) {
              float[] v = sampledVectors.get(i);
              double bestDist = Double.MAX_VALUE;
              int bestC = 0;
              for (int c = 0; c < effectiveK; c++) {
                double d = computeDistance(v, currentCentroids.get(c), dimension,
                  config.getDistanceMetric());
                if (d < bestDist) {
                  bestDist = d;
                  bestC = c;
                }
              }
              assignments[i] = bestC;
              clusterSizes[bestC]++;
              currDistortion += bestDist;
              double[] sums = splitSums[bestC];
              for (int d = 0; d < dimension; d++) {
                sums[d] += v[d];
              }
            }
            for (int c = 0; c < effectiveK; c++) {
              if (clusterSizes[c] > 0) {
                float[] updated = new float[dimension];
                double count = clusterSizes[c];
                double[] sums = splitSums[c];
                for (int d = 0; d < dimension; d++) {
                  updated[d] = (float) (sums[d] / count);
                }
                if ("COSINE".equalsIgnoreCase(config.getDistanceMetric())) {
                  updated = KMeansTrainer.l2Normalize(updated);
                }
                currentCentroids.set(c, updated);
              }
            }
          }

          postSplitSkew = ClusterSkewMetrics.compute(clusterSizes);
          LOGGER.info("Distributed K-Means split rebalance complete: effectiveK = {}", effectiveK);
        }
      }

      ClusterSkewMetrics finalSkew = (postSplitSkew != null) ? postSplitSkew : preSplitSkew;

      if (converged) {
        completeMarker = new Path(workDir, "_complete");
        writeCentroids(conf, new Path(workDir, "final_centroids.seq"), currentCentroids);
        if (!fs.exists(completeMarker)) {
          fs.createNewFile(completeMarker);
        }
      }

      succeeded = true;

      return new KMeansResult(currentCentroids, targetK, finalIterations, converged, currDistortion,
        clusterSizes, assignments, finalSkew, preSplitSkew, postSplitSkew, hasSplit, dimension,
        config.getDistanceMetric(), distortionHistory);

    } finally {
      if (cleanUpWorkDir && succeeded) {
        try {
          fs.delete(workDir, true);
        } catch (Exception e) {
          LOGGER.warn("Failed to clean up temporary HDFS work dir {}", workDir, e);
        }
      } else if (cleanUpWorkDir && !succeeded) {
        LOGGER.warn("K-Means training failed. Intermediate state preserved at {}. "
          + "Supply this path via phoenix.vector.kmeans.hdfs.work.dir to resume.", workDir);
      }
    }
  }

  private static class Checkpoint {
    final int iteration;
    final List<float[]> centroids;
    final Path centroidFile;
    final double distortion;
    final int[] clusterSizes;

    Checkpoint(int iteration, List<float[]> centroids, Path centroidFile, double distortion,
      int[] clusterSizes) {
      this.iteration = iteration;
      this.centroids = centroids;
      this.centroidFile = centroidFile;
      this.distortion = distortion;
      this.clusterSizes = clusterSizes;
    }
  }

  private static Checkpoint findLastCheckpoint(FileSystem fs, Path centroidBaseDir,
    Configuration conf, List<float[]> sampledVectors, int dim, String metric) throws IOException {
    if (!fs.exists(centroidBaseDir)) {
      return null;
    }
    FileStatus[] dirs = fs.listStatus(centroidBaseDir);
    if (dirs == null || dirs.length == 0) {
      return null;
    }
    List<Integer> iterNums = new ArrayList<>();
    for (FileStatus dir : dirs) {
      String name = dir.getPath().getName();
      if (name.startsWith("iter_") && dir.isDirectory()) {
        try {
          iterNums.add(Integer.parseInt(name.substring(5)));
        } catch (NumberFormatException ignored) {
        }
      }
    }
    iterNums.sort((a, b) -> Integer.compare(b, a));
    int maxIter = -1;
    List<float[]> centroids = null;
    Path checkpointDir = null;
    for (int iterNum : iterNums) {
      Path dir = new Path(centroidBaseDir, "iter_" + iterNum);
      try {
        List<float[]> c = readCentroids(conf, dir);
        if (c != null && !c.isEmpty()) {
          maxIter = iterNum;
          centroids = c;
          checkpointDir = dir;
          break;
        }
      } catch (Exception ignored) {
      }
    }
    if (maxIter < 0 || centroids == null || checkpointDir == null) {
      return null;
    }
    Path centroidFile = findCentroidFile(fs, checkpointDir);
    SummaryMetrics summary =
      readSummaryMetrics(fs, checkpointDir, sampledVectors, centroids, dim, metric);
    return new Checkpoint(maxIter, centroids, centroidFile, summary.distortion,
      summary.clusterSizes);
  }

  private static class SummaryMetrics {
    final double distortion;
    final int[] clusterSizes;

    SummaryMetrics(double distortion, int[] clusterSizes) {
      this.distortion = distortion;
      this.clusterSizes = clusterSizes;
    }
  }

  private static SummaryMetrics readSummaryMetrics(FileSystem fs, Path iterOutputDir,
    List<float[]> sampledVectors, List<float[]> centroids, int dim, String metric) {
    Path summaryFile = new Path(iterOutputDir, "_summary");
    try {
      if (fs.exists(summaryFile)) {
        try (BufferedReader br =
          new BufferedReader(new InputStreamReader(fs.open(summaryFile), StandardCharsets.UTF_8))) {
          String distLine = br.readLine();
          String kLine = br.readLine();
          String sizesLine = br.readLine();
          if (distLine != null && kLine != null && sizesLine != null) {
            double dist = Double.parseDouble(distLine.trim());
            int k = Integer.parseInt(kLine.trim());
            String[] parts = sizesLine.trim().split("\\s+");
            int[] sizes = new int[k];
            for (int i = 0; i < Math.min(k, parts.length); i++) {
              sizes[i] = (int) Long.parseLong(parts[i]);
            }
            return new SummaryMetrics(dist, sizes);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Could not read _summary file from {}, recalculating", iterOutputDir, e);
    }

    // Recompute summary metrics directly from sampled vectors if the side file is unavailable
    int k = centroids.size();
    int[] sizes = new int[k];
    double totalDist = 0.0;
    for (float[] v : sampledVectors) {
      double bestDist = Double.MAX_VALUE;
      int bestC = 0;
      for (int c = 0; c < k; c++) {
        double d = computeDistance(v, centroids.get(c), dim, metric);
        if (d < bestDist) {
          bestDist = d;
          bestC = c;
        }
      }
      sizes[bestC]++;
      totalDist += bestDist;
    }
    return new SummaryMetrics(totalDist, sizes);
  }
}
