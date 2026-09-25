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
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.index.vector.KMeansConfig;
import org.apache.phoenix.index.vector.KMeansResult;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.PosixParser;

/** Orchestration entry point for distributed K-Means centroid training. */
public class KMeansTool extends Configured implements Tool {

  private static final Logger LOGGER = LoggerFactory.getLogger(KMeansTool.class);

  private static final Option TABLE_OPTION =
    new Option("t", "table", true, "Source Phoenix table name");
  private static final Option VECTOR_COLUMN_OPTION =
    new Option("vc", "vector-column", true, "Vector column name");
  private static final Option DIMENSION_OPTION =
    new Option("d", "dimension", true, "Vector dimension (auto-detected if omitted)");
  private static final Option CENTROIDS_OPTION =
    new Option("k", "centroids", true, "Target number of centroids (k)");
  private static final Option METRIC_OPTION =
    new Option("m", "metric", true, "Distance metric: L2 (default), COSINE, INNER_PRODUCT");
  private static final Option SAMPLE_SIZE_OPTION =
    new Option("s", "sample-size", true, "Reservoir sample size (default 10000)");
  private static final Option MAX_ITERATIONS_OPTION =
    new Option("i", "max-iterations", true, "Maximum Lloyd's iterations (default 100)");
  private static final Option CONVERGENCE_THRESHOLD_OPTION = new Option("ct",
    "convergence-threshold", true, "Convergence relative distortion threshold (default 1e-4)");
  private static final Option RANDOM_SEED_OPTION =
    new Option("rs", "random-seed", true, "Random seed for deterministic training");
  private static final Option OUTPUT_DIR_OPTION =
    new Option("o", "output-dir", true, "HDFS working directory for sample and centroid files");
  private static final Option LOCAL_OPTION =
    new Option("l", "local", false, "Force local (client-side) training fallback");
  private static final Option HELP_OPTION =
    new Option("h", "help", false, "Print this help message");

  /**
   * Entry point for distributed K-Means centroid training.
   * @param conf         Hadoop configuration
   * @param tableName    source table name
   * @param vectorColumn vector column name
   * @param dimension    vector dimension (or <= 0 for auto-detection)
   * @param k            target centroid count
   * @param config       k-means training configuration
   * @return {@link KMeansResult} containing trained centroids and skew metrics
   * @throws IOException          on I/O or MapReduce error
   * @throws InterruptedException if interrupted
   */
  public static KMeansResult trainDistributed(Configuration conf, String tableName,
    String vectorColumn, int dimension, int k, KMeansConfig config)
    throws IOException, InterruptedException {

    if (config == null) {
      config = KMeansConfig.defaultConfig();
    }

    if (PhoenixConfigurationUtil.isKMeansLocal(conf)) {
      LOGGER.info("phoenix.vector.kmeans.local is true: using client-side KMeansTrainer");
      try {
        String url = QueryUtil.getConnectionUrl(new Properties(), conf);
        try (Connection conn = DriverManager.getConnection(url)) {
          List<float[]> sampled =
            KMeansTrainer.sampleVectors(conn, tableName, vectorColumn, config.getSampleSize());
          return KMeansTrainer.train(sampled, k, config);
        }
      } catch (Exception e) {
        throw new IOException("Failed to run local k-means training fallback", e);
      }
    }

    try {
      return KMeansIterationDriver.run(conf, tableName, vectorColumn, dimension, k, config);
    } catch (ClassNotFoundException e) {
      throw new IOException("MapReduce class resolution error during K-Means training", e);
    }
  }

  private Options buildOptions() {
    Options options = new Options();
    options.addOption(TABLE_OPTION);
    options.addOption(VECTOR_COLUMN_OPTION);
    options.addOption(DIMENSION_OPTION);
    options.addOption(CENTROIDS_OPTION);
    options.addOption(METRIC_OPTION);
    options.addOption(SAMPLE_SIZE_OPTION);
    options.addOption(MAX_ITERATIONS_OPTION);
    options.addOption(CONVERGENCE_THRESHOLD_OPTION);
    options.addOption(RANDOM_SEED_OPTION);
    options.addOption(OUTPUT_DIR_OPTION);
    options.addOption(LOCAL_OPTION);
    options.addOption(HELP_OPTION);
    return options;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = buildOptions();
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println("Failed to parse arguments: " + e.getMessage());
      new HelpFormatter().printHelp("KMeansTool", options);
      return 1;
    }

    if (cmd.hasOption("h")) {
      new HelpFormatter().printHelp("KMeansTool", options);
      return 0;
    }

    if (!cmd.hasOption("t") || !cmd.hasOption("vc") || !cmd.hasOption("k")) {
      System.err
        .println("Missing required options: -t (table), -vc (vector-column), -k (centroids)");
      new HelpFormatter().printHelp("KMeansTool", options);
      return 1;
    }

    String tableName = cmd.getOptionValue("t");
    String vectorColumn = cmd.getOptionValue("vc");
    int k = Integer.parseInt(cmd.getOptionValue("k"));
    int dimension = cmd.hasOption("d") ? Integer.parseInt(cmd.getOptionValue("d")) : 0;

    KMeansConfig.Builder configBuilder = KMeansConfig.newBuilder();
    if (cmd.hasOption("m")) {
      configBuilder.distanceMetric(cmd.getOptionValue("m"));
    }
    if (cmd.hasOption("s")) {
      configBuilder.sampleSize(Integer.parseInt(cmd.getOptionValue("s")));
    }
    if (cmd.hasOption("i")) {
      configBuilder.maxIterations(Integer.parseInt(cmd.getOptionValue("i")));
    }
    if (cmd.hasOption("ct")) {
      configBuilder.convergenceThreshold(Double.parseDouble(cmd.getOptionValue("ct")));
    }
    if (cmd.hasOption("rs")) {
      configBuilder.randomSeed(Long.parseLong(cmd.getOptionValue("rs")));
    }

    Configuration conf = getConf();
    if (cmd.hasOption("o")) {
      PhoenixConfigurationUtil.setKMeansHdfsWorkDir(conf, cmd.getOptionValue("o"));
    }
    if (cmd.hasOption("l")) {
      PhoenixConfigurationUtil.setKMeansLocal(conf, true);
    }

    KMeansResult result =
      trainDistributed(conf, tableName, vectorColumn, dimension, k, configBuilder.build());

    System.out.println("Distributed K-Means Training Completed Successfully.");
    System.out.println(
      "Effective K: " + result.getEffectiveK() + " (requested: " + result.getRequestedK() + ")");
    System.out.println(
      "Iterations: " + result.getIterations() + " (converged: " + result.isConverged() + ")");
    System.out.println("Final Distortion: " + result.getFinalDistortion());
    System.out.println("Skew CV: " + result.getSkewMetrics().getCoefficientOfVariation());
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(), new KMeansTool(), args);
    System.exit(exitCode);
  }
}
