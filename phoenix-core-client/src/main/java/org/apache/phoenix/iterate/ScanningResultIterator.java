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
package org.apache.phoenix.iterate;

import static org.apache.phoenix.exception.SQLExceptionCode.OPERATION_TIMED_OUT;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.CombinableMetric;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

public class ScanningResultIterator implements ResultIterator {

  private static final Logger LOG = LoggerFactory.getLogger(ScanningResultIterator.class);
  private final ResultScanner scanner;
  private final ScanMetricsHolder scanMetricsHolder;
  boolean scanMetricsUpdated;
  boolean scanMetricsEnabled;
  private StatementContext context;
  private static boolean throwExceptionIfScannerClosedForceFully = false;

  private final boolean isMapReduceContext;
  private final long maxQueryEndTime;
  private final TableName tableName;
  private final boolean isScanMetricsByRegionEnabled;

  private long dummyRowCounter = 0;

  private final ScanningResultPostDummyResultCaller scanningResultPostDummyResultCaller;
  private final ScanningResultPostValidResultCaller scanningResultPostValidResultCaller;

  public ScanningResultIterator(ResultScanner scanner, Scan scan,
    ScanMetricsHolder scanMetricsHolder, StatementContext context, boolean isMapReduceContext,
    long maxQueryEndTime, TableName tableName) {
    this.scanner = scanner;
    this.tableName = tableName;
    this.scanMetricsHolder = scanMetricsHolder;
    this.context = context;
    scanMetricsUpdated = false;
    scanMetricsEnabled = scan.isScanMetricsEnabled();
    this.isMapReduceContext = isMapReduceContext;
    this.maxQueryEndTime = maxQueryEndTime;
    Class<? extends ScanningResultPostDummyResultCaller> dummyResultCallerClazz =
      context.getConnection().getQueryServices().getConfiguration().getClass(
        QueryServices.PHOENIX_POST_DUMMY_PROCESS, ScanningResultPostDummyResultCaller.class,
        ScanningResultPostDummyResultCaller.class);
    this.scanningResultPostDummyResultCaller = ReflectionUtils.newInstance(dummyResultCallerClazz,
      context.getConnection().getQueryServices().getConfiguration());
    Class<? extends ScanningResultPostValidResultCaller> validResultCallerClazz =
      context.getConnection().getQueryServices().getConfiguration().getClass(
        QueryServices.PHOENIX_POST_VALID_PROCESS, ScanningResultPostValidResultCaller.class,
        ScanningResultPostValidResultCaller.class);
    this.scanningResultPostValidResultCaller = ReflectionUtils.newInstance(validResultCallerClazz,
      context.getConnection().getQueryServices().getConfiguration());
    this.isScanMetricsByRegionEnabled = scan.isScanMetricsByRegionEnabled();
  }

  @Override
  public void close() throws SQLException {
    // close the scanner so that metrics are available
    scanner.close();
    updateMetrics();
  }

  private void changeMetric(CombinableMetric metric, Long value) {
    if (value != null) {
      metric.change(value);
    }
  }

  private void changeMetric(GlobalClientMetrics metric, Long value) {
    if (value != null) {
      metric.update(value);
    }
  }

  private void updateMetrics() {

    if (scanMetricsEnabled && !scanMetricsUpdated) {
      ScanMetrics scanMetrics = scanner.getScanMetrics();
      Map<String, Long> scanMetricsMap = scanMetrics.getMetricsMap();
      scanMetricsHolder.setScanMetricMap(scanMetricsMap);
      scanMetricsHolder.populateMetrics(dummyRowCounter);
      GlobalClientMetrics.populateMetrics(scanMetricsMap, dummyRowCounter);
      PhoenixConnection connection = context.getConnection();
      int slowestScanMetricsCount = connection.getSlowestScanMetricsCount();
      if (slowestScanMetricsCount > 0) {
        scanMetricsHolder.setScanMetricsByRegion(scanMetrics.collectMetricsByRegion());
        context.getSlowestScanMetricsQueue().add(scanMetricsHolder);
      }

      scanMetricsUpdated = true;
    }

  }

  @Override
  public Tuple next() throws SQLException {
    try {
      Result result = scanner.next();
      while (result != null && (result.isEmpty() || isDummy(result))) {
        dummyRowCounter += 1;
        long timeOutForScan = maxQueryEndTime - EnvironmentEdgeManager.currentTimeMillis();
        if (!context.getHasFirstValidResult() && timeOutForScan < 0) {
          throw new SQLExceptionInfo.Builder(OPERATION_TIMED_OUT)
            .setMessage(". Query couldn't be completed in the allotted time : "
              + context.getStatement().getQueryTimeoutInMillis() + " ms")
            .build().buildException();
        }
        if (
          !isMapReduceContext
            && (context.getConnection().isClosing() || context.getConnection().isClosed())
        ) {
          LOG.warn("Closing ResultScanner as Connection is already closed or in middle of closing");
          if (throwExceptionIfScannerClosedForceFully) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.FAILED_KNOWINGLY_FOR_TEST).build()
              .buildException();
          }
          close();
          return null;
        }
        processAfterRetrievingDummyResult();
        result = scanner.next();
      }
      if (result == null) {
        close(); // Free up resources early
        return null;
      }
      context.setHasFirstValidResult(true);
      // TODO: use ResultTuple.setResult(result)?
      // Need to create a new one if holding on to it (i.e. OrderedResultIterator)
      processAfterRetrievingValidResult();
      return new ResultTuple(result);
    } catch (IOException e) {
      throw ClientUtil.parseServerException(e);
    }
  }

  private void processAfterRetrievingDummyResult() {
    scanningResultPostDummyResultCaller.postDummyProcess();
  }

  private void processAfterRetrievingValidResult() {
    scanningResultPostValidResultCaller.postValidRowProcess();
  }

  @Override
  public void explain(List<String> planSteps) {
  }

  @Override
  public void explain(List<String> planSteps,
    ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
  }

  @Override
  public String toString() {
    return "ScanningResultIterator [scanner=" + scanner + "]";
  }

  public ResultScanner getScanner() {
    return scanner;
  }

  @VisibleForTesting
  public static void setIsScannerClosedForcefully(boolean throwException) {
    throwExceptionIfScannerClosedForceFully = throwException;
  }
}
