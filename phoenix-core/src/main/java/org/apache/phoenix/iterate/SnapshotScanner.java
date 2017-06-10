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

package org.apache.phoenix.iterate;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

public class SnapshotScanner extends AbstractClientScanner {

  private static final Log LOG = LogFactory.getLog(SnapshotScanner.class);

  private RegionScanner scanner = null;
  private HRegion region;
  List<Cell> values;

  public SnapshotScanner(Configuration conf, FileSystem fs, Path rootDir,
      HTableDescriptor htd, HRegionInfo hri,  Scan scan) throws Throwable{

    scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    values = new ArrayList<>();
    this.region = HRegion.openHRegion(conf, fs, rootDir, hri, htd, null, null, null);

    // process the region scanner for non-aggregate queries
    PTable.QualifierEncodingScheme encodingScheme = EncodedColumnsUtil.getQualifierEncodingScheme(scan);
    boolean useNewValueColumnQualifier = EncodedColumnsUtil.useNewValueColumnQualifier(scan);

    RegionCoprocessorEnvironment snapshotEnv = getSnapshotContextEnvironment(conf);

    RegionScannerFactory regionScannerFactory;
    if (scan.getAttribute(BaseScannerRegionObserver.NON_AGGREGATE_QUERY) != null) {
      regionScannerFactory = new NonAggregateRegionScannerFactory(snapshotEnv, useNewValueColumnQualifier, encodingScheme);
    } else {
      /* future work : Snapshot M/R jobs for aggregate queries*/
      throw new UnsupportedOperationException("Snapshot M/R jobs not available for aggregate queries");
    }

    this.scanner = regionScannerFactory.getRegionScanner(scan, region.getScanner(scan));
    region.startRegionOperation();
  }


  @Override
  public Result next() throws IOException {
    values.clear();
    scanner.nextRaw(values);
    if (values.isEmpty()) {
      //we are done
      return null;
    }

    return Result.create(values);
  }

  @Override
  public void close() {
    if (this.scanner != null) {
      try {
        this.scanner.close();
        this.scanner = null;
      } catch (IOException e) {
        LOG.warn("Exception while closing scanner", e);
      }
    }
    if (this.region != null) {
      try {
        this.region.closeRegionOperation();
        this.region.close(true);
        this.region = null;
      } catch (IOException e) {
        LOG.warn("Exception while closing scanner", e);
      }
    }
  }

  @Override
  public boolean renewLease() {
    return false;
  }

  private RegionCoprocessorEnvironment getSnapshotContextEnvironment(final Configuration conf) {
    return new RegionCoprocessorEnvironment() {
      @Override
      public Region getRegion() {
        return region;
      }

      @Override
      public HRegionInfo getRegionInfo() {
        return region.getRegionInfo();
      }

      @Override
      public RegionServerServices getRegionServerServices() {
        throw new UnsupportedOperationException();
      }

      @Override
      public ConcurrentMap<String, Object> getSharedData() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getVersion() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getHBaseVersion() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Coprocessor getInstance() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getPriority() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getLoadSequence() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Configuration getConfiguration() {
        return conf;
      }

      @Override
      public HTableInterface getTable(TableName tableName) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public HTableInterface getTable(TableName tableName, ExecutorService executorService)
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public ClassLoader getClassLoader() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
