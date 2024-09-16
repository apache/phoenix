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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.schema.stats.DefaultStatisticsCollector;
import org.apache.phoenix.schema.stats.NoOpStatisticsCollector;
import org.apache.phoenix.schema.stats.StatisticsCollector;
import org.apache.phoenix.schema.stats.StatisticsWriter;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Scan over a region from restored snapshot
 */
public class SnapshotScanner extends AbstractClientScanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotScanner.class);
  private final Scan scan;
  private RegionScanner scanner;
  private HRegion region;
  private List<Cell> values;
  private StatisticsCollector statisticsCollector;

  public SnapshotScanner(Configuration conf, FileSystem fs, Path rootDir,
      TableDescriptor htd, RegionInfo hri,  Scan scan) throws Throwable{

    LOGGER.info("Creating SnapshotScanner for region: " + hri);

    scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    values = new ArrayList<>();

    // TODO : Use hbase provided snapshot scanner (make it IA.LimitedPrivate)
    // region init should follow the same pattern as hbase ClientSideRegionScanner.
    initRegionForSnapshotScanner(conf, fs, rootDir, htd, hri);

    this.scan = scan;

    RegionCoprocessorEnvironment snapshotEnv = getSnapshotContextEnvironment(conf);

    // Collect statistics during scan if ANALYZE_TABLE attribute is set
    if (ScanUtil.isAnalyzeTable(scan)) {
      this.scanner = region.getScanner(scan);
      PhoenixConnection connection = (PhoenixConnection) ConnectionUtil.getInputConnection(conf, new Properties());
      String tableName = region.getTableDescriptor().getTableName().getNameAsString();
      TableName physicalTableName = SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, conf);
      Table table = connection.getQueryServices().getTable(physicalTableName.getName());
      StatisticsWriter statsWriter = StatisticsWriter.newWriter(connection, tableName, HConstants.LATEST_TIMESTAMP);
      statisticsCollector = new DefaultStatisticsCollector(conf, region,
              tableName, null, null, null, statsWriter, table);
    } else if (scan.getAttribute(BaseScannerRegionObserverConstants.NON_AGGREGATE_QUERY) != null) {
      RegionScannerFactory regionScannerFactory = new NonAggregateRegionScannerFactory(snapshotEnv);
      this.scanner = regionScannerFactory.getRegionScanner(scan, region.getScanner(scan));
      statisticsCollector = new NoOpStatisticsCollector();
    } else {
      /* future work : Snapshot M/R jobs for aggregate queries*/
      throw new UnsupportedOperationException("Snapshot M/R jobs not available for aggregate queries");
    }

    statisticsCollector.init();
    region.startRegionOperation();
  }

  /**
   * Initialize region for snapshot scanner utility. This is client side region initialization and
   * hence it should follow the same region init pattern as the one used by hbase
   * ClientSideRegionScanner.
   *
   * @param conf The configuration.
   * @param fs The filesystem instance.
   * @param rootDir Restored region root dir.
   * @param htd The table descriptor instance used to retrieve the region root dir.
   * @param hri The region info.
   * @throws IOException If region init throws IOException.
   */
  private void initRegionForSnapshotScanner(Configuration conf, FileSystem fs, Path rootDir,
                                            TableDescriptor htd,
                                            RegionInfo hri) throws IOException {
    region = HRegion.newHRegion(CommonFSUtils.getTableDir(rootDir, htd.getTableName()), null, fs,
            conf, hri, htd, null);
    region.setRestoredRegion(true);
    // non RS process does not have a block cache, and this a client side scanner,
    // create one for MapReduce jobs to cache the INDEX block by setting to use
    // IndexOnlyLruBlockCache and set a value to HBASE_CLIENT_SCANNER_BLOCK_CACHE_SIZE_KEY
    conf.set(BlockCacheFactory.BLOCKCACHE_POLICY_KEY, "IndexOnlyLRU");
    // HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY is only available from 2.4.6
    // We are using the string directly here to let Phoenix compile with earlier versions.
    // Note that it won't do anything before HBase 2.4.6
    conf.setIfUnset("hfile.onheap.block.cache.fixed.size",
            String.valueOf(32 * 1024 * 1024L));
    // don't allow L2 bucket cache for non RS process to avoid unexpected disk usage.
    conf.unset(HConstants.BUCKET_CACHE_IOENGINE_KEY);
    //PHOENIX-7367 - non RS process doesn't have MemstoreLab's ChunkCreator initialized
    //so we disable it to avoid NPE while closing the memstore as part of region close
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    region.setBlockCache(BlockCacheFactory.createBlockCache(conf));
    // we won't initialize the MobFileCache when not running in RS process. so provided an
    // initialized cache. Consider the case: an CF was set from an mob to non-mob. if we only
    // initialize cache for MOB region, NPE from HMobStore will still happen. So Initialize the
    // cache for every region although it may hasn't any mob CF, BTW the cache is very light-weight.
    region.setMobFileCache(new MobFileCache(conf));
    region.initialize();
  }


  @Override
  public Result next() throws IOException {
    values.clear();
    boolean hasMore = scanner.nextRaw(values);
    statisticsCollector.collectStatistics(values);
    if (hasMore || !values.isEmpty()) {
      return Result.create(values);
    } else {
      //we are done
      return null;
    }
  }

  @Override
  public void close() {
    if (this.scanner != null) {
      try {
        statisticsCollector.updateStatistics(region, scan);
        this.scanner.close();
        this.scanner = null;
      } catch (IOException e) {
        LOGGER.warn("Exception while closing scanner", e);
      }
    }
    if (this.region != null) {
      try {
        this.region.closeRegionOperation();
        this.region.close(true);
        this.region = null;
      } catch (IOException e) {
        LOGGER.warn("Exception while closing scanner", e);
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
      public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
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
      public ClassLoader getClassLoader() {
        throw new UnsupportedOperationException();
      }

    @Override
    public RegionCoprocessor getInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OnlineRegions getOnlineRegions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerName getServerName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection getConnection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExtendedCellBuilder getCellBuilder() {
        throw new UnsupportedOperationException();
    }
    };
  }
}
