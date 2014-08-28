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
package org.apache.phoenix.schema.stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.StatCollectorProtos;
import org.apache.phoenix.coprocessor.generated.StatCollectorProtos.StatCollectRequest;
import org.apache.phoenix.coprocessor.generated.StatCollectorProtos.StatCollectResponse;
import org.apache.phoenix.coprocessor.generated.StatCollectorProtos.StatCollectResponse.Builder;
import org.apache.phoenix.coprocessor.generated.StatCollectorProtos.StatCollectService;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.util.SchemaUtil;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * An endpoint implementation that allows to collect the stats for a given
 * region and groups the stat per family. This is also an RegionObserver that
 * collects the information on compaction also. The user would be allowed to
 * invoke this endpoint and thus populate the Phoenix stats table with the max
 * key, min key and guide posts for the given region. The stats can be consumed
 * by the stats associated with every PTable and the same can be used to
 * parallelize the queries
 */
public class StatisticsCollector extends BaseRegionObserver implements CoprocessorService,
    Coprocessor, StatisticsTracker, StatCollectService.Interface {

  public static void addToTable(HTableDescriptor desc) throws IOException {
    desc.addCoprocessor(StatisticsCollector.class.getName());
  }

  private byte[] min;
  private byte[] max;
  private long guidepostDepth;
  private long byteCount = 0;
  private List<byte[]> guidePosts = new ArrayList<byte[]>();
  private Set<byte[]> familyMap = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  private RegionCoprocessorEnvironment env;
  protected StatisticsTable stats;
  private static final Log LOG = LogFactory.getLog(StatCollectService.class);

  @Override
  public void collectStat(RpcController controller, StatCollectRequest request,
      RpcCallback<StatCollectResponse> done) {
    HRegion region = env.getRegion();
    if (familyMap != null) {
      familyMap.clear();
    }
    Scan scan = new Scan();
    // We should have a mechanism to set the caching here.
    // TODO : Should the scan object be coming from the endpoint?
    scan.setCaching(1000);
    // TODO : Should we scan fully or should we do as done StatsManagerImpl
    // reading the
    // first row and the
    // last row after setting scan.setReserved(true). But doing so we cannot get
    // the guideposts
    RegionScanner scanner = null;
    int count = 0;
    try {
      scanner = region.getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMore = true;
      while (hasMore) {
        // Am getting duplicates here?  Need to avoid that
        hasMore = scanner.next(results);
        updateStat(results);
        count += results.size();
        results.clear();
        while (!hasMore) {
          break;
        }
      }
    } catch (IOException e) {
      LOG.error(e);
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
          IOException toThrow = null;
          try {
            // update the statistics table
            for (byte[] fam : familyMap) {
              byte[] tableKey = SchemaUtil.getTableKey(request.getTenantIdBytes().toByteArray(),
                  request.getSchemaNameBytes().toByteArray(), region.getRegionInfo().getTable()
                      .getName());
              stats.updateStats(new StatisticsWriter(tableKey,
                  Bytes.toBytes(region.getRegionInfo().getRegionNameAsString())), this, fam);
            }
          } catch (IOException e) {
            LOG.error("Failed to update statistics table!", e);
            toThrow = e;
          }
        } catch (IOException e) {
          LOG.error(e);
          ResponseConverter.setControllerException(controller, e);
        }
      }
    }
    Builder newBuilder = StatCollectResponse.newBuilder();
    newBuilder.setRowsScanned(count);
    StatCollectResponse result = newBuilder.build();
    done.run(result);
  }

  /**
   * Update the current statistics based on the lastest batch of key-values from
   * the underlying scanner
   * 
   * @param results
   *          next batch of {@link KeyValue}s
   */
  protected void updateStat(final List<Cell> results) {
    for (Cell c : results) {
      updateStatistic(KeyValueUtil.ensureKeyValue(c));
    }
  }

  @Override
  public Service getService() {
    return StatCollectorProtos.StatCollectService.newReflectiveService(this);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
    HTableDescriptor desc = ((RegionCoprocessorEnvironment) env).getRegion().getTableDesc();
    // Get the stats table associated with the current table on which the CP is
    // triggered
    stats = StatisticsTable.getStatisticsTableForCoprocessor(env, desc.getName());
    guidepostDepth = env.getConfiguration().getLong(
        StatisticsConstants.HISTOGRAM_BYTE_DEPTH_CONF_KEY,
        StatisticsConstants.HISTOGRAM_DEFAULT_BYTE_DEPTH);
  }

  @Override
  public void stop(CoprocessorEnvironment arg0) throws IOException {
    stats.close();
  }

  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    // TODO : Major concern here is that we cannot get the complete name of the table with schema and tenantId here.  
    InternalScanner internalScan = s;
    // See if this is for Major compaction
    if (scanType.equals(ScanType.COMPACT_DROP_DELETES)) {
      // this is the first CP accessed, so we need to just create a major
      // compaction scanner, just
      // like in the compactor
      if (s == null) {
        Scan scan = new Scan();
        scan.setMaxVersions(store.getFamily().getMaxVersions());
        long smallestReadPoint = store.getSmallestReadPoint();
        internalScan = new StoreScanner(store, store.getScanInfo(), scan, scanners, scanType,
            smallestReadPoint, earliestPutTs);
      }
      InternalScanner scanner = getInternalScanner(c, store, internalScan,
          store.getColumnFamilyName());
      if (scanner != null) {
        internalScan = scanner;
      }
    }
    return internalScan;
  }

  protected InternalScanner getInternalScanner(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, InternalScanner internalScan, String family) {
    return new StatisticsScanner(this, stats, c.getEnvironment().getRegion().getRegionInfo(),
        internalScan, Bytes.toBytes(family));
  }

  @Override
  public void clear() {
    this.max = null;
    this.min = null;
    this.guidePosts = null;
  }

  @Override
  public void updateStatistic(KeyValue kv) {
    // first time through, so both are null
    byte[] fam = kv.getFamily();
    familyMap.add(fam);
    if (min == null) {
      min = StatisticsUtils.copyRow(kv);
      // Ideally the max key also should be added in this case
      max = StatisticsUtils.copyRow(kv);
    } else {
      if (Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), min, 0,
          min.length) < 0) {
        min = StatisticsUtils.copyRow(kv);
      }
      if (Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), max, 0,
          max.length) > 0) {
        max = StatisticsUtils.copyRow(kv);
      }
    }
    byteCount += kv.getLength();
    if (byteCount >= guidepostDepth) {
      guidePosts.add(new ImmutableBytesPtr(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength())
          .copyBytesIfNecessary());
      // reset the count for the next key
      byteCount = 0;
    }
  }

  @Override
  public StatisticsValue getMaxKey() {
    if (max != null) {
      return new StatisticsValue(StatisticsConstants.MIN_MAX_STAT, StatisticsConstants.MAX_SUFFIX,
          max);
    }
    return null;
  }

  @Override
  public StatisticsValue getMinKey() {
    if (min != null) {
      return new StatisticsValue(StatisticsConstants.MIN_MAX_STAT, StatisticsConstants.MIN_SUFFIX,
          min);
    }
    return null;
  }

  @Override
  public StatisticsValue getGuidePosts() {
    byte[][] array = new byte[guidePosts.size()][];
    int i = 0;
    for (byte[] element : guidePosts) {
      array[i] = element;
      i++;
    }
    PhoenixArray phoenixArray = new PhoenixArray(PDataType.VARBINARY, array);
    if (guidePosts != null) {
      return new StatisticsValue(StatisticsConstants.GUIDE_POSTS_STATS,
          StatisticsConstants.GUIDE_POST_SUFFIX, PDataType.VARBINARY_ARRAY.toBytes(phoenixArray));
    }
    return null;
  }

}
