package org.apache.phoenix.iterate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ServerUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TableSnapshotResultIterator implements ResultIterator {

  private final Scan scan;
  private ResultIterator scanIterator;
  private Configuration configuration;
  private final ScanMetricsHolder scanMetricsHolder;
  private Tuple lastTuple = null;
  private static final ResultIterator UNINITIALIZED_SCANNER = ResultIterator.EMPTY_ITERATOR;
  private ArrayList<HRegionInfo> regions;
  private HTableDescriptor htd;
  private String snapshotName;

  private Path restoreDir;
  private Path rootDir;
  private FileSystem fs;
  private int currentRegion;
  private boolean closed = false;

  public TableSnapshotResultIterator(Configuration configuration, Scan scan, ScanMetricsHolder scanMetricsHolder)
      throws IOException {
    this.configuration = configuration;
    this.currentRegion = -1;
    this.scan = scan;
    this.scanMetricsHolder = scanMetricsHolder;
    this.scanIterator = UNINITIALIZED_SCANNER;
    this.restoreDir = new Path(configuration.get(PhoenixConfigurationUtil.RESTORE_DIR_KEY));
    this.snapshotName = configuration.get(
        PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
    this.rootDir = FSUtils.getRootDir(configuration);
    this.fs = rootDir.getFileSystem(configuration);
    init();
  }

  private void init() throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    HBaseProtos.SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(configuration, fs, snapshotDir, snapshotDesc);

    // load table descriptor
    this.htd = manifest.getTableDescriptor();

    List<SnapshotProtos.SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null) {
      throw new IllegalArgumentException("Snapshot seems empty");
    }

    this.regions = new ArrayList<>(regionManifests.size());
    for (SnapshotProtos.SnapshotRegionManifest regionManifest : regionManifests) {
      // load region descriptor
      HRegionInfo hri = HRegionInfo.convert(regionManifest.getRegionInfo());

      if (CellUtil.overlappingKeys(scan.getStartRow(), scan.getStopRow(),
          hri.getStartKey(), hri.getEndKey())) {
        this.regions.add(hri);
      }
    }

    Collections.sort(this.regions);
    RestoreSnapshotHelper.copySnapshotForScanner(configuration, fs,
        rootDir, restoreDir, snapshotName);
  }

  public boolean initSnapshotScanner() throws SQLException {
    if (closed) {
      return true;
    }
    ResultIterator delegate = this.scanIterator;
    if (delegate == UNINITIALIZED_SCANNER) {
      ++this.currentRegion;
      if (this.currentRegion >= this.regions.size())
        return false;
      try {
        HRegionInfo hri = regions.get(this.currentRegion);
        this.scanIterator =
            new ScanningResultIterator(new SnapshotScanner(configuration, fs, restoreDir, htd, hri, scan),
                scan, scanMetricsHolder);
      } catch (Throwable e) {
        throw ServerUtil.parseServerException(e);
      }
    }
    return true;
  }

  @Override
  public Tuple next() throws SQLException {
    while (true) {
      if (!initSnapshotScanner())
        return null;
      try {
        lastTuple = scanIterator.next();
        if (lastTuple != null) {
          ImmutableBytesWritable ptr = new ImmutableBytesWritable();
          lastTuple.getKey(ptr);
          return lastTuple;
        }
      } finally {
        if (lastTuple == null) {
          scanIterator.close();
          scanIterator = UNINITIALIZED_SCANNER;
        }
      }
    }
  }

  @Override
  public void close() throws SQLException {
    closed = true; // ok to say closed even if the below code throws an exception
    try {
      scanIterator.close();
      fs.delete(this.restoreDir, true);
    } catch (IOException e) {
      throw ServerUtil.parseServerException(e);
    } finally {
      scanIterator = UNINITIALIZED_SCANNER;
    }
  }

  @Override
  public void explain(List<String> planSteps) {
    // noop
  }

}
