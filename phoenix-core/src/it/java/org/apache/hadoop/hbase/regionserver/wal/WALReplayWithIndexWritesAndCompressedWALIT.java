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

package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexTestingUtils;
import org.apache.phoenix.hbase.index.TableName;
import org.apache.phoenix.hbase.index.covered.example.ColumnGroup;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumn;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumnIndexSpecifierBuilder;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumnIndexer;
import org.apache.phoenix.util.ConfigUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * For pre-0.94.9 instances, this class tests correctly deserializing WALEdits w/o compression. Post
 * 0.94.9 we can support a custom  {@link WALCellCodec} which handles reading/writing the compressed
 * edits.
 * <p>
 * Most of the underlying work (creating/splitting the WAL, etc) is from
 * org.apache.hadoop.hhbase.regionserver.wal.TestWALReplay, copied here for completeness and ease of
 * use.
 * <p>
 * This test should only have a single test - otherwise we will start/stop the minicluster multiple
 * times, which is probably not what you want to do (mostly because its so much effort).
 */
@Category(NeedsOwnMiniClusterTest.class)
public class WALReplayWithIndexWritesAndCompressedWALIT {

  public static final Log LOG = LogFactory.getLog(TestWALReplay.class);
  @Rule
  public TableName table = new TableName();
  private String INDEX_TABLE_NAME = table.getTableNameString() + "_INDEX";

  final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private Path hbaseRootDir = null;
  private Path oldLogDir;
  private Path logDir;
  private FileSystem fs;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    setupCluster();
    Path hbaseRootDir = UTIL.getDataTestDir();
    this.conf = HBaseConfiguration.create(UTIL.getConfiguration());
    this.fs = UTIL.getDFSCluster().getFileSystem();
    this.hbaseRootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
    this.oldLogDir = new Path(this.hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    this.logDir = new Path(this.hbaseRootDir, HConstants.HREGION_LOGDIR_NAME);
  }

  private void setupCluster() throws Exception {
    configureCluster();
    startCluster();
  }

  protected void configureCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    setDefaults(conf);

    // enable WAL compression
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    // set replication required parameter
    ConfigUtil.setReplicationConfigIfAbsent(conf);
  }

  protected final void setDefaults(Configuration conf) {
    // make sure writers fail quickly
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 1000);
    conf.setInt("zookeeper.recovery.retry", 3);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 100);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 30000);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 5000);
    // enable appends
    conf.setBoolean("dfs.support.append", true);
    IndexTestingUtils.setupConfig(conf);
  }

  protected void startCluster() throws Exception {
    UTIL.startMiniDFSCluster(3);
    UTIL.startMiniZKCluster();

    Path hbaseRootDir = UTIL.getDFSCluster().getFileSystem().makeQualified(new Path("/hbase"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    UTIL.getConfiguration().set(HConstants.HBASE_DIR, hbaseRootDir.toString());
    UTIL.startMiniHBaseCluster(1, 1);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniHBaseCluster();
    UTIL.shutdownMiniDFSCluster();
    UTIL.shutdownMiniZKCluster();
  }


  private void deleteDir(final Path p) throws IOException {
    if (this.fs.exists(p)) {
      if (!this.fs.delete(p, true)) {
        throw new IOException("Failed remove of " + p);
      }
    }
  }

  /**
   * Test writing edits into an HRegion, closing it, splitting logs, opening Region again. Verify
   * seqids.
   * @throws Exception on failure
   */
  @SuppressWarnings("deprecation")
@Test
  public void testReplayEditsWrittenViaHRegion() throws Exception {
    final String tableNameStr = "testReplayEditsWrittenViaHRegion";
    final HRegionInfo hri = new HRegionInfo(org.apache.hadoop.hbase.TableName.valueOf(tableNameStr), 
        null, null, false);
    final Path basedir = FSUtils.getTableDir(hbaseRootDir, org.apache.hadoop.hbase.TableName.valueOf(tableNameStr));
    deleteDir(basedir);
    final HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    
    //setup basic indexing for the table
    // enable indexing to a non-existant index table
    byte[] family = new byte[] { 'a' };
    ColumnGroup fam1 = new ColumnGroup(INDEX_TABLE_NAME);
    fam1.add(new CoveredColumn(family, CoveredColumn.ALL_QUALIFIERS));
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);
    builder.build(htd);

    // create the region + its WAL
    HRegion region0 = HRegion.createHRegion(hri, hbaseRootDir, this.conf, htd);
    region0.close();
    region0.getWAL().close();

    WALFactory walFactory = new WALFactory(this.conf, null, "localhost,1234");

    WAL wal = createWAL(this.conf, walFactory);
    RegionServerServices mockRS = Mockito.mock(RegionServerServices.class);
    // mock out some of the internals of the RSS, so we can run CPs
    Mockito.when(mockRS.getWAL(null)).thenReturn(wal);
    RegionServerAccounting rsa = Mockito.mock(RegionServerAccounting.class);
    Mockito.when(mockRS.getRegionServerAccounting()).thenReturn(rsa);
    ServerName mockServerName = Mockito.mock(ServerName.class);
    Mockito.when(mockServerName.getServerName()).thenReturn(tableNameStr + ",1234");
    Mockito.when(mockRS.getServerName()).thenReturn(mockServerName);
    HRegion region = new HRegion(basedir, wal, this.fs, this.conf, hri, htd, mockRS);
    region.initialize();
    region.getSequenceId().set(0);

    //make an attempted write to the primary that should also be indexed
    byte[] rowkey = Bytes.toBytes("indexed_row_key");
    Put p = new Put(rowkey);
    p.add(family, Bytes.toBytes("qual"), Bytes.toBytes("value"));
    region.put(p);

    // we should then see the server go down
    Mockito.verify(mockRS, Mockito.times(1)).abort(Mockito.anyString(),
      Mockito.any(Exception.class));

    // then create the index table so we are successful on WAL replay
    CoveredColumnIndexer.createIndexTable(UTIL.getHBaseAdmin(), INDEX_TABLE_NAME);

    // run the WAL split and setup the region
    runWALSplit(this.conf, walFactory);
    WAL wal2 = createWAL(this.conf, walFactory);
    HRegion region1 = new HRegion(basedir, wal2, this.fs, this.conf, hri, htd, mockRS);

    // initialize the region - this should replay the WALEdits from the WAL
    region1.initialize();

    // now check to ensure that we wrote to the index table
    HTable index = new HTable(UTIL.getConfiguration(), INDEX_TABLE_NAME);
    int indexSize = getKeyValueCount(index);
    assertEquals("Index wasn't propertly updated from WAL replay!", 1, indexSize);
    Get g = new Get(rowkey);
    final Result result = region1.get(g);
    assertEquals("Primary region wasn't updated from WAL replay!", 1, result.size());

    // cleanup the index table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.disableTable(INDEX_TABLE_NAME);
    admin.deleteTable(INDEX_TABLE_NAME);
    admin.close();
  }

  /**
   * Create simple HTD with three families: 'a', 'b', and 'c'
   * @param tableName name of the table descriptor
   * @return
   */
  private HTableDescriptor createBasic3FamilyHTD(final String tableName) {
    @SuppressWarnings("deprecation")
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor a = new HColumnDescriptor(Bytes.toBytes("a"));
    htd.addFamily(a);
    HColumnDescriptor b = new HColumnDescriptor(Bytes.toBytes("b"));
    htd.addFamily(b);
    HColumnDescriptor c = new HColumnDescriptor(Bytes.toBytes("c"));
    htd.addFamily(c);
    return htd;
  }

  /*
   * @param c
   * @return WAL with retries set down from 5 to 1 only.
   * @throws IOException
   */
  private WAL createWAL(final Configuration c, WALFactory walFactory) throws IOException {
    WAL wal = walFactory.getWAL(new byte[]{});

    // Set down maximum recovery so we dfsclient doesn't linger retrying something
    // long gone.
    HBaseTestingUtility.setMaxRecoveryErrorCount(((FSHLog) wal).getOutputStream(), 1);
    return wal;
  }

  /*
   * Run the split. Verify only single split file made.
   * @param c
   * @return The single split file made
   * @throws IOException
   */
  private Path runWALSplit(final Configuration c, WALFactory walFactory) throws IOException {
    FileSystem fs = FileSystem.get(c);
    
    List<Path> splits = WALSplitter.split(this.hbaseRootDir, new Path(this.logDir, "localhost,1234"),
        this.oldLogDir, fs, c, walFactory);
    // Split should generate only 1 file since there's only 1 region
    assertEquals("splits=" + splits, 1, splits.size());
    // Make sure the file exists
    assertTrue(fs.exists(splits.get(0)));
    LOG.info("Split file=" + splits.get(0));
    return splits.get(0);
  }

  @SuppressWarnings("deprecation")
private int getKeyValueCount(HTable table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE - 1);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count += res.list().size();
      LOG.debug(count + ") " + res);
    }
    results.close();

    return count;
  }
}

