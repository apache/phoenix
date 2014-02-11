package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.util.ConfigUtil;

/**
 * Do the WAL Replay test but with the WALEditCodec, rather than an {@link IndexedHLogReader}, but
 * still with compression
 */
public class TestWALReplayWithIndexWritesAndUncompressedWALInHBase_094_9 extends TestWALReplayWithIndexWritesAndCompressedWAL {

  @Override
  protected void configureCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    setDefaults(conf);
    LOG.info("Setting HLog impl to indexed log reader");
    conf.set(IndexManagementUtil.HLOG_READER_IMPL_KEY, IndexedHLogReader.class.getName());

    // disable WAL compression
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    // set replication required parameter
    ConfigUtil.setReplicationConfigIfAbsent(conf);
  }
}