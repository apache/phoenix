package org.apache.hadoop.hbase.index.table;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.index.util.ImmutableBytesPtr;
import org.apache.hadoop.hbase.index.util.IndexManagementUtil;

public class CoprocessorHTableFactory implements HTableFactory {

  /** Number of milliseconds per-interval to retry zookeeper */
  private static final String ZOOKEEPER_RECOVERY_RETRY_INTERVALMILL = "zookeeper.recovery.retry.intervalmill";
  /** Number of retries for zookeeper */
  private static final String ZOOKEEPER_RECOVERY_RETRY_KEY = "zookeeper.recovery.retry";
  private static final Log LOG = LogFactory.getLog(CoprocessorHTableFactory.class);
  private CoprocessorEnvironment e;

  public CoprocessorHTableFactory(CoprocessorEnvironment e) {
    this.e = e;
  }

  @Override
  public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException {
    Configuration conf = e.getConfiguration();
    // make sure writers fail fast
    IndexManagementUtil.setIfNotSet(conf, HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    IndexManagementUtil.setIfNotSet(conf, HConstants.HBASE_CLIENT_PAUSE, 1000);
    IndexManagementUtil.setIfNotSet(conf, ZOOKEEPER_RECOVERY_RETRY_KEY, 3);
    IndexManagementUtil.setIfNotSet(conf, ZOOKEEPER_RECOVERY_RETRY_INTERVALMILL, 100);
    IndexManagementUtil.setIfNotSet(conf, HConstants.ZK_SESSION_TIMEOUT, 30000);
    IndexManagementUtil.setIfNotSet(conf, HConstants.HBASE_RPC_TIMEOUT_KEY, 5000);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating new HTable: " + Bytes.toString(tablename.copyBytesIfNecessary()));
    }
    return this.e.getTable(tablename.copyBytesIfNecessary());
  }

  @Override
  public void shutdown() {
    // noop
  }
}