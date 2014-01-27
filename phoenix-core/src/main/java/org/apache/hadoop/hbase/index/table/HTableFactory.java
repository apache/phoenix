package org.apache.hadoop.hbase.index.table;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;

import org.apache.hadoop.hbase.index.util.ImmutableBytesPtr;

public interface HTableFactory {

  public HTableInterface getTable(ImmutableBytesPtr tablename) throws IOException;

  public void shutdown();
}