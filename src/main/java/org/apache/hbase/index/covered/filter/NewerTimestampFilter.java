package org.apache.hbase.index.covered.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * Server-side only class used in the indexer to filter out keyvalues newer than a given timestamp
 * (so allows anything <code><=</code> timestamp through).
 * <p>
 * Note,<tt>this</tt> doesn't support {@link #write(DataOutput)} or {@link #readFields(DataInput)}.
 */
public class NewerTimestampFilter extends FilterBase {

  private long timestamp;

  public NewerTimestampFilter(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue ignored) {
    return ignored.getTimestamp() > timestamp ? ReturnCode.SKIP : ReturnCode.INCLUDE;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("TimestampFilter is server-side only!");
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("TimestampFilter is server-side only!");
  }
}