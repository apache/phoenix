package org.apache.hbase.index.scanner;

import java.io.IOException;
import java.util.SortedSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

import org.apache.hbase.index.covered.KeyValueStore;

/**
 * Combine a simplified version of the logic in the ScanQueryMatcher and the KeyValueScanner. We can
 * get away with this here because we are only concerned with a single MemStore for the index; we
 * don't need to worry about multiple column families or minimizing seeking through file - we just
 * want to iterate the kvs quickly, in-memory.
 */
public class FilteredKeyValueScanner implements KeyValueScanner {

  private KeyValueScanner delegate;
  private Filter filter;

  public FilteredKeyValueScanner(Filter filter, KeyValueStore store) {
    this(filter, store.getScanner());
  }

  private FilteredKeyValueScanner(Filter filter, KeyValueScanner delegate) {
    this.delegate = delegate;
    this.filter = filter;
  }

  @Override
  public KeyValue peek() {
    return delegate.peek();
  }

  /**
   * Same a {@link KeyValueScanner#next()} except that we filter out the next {@link KeyValue} until
   * we find one that passes the filter.
   * @return the next {@link KeyValue} or <tt>null</tt> if no next {@link KeyValue} is present and
   *         passes all the filters.
   */
  @Override
  public KeyValue next() throws IOException {
    seekToNextUnfilteredKeyValue();
    return delegate.next();
  }

  @Override
  public boolean seek(KeyValue key) throws IOException {
    if(filter.filterAllRemaining()){
      return false;
    }
    // see if we can seek to the next key
    if (!delegate.seek(key)) {
      return false;
    }

    return seekToNextUnfilteredKeyValue();
  }

  private boolean seekToNextUnfilteredKeyValue() throws IOException {
    while (true) {
      KeyValue peeked = delegate.peek();
      // no more key values, so we are done
      if (peeked == null) {
        return false;
      }

      // filter the peeked value to see if it should be served
      ReturnCode code = filter.filterKeyValue(peeked);
      switch (code) {
      // included, so we are done
      case INCLUDE:
      case INCLUDE_AND_NEXT_COL:
        return true;
      // not included, so we need to go to the next row
      case SKIP:
      case NEXT_COL:
      case NEXT_ROW:
        delegate.next();
        break;
      // use a seek hint to find out where we should go
      case SEEK_NEXT_USING_HINT:
        delegate.seek(filter.getNextKeyHint(peeked));
      }
    }
  }

  @Override
  public boolean reseek(KeyValue key) throws IOException {
    this.delegate.reseek(key);
    return this.seekToNextUnfilteredKeyValue();
  }

  @Override
  public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom) throws IOException {
    return this.reseek(kv);
  }

  @Override
  public boolean isFileScanner() {
    return false;
  }

  @Override
  public long getSequenceID() {
    return this.delegate.getSequenceID();
  }

  @Override
  public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns, long oldestUnexpiredTS) {
    throw new UnsupportedOperationException(this.getClass().getName()
        + " doesn't support checking to see if it should use a scanner!");
  }


  @Override
  public boolean realSeekDone() {
    return this.delegate.realSeekDone();
  }

  @Override
  public void enforceSeek() throws IOException {
    this.delegate.enforceSeek();
  }

  @Override
  public void close() {
    this.delegate.close();
  }
}