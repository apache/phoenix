package org.apache.hadoop.hbase.index.scanner;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;


/**
 * {@link Scanner} that has no underlying data
 */
public class EmptyScanner implements Scanner {

  @Override
  public KeyValue next() throws IOException {
    return null;
  }

  @Override
  public boolean seek(KeyValue next) throws IOException {
    return false;
  }

  @Override
  public KeyValue peek() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    // noop
  }
}