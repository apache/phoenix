package org.apache.phoenix.replication.log;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * The parts of the HDFS FSDataInputStream interface contract that we want to generalize.
 */
public interface SeekableDataInput extends DataInput, Closeable, Seekable, PositionedReadable {

  int read(byte[] buf) throws IOException;

  int read(byte[] buf, int pos, int len) throws IOException;

}
