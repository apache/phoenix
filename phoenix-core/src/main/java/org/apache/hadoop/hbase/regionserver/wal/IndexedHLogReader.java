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

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.io.Writable;



/**
 * A WALReader that can also deserialize custom {@link WALEdit}s that contain index information.
 * <p>
 * This is basically a wrapper around a {@link SequenceFileLogReader} that has a custom
 * {@link SequenceFileLogReader.WALReader#next(Object)} method that only replaces the creation of the WALEdit with our own custom
 * type
 * <p>
 * This is a little bit of a painful way of going about this, but saves the effort of hacking the
 * HBase source (and deal with getting it reviewed and backported, etc.) and still works.
 */
/*
 * TODO: Support splitting index updates into their own WAL entries on recovery (basically, just
 * queue them up in next), if we know that the region was on the server when it crashed. However,
 * this is kind of difficult as we need to know a lot of things the state of the system - basically,
 * we need to track which of the regions were on the server when it crashed only only split those
 * edits out into their respective regions.
 */
public class IndexedHLogReader implements Reader {
  private static final Log LOG = LogFactory.getLog(IndexedHLogReader.class);

  private SequenceFileLogReader delegate;


  private static class IndexedWALReader extends SequenceFileLogReader.WALReader {

    /**
     * @param fs
     * @param p
     * @param c
     * @throws IOException
     */
    IndexedWALReader(FileSystem fs, Path p, Configuration c) throws IOException {
      super(fs, p, c);
    }

    /**
     * we basically have to reproduce what the SequenceFile.Reader is doing in next(), but without
     * the check on the value class, since we have a special value class that doesn't directly match
     * what was specified in the file header
     */
    @Override
    public synchronized boolean next(Writable key, Writable val) throws IOException {
      boolean more = next(key);

      if (more) {
        getCurrentValue(val);
      }

      return more;
    }

  }

  public IndexedHLogReader() {
    this.delegate = new SequenceFileLogReader();
  }

  @Override
  public void init(final FileSystem fs, final Path path, Configuration conf) throws IOException {
    this.delegate.init(fs, path, conf);
    // close the old reader and replace with our own, custom one
    this.delegate.reader.close();
    this.delegate.reader = new IndexedWALReader(fs, path, conf);
    Exception e = new Exception();
    LOG.info("Instantiated indexed log reader." + Arrays.toString(e.getStackTrace()));
    LOG.info("Got conf: " + conf);
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
  }

  @Override
  public Entry next() throws IOException {
    return next(null);
  }

  @Override
  public Entry next(Entry reuse) throws IOException {
    delegate.entryStart = delegate.reader.getPosition();
    HLog.Entry e = reuse;
    if (e == null) {
      HLogKey key;
      if (delegate.keyClass == null) {
        key = HLog.newKey(delegate.conf);
      } else {
        try {
          key = delegate.keyClass.newInstance();
        } catch (InstantiationException ie) {
          throw new IOException(ie);
        } catch (IllegalAccessException iae) {
          throw new IOException(iae);
        }
      }
      WALEdit val = new WALEdit();
      e = new HLog.Entry(key, val);
    }

    // now read in the HLog.Entry from the WAL
    boolean nextPairValid = false;
    try {
      if (delegate.compressionContext != null) {
        throw new UnsupportedOperationException(
            "Reading compression isn't supported with the IndexedHLogReader! Compresed WALEdits "
                + "are only support for HBase 0.94.9+ and with the IndexedWALEditCodec!");
      }
      // this is the special bit - we use our custom entry to read in the key-values that have index
      // information, but otherwise it looks just like a regular WALEdit
      IndexedWALEdit edit = new IndexedWALEdit(e.getEdit());
      nextPairValid = delegate.reader.next(e.getKey(), edit);
    } catch (IOException ioe) {
      throw delegate.addFileInfoToException(ioe);
    }
    delegate.edit++;
    if (delegate.compressionContext != null && delegate.emptyCompressionContext) {
      delegate.emptyCompressionContext = false;
    }
    return nextPairValid ? e : null;
  }

  @Override
  public void seek(long pos) throws IOException {
    this.delegate.seek(pos);
  }

  @Override
  public long getPosition() throws IOException {
    return this.delegate.getPosition();
  }

  @Override
  public void reset() throws IOException {
    this.delegate.reset();
  }
}