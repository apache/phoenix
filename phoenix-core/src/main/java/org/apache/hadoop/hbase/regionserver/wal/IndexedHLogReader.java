package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
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
public class IndexedHLogReader extends ProtobufLogReader {
  private static final Log LOG = LogFactory.getLog(IndexedHLogReader.class);

  @Override
  protected void initAfterCompression() throws IOException {
      conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
      super.initAfterCompression();
  }
}