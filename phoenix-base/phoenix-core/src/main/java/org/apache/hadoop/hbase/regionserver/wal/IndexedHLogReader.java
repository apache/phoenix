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

  @Override
  protected void initAfterCompression() throws IOException {
      conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
      super.initAfterCompression();
  }
}