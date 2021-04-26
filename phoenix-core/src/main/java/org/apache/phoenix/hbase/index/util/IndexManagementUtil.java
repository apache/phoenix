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
package org.apache.phoenix.hbase.index.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.builder.FatalIndexBuildingFailureException;
import org.apache.phoenix.hbase.index.builder.IndexBuildingFailureException;
import org.apache.phoenix.hbase.index.covered.Batch;
import org.apache.phoenix.hbase.index.covered.data.LazyValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.scanner.ScannerBuilder.CoveredDeleteScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.primitives.Longs;

/**
 * Utility class to help manage indexes
 */
public class IndexManagementUtil {

    private IndexManagementUtil() {
        // private ctor for util classes
    }

    // Don't rely on statically defined classes constants from classes that may not exist
    // in earlier HBase versions
    public static final String INDEX_WAL_EDIT_CODEC_CLASS_NAME = "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec";
    public static final String HLOG_READER_IMPL_KEY = "hbase.regionserver.hlog.reader.impl";
    public static final String WAL_EDIT_CODEC_CLASS_KEY = "hbase.regionserver.wal.codec";

    private static final String INDEX_HLOG_READER_CLASS_NAME = "org.apache.hadoop.hbase.regionserver.wal.IndexedHLogReader";
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexManagementUtil.class);

    public static boolean isWALEditCodecSet(Configuration conf) {
        // check to see if the WALEditCodec is installed
        try {
            // Use reflection to load the IndexedWALEditCodec, since it may not load with an older version
            // of HBase
            Class.forName(INDEX_WAL_EDIT_CODEC_CLASS_NAME);
        } catch (Throwable t) {
            return false;
        }
    if (INDEX_WAL_EDIT_CODEC_CLASS_NAME.equals(conf
        .get(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, null))) {
            // its installed, and it can handle compression and non-compression cases
            return true;
        }
        return false;
    }

    public static void ensureMutableIndexingCorrectlyConfigured(Configuration conf) throws IllegalStateException {

        // check to see if the WALEditCodec is installed
        if (isWALEditCodecSet(conf)) { return; }

        // otherwise, we have to install the indexedhlogreader, but it cannot have compression
        String codecClass = INDEX_WAL_EDIT_CODEC_CLASS_NAME;
        String indexLogReaderName = INDEX_HLOG_READER_CLASS_NAME;
        try {
            // Use reflection to load the IndexedHLogReader, since it may not load with an older version
            // of HBase
            Class.forName(indexLogReaderName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(codecClass + " is not installed, but "
                    + indexLogReaderName + " hasn't been installed in hbase-site.xml under " + HLOG_READER_IMPL_KEY);
        }
        if (indexLogReaderName.equals(conf.get(HLOG_READER_IMPL_KEY, indexLogReaderName))) {
            if (conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false)) { throw new IllegalStateException(
                    "WAL Compression is only supported with " + codecClass
            + ". You can install in hbase-site.xml, under " + WALCellCodec.WAL_CELL_CODEC_CLASS_KEY);
      }
        } else {
            throw new IllegalStateException(codecClass + " is not installed, but "
                    + indexLogReaderName + " hasn't been installed in hbase-site.xml under " + HLOG_READER_IMPL_KEY);
        }

    }

    public static ValueGetter createGetterFromScanner(CoveredDeleteScanner scanner, byte[] currentRow) {
        return scanner!=null ? new LazyValueGetter(scanner, currentRow) : null;
    }

    /**
     * check to see if the kvs in the update match any of the passed columns. Generally, this is useful to for an index
     * codec to determine if a given update should even be indexed. This assumes that for any index, there are going to
     * small number of columns, versus the number of kvs in any one batch.
     */
    public static boolean updateMatchesColumns(Collection<KeyValue> update, List<ColumnReference> columns) {
        // check to see if the kvs in the new update even match any of the columns requested
        // assuming that for any index, there are going to small number of columns, versus the number of
        // kvs in any one batch.
        boolean matches = false;
        outer: for (KeyValue kv : update) {
            for (ColumnReference ref : columns) {
                if (ref.matchesFamily(kv.getFamilyArray(), kv.getFamilyOffset(),
                    kv.getFamilyLength())
                        && ref.matchesQualifier(kv.getQualifierArray(), kv.getQualifierOffset(),
                            kv.getQualifierLength())) {
                    matches = true;
                    // if a single column matches a single kv, we need to build a whole scanner
                    break outer;
                }
            }
        }
        return matches;
    }

    /**
     * Check to see if the kvs in the update match any of the passed columns. Generally, this is useful to for an index
     * codec to determine if a given update should even be indexed. This assumes that for any index, there are going to
     * small number of kvs, versus the number of columns in any one batch.
     * <p>
     * This employs the same logic as {@link #updateMatchesColumns(Collection, List)}, but is flips the iteration logic
     * to search columns before kvs.
     */
    public static boolean columnMatchesUpdate(List<ColumnReference> columns, Collection<KeyValue> update) {
        boolean matches = false;
        outer: for (ColumnReference ref : columns) {
            for (KeyValue kv : update) {
                if (ref.matchesFamily(kv.getFamilyArray(), kv.getFamilyOffset(),
                    kv.getFamilyLength())
                        && ref.matchesQualifier(kv.getQualifierArray(), kv.getQualifierOffset(),
                            kv.getQualifierLength())) {
                    matches = true;
                    // if a single column matches a single kv, we need to build a whole scanner
                    break outer;
                }
            }
        }
        return matches;
    }

    public static Scan newLocalStateScan(List<? extends Iterable<? extends ColumnReference>> refsArray) {
        return newLocalStateScan(null, refsArray);
    }

    public static Scan newLocalStateScan(Scan scan, List<? extends Iterable<? extends ColumnReference>> refsArray) {
        Scan s = scan;
        if (scan == null) {
            s = new Scan();
        }
        s.setRaw(true);
        // add the necessary columns to the scan
        for (Iterable<? extends ColumnReference> refs : refsArray) {
            for (ColumnReference ref : refs) {
                s.addFamily(ref.getFamily());
            }
        }
        s.setMaxVersions();
        return s;
    }

    /**
     * Propagate the given failure as a generic {@link IOException}, if it isn't already
     * 
     * @param e
     *            reason indexing failed. If ,<tt>null</tt>, throws a {@link NullPointerException}, which should unload
     *            the coprocessor.
     */
    public static void rethrowIndexingException(Throwable e) throws IOException {
        try {
            throw e;
        } catch (IOException | FatalIndexBuildingFailureException e1) {
            LOGGER.info("Rethrowing " + e);
            throw e1;
        }
        catch (Throwable e1) {
            LOGGER.info("Rethrowing " + e1 + " as a " +
                    IndexBuildingFailureException.class.getSimpleName());
            throw new IndexBuildingFailureException("Failed to build index for unexpected reason!", e1);
        }
    }

    public static void setIfNotSet(Configuration conf, String key, int value) {
        if (conf.get(key) == null) {
            conf.setInt(key, value);
        }
    }

    /**
     * Batch all the {@link KeyValue}s in a collection of kvs by timestamp. Updates any {@link KeyValue} with a
     * timestamp == {@link HConstants#LATEST_TIMESTAMP} to the timestamp at the time the method is called.
     * 
     * @param kvs {@link KeyValue}s to break into batches
     * @param batches to update with the given kvs
     */
    public static void createTimestampBatchesFromKeyValues(Collection<KeyValue> kvs, Map<Long, Batch> batches) {
        // batch kvs by timestamp
        for (KeyValue kv : kvs) {
            long ts = kv.getTimestamp();
            Batch batch = batches.get(ts);
            if (batch == null) {
                batch = new Batch(ts);
                batches.put(ts, batch);
            }
            batch.add(kv);
        }
    }

    /**
     * Batch all the {@link KeyValue}s in a {@link Mutation} by timestamp. Updates any {@link KeyValue} with a timestamp
     * == {@link HConstants#LATEST_TIMESTAMP} to the timestamp at the time the method is called.
     * 
     * @param m {@link Mutation} from which to extract the {@link KeyValue}s
     * @return the mutation, broken into batches and sorted in ascending order (smallest first)
     */
    public static Collection<Batch> createTimestampBatchesFromMutation(Mutation m) {
        Map<Long, Batch> batches = new HashMap<Long, Batch>();
        for (List<Cell> family : m.getFamilyCellMap().values()) {
            List<KeyValue> familyKVs = KeyValueUtil.ensureKeyValues(family);
            createTimestampBatchesFromKeyValues(familyKVs, batches);
        }
        // sort the batches
        List<Batch> sorted = new ArrayList<Batch>(batches.values());
        Collections.sort(sorted, new Comparator<Batch>() {
            @Override
            public int compare(Batch o1, Batch o2) {
                return Longs.compare(o1.getTimestamp(), o2.getTimestamp());
            }
        });
        return sorted;
    }

    public static Collection<? extends Mutation> flattenMutationsByTimestamp(Collection<? extends Mutation> mutations) {
          List<Mutation> flattenedMutations = Lists.newArrayListWithExpectedSize(mutations.size() * 10);
          for (Mutation m : mutations) {
              byte[] row = m.getRow();
              Collection<Batch> batches = createTimestampBatchesFromMutation(m);
              for (Batch batch : batches) {
                  Mutation mWithSameTS;
                  Cell firstCell = batch.getKvs().get(0);
                  if (KeyValue.Type.codeToType(firstCell.getTypeByte()) == KeyValue.Type.Put) {
                      mWithSameTS = new Put(row);
                  } else {
                      mWithSameTS = new Delete(row);
                  }
                  if (m.getAttributesMap() != null) {
                      for (Map.Entry<String,byte[]> entry : m.getAttributesMap().entrySet()) {
                          mWithSameTS.setAttribute(entry.getKey(), entry.getValue());
                      }
                  }
                  for (Cell cell : batch.getKvs()) {
                      byte[] fam = CellUtil.cloneFamily(cell);
                      List<Cell> famCells = mWithSameTS.getFamilyCellMap().get(fam);
                      if (famCells == null) {
                          famCells = Lists.newArrayList();
                          mWithSameTS.getFamilyCellMap().put(fam, famCells);
                      }
                      famCells.add(cell);
                  }
                  flattenedMutations.add(mWithSameTS);
              }
          }
          return flattenedMutations;
      }
}