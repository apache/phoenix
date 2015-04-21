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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;

import com.google.common.collect.Maps;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.builder.IndexBuildingFailureException;
import org.apache.phoenix.hbase.index.covered.data.LazyValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.scanner.Scanner;

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
    private static final Log LOG = LogFactory.getLog(IndexManagementUtil.class);

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

    public static ValueGetter createGetterFromScanner(Scanner scanner, byte[] currentRow) {
        return new LazyValueGetter(scanner, currentRow);
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
                if (ref.matchesFamily(kv.getFamily()) && ref.matchesQualifier(kv.getQualifier())) {
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
                if (ref.matchesFamily(kv.getFamily()) && ref.matchesQualifier(kv.getQualifier())) {
                    matches = true;
                    // if a single column matches a single kv, we need to build a whole scanner
                    break outer;
                }
            }
        }
        return matches;
    }

    public static Scan newLocalStateScan(List<? extends Iterable<? extends ColumnReference>> refsArray) {
        Scan s = new Scan();
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
     *            reason indexing failed. If ,tt>null</tt>, throws a {@link NullPointerException}, which should unload
     *            the coprocessor.
     */
    public static void rethrowIndexingException(Throwable e) throws IOException {
        try {
            throw e;
        } catch (IOException e1) {
            LOG.info("Rethrowing " + e);
            throw e1;
        } catch (Throwable e1) {
            LOG.info("Rethrowing " + e1 + " as a " + IndexBuildingFailureException.class.getSimpleName());
            throw new IndexBuildingFailureException("Failed to build index for unexpected reason!", e1);
        }
    }

    public static void setIfNotSet(Configuration conf, String key, int value) {
        if (conf.get(key) == null) {
            conf.setInt(key, value);
        }
    }
}