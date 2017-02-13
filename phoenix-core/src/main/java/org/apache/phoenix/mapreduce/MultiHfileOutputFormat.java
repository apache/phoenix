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
package org.apache.phoenix.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.AbstractHFileWriter;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.phoenix.mapreduce.bulkload.TableRowkeyPair;
import org.apache.phoenix.mapreduce.bulkload.TargetTableRef;
import org.apache.phoenix.mapreduce.bulkload.TargetTableRefFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * The MultiHfileOutputFormat class simplifies writing HFiles for multiple tables.
 * It has been adapted from {#link HFileOutputFormat2} but differs from the fact it creates
 * HFiles for multiple tables.
 */
public class MultiHfileOutputFormat extends FileOutputFormat<TableRowkeyPair, Cell> {

    private static final Logger LOG = LoggerFactory.getLogger(MultiHfileOutputFormat.class);

    private static final String COMPRESSION_FAMILIES_CONF_KEY =
        "hbase.hfileoutputformat.families.compression";
    private static final String BLOOM_TYPE_FAMILIES_CONF_KEY =
        "hbase.hfileoutputformat.families.bloomtype";
    private static final String BLOCK_SIZE_FAMILIES_CONF_KEY =
        "hbase.mapreduce.hfileoutputformat.blocksize";
    private static final String DATABLOCK_ENCODING_FAMILIES_CONF_KEY =
        "hbase.mapreduce.hfileoutputformat.families.datablock.encoding";

    public static final String DATABLOCK_ENCODING_OVERRIDE_CONF_KEY =
        "hbase.mapreduce.hfileoutputformat.datablock.encoding";
    
    /* Delimiter property used to separate table name and column family */
    private static final String AT_DELIMITER = "@";
    
    @Override
    public RecordWriter<TableRowkeyPair, Cell> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return createRecordWriter(context);
    }

    /**
     * 
     * @param context
     * @return
     * @throws IOException 
     */
    static <V extends Cell> RecordWriter<TableRowkeyPair, V> createRecordWriter(final TaskAttemptContext context)
            throws IOException {
        // Get the path of the temporary output file
        final Path outputPath = FileOutputFormat.getOutputPath(context);
        final Path outputdir = new FileOutputCommitter(outputPath, context).getWorkPath();
        final Configuration conf = context.getConfiguration();
        final FileSystem fs = outputdir.getFileSystem(conf);
     
        final long maxsize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
            HConstants.DEFAULT_MAX_FILE_SIZE);
        // Invented config.  Add to hbase-*.xml if other than default compression.
        final String defaultCompressionStr = conf.get("hfile.compression",
            Compression.Algorithm.NONE.getName());
        final Algorithm defaultCompression = AbstractHFileWriter
            .compressionByName(defaultCompressionStr);
        final boolean compactionExclude = conf.getBoolean(
            "hbase.mapreduce.hfileoutputformat.compaction.exclude", false);

        return new RecordWriter<TableRowkeyPair, V>() {
          // Map of families to writers and how much has been output on the writer.
            private final Map<byte [], WriterLength> writers =
                    new TreeMap<byte [], WriterLength>(Bytes.BYTES_COMPARATOR);
            private byte [] previousRow = HConstants.EMPTY_BYTE_ARRAY;
            private final byte [] now = Bytes.toBytes(System.currentTimeMillis());
            private boolean rollRequested = false;

            @Override
            public void write(TableRowkeyPair row, V cell)
                    throws IOException {
                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                // null input == user explicitly wants to flush
                if (row == null && kv == null) {
                    rollWriters();
                    return;
                }

                // phoenix-2216: start : extract table name from the rowkey
                String tableName = row.getTableName();
                byte [] rowKey = row.getRowkey().get();
                long length = kv.getLength();
                byte [] family = CellUtil.cloneFamily(kv);
                byte[] tableAndFamily = join(tableName, Bytes.toString(family));
                WriterLength wl = this.writers.get(tableAndFamily);
                // phoenix-2216: end

                // If this is a new column family, verify that the directory exists
                if (wl == null) {
                    // phoenix-2216: start : create a directory for table and family within the output dir 
                    Path tableOutputPath = CsvBulkImportUtil.getOutputPath(outputdir, tableName);
                    fs.mkdirs(new Path(tableOutputPath, Bytes.toString(family)));
                    // phoenix-2216: end
                }

                // If any of the HFiles for the column families has reached
                // maxsize, we need to roll all the writers
                if (wl != null && wl.written + length >= maxsize) {
                    this.rollRequested = true;
                }

                // This can only happen once a row is finished though
                if (rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
                    rollWriters();
                }

                // create a new WAL writer, if necessary
                if (wl == null || wl.writer == null) {
                    // phoenix-2216: start : passed even the table name
                    wl = getNewWriter(tableName,family, conf);
                    // phoenix-2216: end
                }

                // we now have the proper WAL writer. full steam ahead
                kv.updateLatestStamp(this.now);
                wl.writer.append(kv);
                wl.written += length;
    
                // Copy the row so we know when a row transition.
                this.previousRow = rowKey;
          }

          private void rollWriters() throws IOException {
              for (WriterLength wl : this.writers.values()) {
                  if (wl.writer != null) {
                      LOG.info("Writer=" + wl.writer.getPath() +
                              ((wl.written == 0)? "": ", wrote=" + wl.written));
                      close(wl.writer);
                  }
                  wl.writer = null;
                  wl.written = 0;
              }
              this.rollRequested = false;
          }

          /* Create a new StoreFile.Writer.
           * @param family
           * @return A WriterLength, containing a new StoreFile.Writer.
           * @throws IOException
           */
          @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BX_UNBOXING_IMMEDIATELY_REBOXED",
              justification="Not important")
          private WriterLength getNewWriter(final String tableName , byte[] family, Configuration conf)
              throws IOException {
          
              WriterLength wl = new WriterLength();
              Path tableOutputPath = CsvBulkImportUtil.getOutputPath(outputdir, tableName);
              Path familydir = new Path(tableOutputPath, Bytes.toString(family));
            
              // phoenix-2216: start : fetching the configuration properties that were set to the table.
              // create a map from column family to the compression algorithm for the table.
              final Map<byte[], Algorithm> compressionMap = createFamilyCompressionMap(conf,tableName);
              final Map<byte[], BloomType> bloomTypeMap = createFamilyBloomTypeMap(conf,tableName);
              final Map<byte[], Integer> blockSizeMap = createFamilyBlockSizeMap(conf,tableName);
              // phoenix-2216: end
            
              String dataBlockEncodingStr = conf.get(DATABLOCK_ENCODING_OVERRIDE_CONF_KEY);
              final Map<byte[], DataBlockEncoding> datablockEncodingMap = createFamilyDataBlockEncodingMap(conf,tableName);
              final DataBlockEncoding overriddenEncoding;
              if (dataBlockEncodingStr != null) {
                  overriddenEncoding = DataBlockEncoding.valueOf(dataBlockEncodingStr);
              } else {
                  overriddenEncoding = null;
              }
            
              Algorithm compression = compressionMap.get(family);
              compression = compression == null ? defaultCompression : compression;
              BloomType bloomType = bloomTypeMap.get(family);
              bloomType = bloomType == null ? BloomType.NONE : bloomType;
              Integer blockSize = blockSizeMap.get(family);
              blockSize = blockSize == null ? HConstants.DEFAULT_BLOCKSIZE : blockSize;
              DataBlockEncoding encoding = overriddenEncoding;
              encoding = encoding == null ? datablockEncodingMap.get(family) : encoding;
              encoding = encoding == null ? DataBlockEncoding.NONE : encoding;
              Configuration tempConf = new Configuration(conf);
              tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
              HFileContextBuilder contextBuilder = new HFileContextBuilder()
                                        .withCompression(compression)
                                        .withChecksumType(HStore.getChecksumType(conf))
                                        .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                                        .withBlockSize(blockSize);
              contextBuilder.withDataBlockEncoding(encoding);
              HFileContext hFileContext = contextBuilder.build();
                                        
              wl.writer = new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs)
                .withOutputDir(familydir).withBloomType(bloomType)
                .withComparator(KeyValue.COMPARATOR)
                .withFileContext(hFileContext).build();

              // join and put it in the writers map .
              // phoenix-2216: start : holds a map of writers where the 
              //                       key in the map is a join byte array of table name and family.
              byte[] tableAndFamily = join(tableName, Bytes.toString(family));
              this.writers.put(tableAndFamily, wl);
              // phoenix-2216: end
              return wl;
          }

          private void close(final StoreFile.Writer w) throws IOException {
              if (w != null) {
                  w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
                          Bytes.toBytes(System.currentTimeMillis()));
                  w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
                          Bytes.toBytes(context.getTaskAttemptID().toString()));
                  w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
                          Bytes.toBytes(true));
                  w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
                          Bytes.toBytes(compactionExclude));
                  w.appendTrackedTimestampsToMetadata();
                  w.close();
              }
          }

          @Override
          public void close(TaskAttemptContext c) throws IOException, InterruptedException {
              for (WriterLength wl: this.writers.values()) {
                  close(wl.writer);
              }
          }
        };
     }
    
    /*
     * Data structure to hold a Writer and amount of data written on it.
     */
    static class WriterLength {
      long written = 0;
      StoreFile.Writer writer = null;
    }
    
    /**
     * joins the table name and the family with a delimiter.
     * @param tableName
     * @param family
     * @return
     */
    private static byte[] join(String tableName, String family) {
      return Bytes.toBytes(tableName + AT_DELIMITER + family);
    }
    
    /**
     * Runs inside the task to deserialize column family to compression algorithm
     * map from the configuration.
     *
     * @param conf to read the serialized values from
     * @return a map from column family to the configured compression algorithm
     */
    @VisibleForTesting
    static Map<byte[], Algorithm> createFamilyCompressionMap(Configuration conf,final String tableName) {
        Map<byte[], Algorithm> compressionMap = new TreeMap<byte[],Algorithm>(Bytes.BYTES_COMPARATOR);
        Map<String, String> tableConfigs = getTableConfigurations(conf, tableName);
        if(tableConfigs == null) {
            return compressionMap;
        }
        Map<byte[], String> stringMap = createFamilyConfValueMap(tableConfigs,COMPRESSION_FAMILIES_CONF_KEY);
        for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
            Algorithm algorithm = AbstractHFileWriter.compressionByName(e.getValue());
            compressionMap.put(e.getKey(), algorithm);
        }
        return compressionMap;
    }

    /**
     * Returns the set of configurations that have been configured for the table during job initialization.
     * @param conf
     * @param tableName
     * @return
     */
    private static Map<String, String> getTableConfigurations(Configuration conf, final String tableName) {
        String tableDefn = conf.get(tableName);
        if(StringUtils.isEmpty(tableDefn)) {
            return null;
        }
        TargetTableRef table = TargetTableRefFunctions.FROM_JSON.apply(tableDefn);
        Map<String,String> tableConfigs = table.getConfiguration();
        return tableConfigs;
    }

    /**
     * Runs inside the task to deserialize column family to bloom filter type
     * map from the configuration.
     *
     * @param conf to read the serialized values from
     * @return a map from column family to the the configured bloom filter type
     */
    @VisibleForTesting
    static Map<byte[], BloomType> createFamilyBloomTypeMap(Configuration conf,final String tableName) {
        Map<byte[], BloomType> bloomTypeMap = new TreeMap<byte[],BloomType>(Bytes.BYTES_COMPARATOR);
        Map<String, String> tableConfigs = getTableConfigurations(conf, tableName);
        if(tableConfigs == null) {
            return bloomTypeMap;
        }
        Map<byte[], String> stringMap = createFamilyConfValueMap(tableConfigs,BLOOM_TYPE_FAMILIES_CONF_KEY);
        for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
           BloomType bloomType = BloomType.valueOf(e.getValue());
           bloomTypeMap.put(e.getKey(), bloomType);
       }
       return bloomTypeMap;
    }

    /**
     * Runs inside the task to deserialize column family to block size
     * map from the configuration.
     *
     * @param conf to read the serialized values from
     * @return a map from column family to the configured block size
     */
    @VisibleForTesting
    static Map<byte[], Integer> createFamilyBlockSizeMap(Configuration conf,final String tableName) {
        Map<byte[], Integer> blockSizeMap = new TreeMap<byte[],Integer>(Bytes.BYTES_COMPARATOR);
        Map<String, String> tableConfigs = getTableConfigurations(conf, tableName);
        if(tableConfigs == null) {
            return blockSizeMap;
        }
        Map<byte[], String> stringMap = createFamilyConfValueMap(tableConfigs,BLOCK_SIZE_FAMILIES_CONF_KEY);
        for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
            Integer blockSize = Integer.parseInt(e.getValue());
            blockSizeMap.put(e.getKey(), blockSize);
        }
        return blockSizeMap;
    }

    /**
     * Runs inside the task to deserialize column family to data block encoding
     * type map from the configuration.
     *
     * @param conf to read the serialized values from
     * @return a map from column family to HFileDataBlockEncoder for the
     *         configured data block type for the family
     */
    @VisibleForTesting
    static Map<byte[], DataBlockEncoding> createFamilyDataBlockEncodingMap(Configuration conf,final String tableName) {
        
        Map<byte[], DataBlockEncoding> encoderMap = new TreeMap<byte[],DataBlockEncoding>(Bytes.BYTES_COMPARATOR);
        Map<String, String> tableConfigs = getTableConfigurations(conf, tableName);
        if(tableConfigs == null) {
            return encoderMap;
        }
        Map<byte[], String> stringMap = createFamilyConfValueMap(tableConfigs,DATABLOCK_ENCODING_FAMILIES_CONF_KEY);
        for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
            encoderMap.put(e.getKey(), DataBlockEncoding.valueOf((e.getValue())));
        }
        return encoderMap;
    }


    /**
     * Run inside the task to deserialize column family to given conf value map.
     *
     * @param conf to read the serialized values from
     * @param confName conf key to read from the configuration
     * @return a map of column family to the given configuration value
     */
    private static Map<byte[], String> createFamilyConfValueMap(Map<String,String> configs, String confName) {
        Map<byte[], String> confValMap = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);
        String confVal = configs.get(confName);
        if(StringUtils.isEmpty(confVal)) {
            return confValMap;
        }
        for (String familyConf : confVal.split("&")) {
            String[] familySplit = familyConf.split("=");
            if (familySplit.length != 2) {
                continue;
            }
            try {
                confValMap.put(URLDecoder.decode(familySplit[0], "UTF-8").getBytes(),
                        URLDecoder.decode(familySplit[1], "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                // will not happen with UTF-8 encoding
                throw new AssertionError(e);
            }
        }
        return confValMap;
    }

    
    /**
     * Configure <code>job</code> with a TotalOrderPartitioner, partitioning against
     * <code>splitPoints</code>. Cleans up the partitions file after job exists.
     */
    static void configurePartitioner(Job job, Set<TableRowkeyPair> tablesStartKeys)
            throws IOException {
        
        Configuration conf = job.getConfiguration();
        // create the partitions file
        Path partitionsPath = new Path(conf.get("hadoop.tmp.dir"), "partitions_" + UUID.randomUUID());
        FileSystem fs = partitionsPath.getFileSystem(conf);
        fs.makeQualified(partitionsPath);
        writePartitions(conf, partitionsPath, tablesStartKeys);
        fs.deleteOnExit(partitionsPath);

        // configure job to use it
        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(conf, partitionsPath);
    }

    private static void writePartitions(Configuration conf, Path partitionsPath,
            Set<TableRowkeyPair> tablesStartKeys) throws IOException {
        
        LOG.info("Writing partition information to " + partitionsPath);
        if (tablesStartKeys.isEmpty()) {
          throw new IllegalArgumentException("No regions passed");
        }

        // We're generating a list of split points, and we don't ever
        // have keys < the first region (which has an empty start key)
        // so we need to remove it. Otherwise we would end up with an
        // empty reducer with index 0
        TreeSet<TableRowkeyPair> sorted = new TreeSet<TableRowkeyPair>(tablesStartKeys);

        TableRowkeyPair first = sorted.first();
        if (!first.getRowkey().equals(HConstants.EMPTY_BYTE_ARRAY)) {
          throw new IllegalArgumentException(
              "First region of table should have empty start key. Instead has: "
              + Bytes.toStringBinary(first.getRowkey().get()));
        }
        sorted.remove(first);

        // Write the actual file
        FileSystem fs = partitionsPath.getFileSystem(conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(
          fs, conf, partitionsPath, TableRowkeyPair.class,
          NullWritable.class);

        try {
          for (TableRowkeyPair startKey : sorted) {
            writer.append(startKey, NullWritable.get());
          }
        } finally {
          writer.close();
        }
        
    }

    /**
     * Serialize column family to compression algorithm map to configuration.
     * Invoked while configuring the MR job for incremental load.
     *
     * @param table to read the properties from
     * @param conf to persist serialized values into
     * @throws IOException
     *           on failure to read column family descriptors
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    @VisibleForTesting
    static String configureCompression(HTableDescriptor tableDescriptor)
        throws UnsupportedEncodingException {
    
        StringBuilder compressionConfigValue = new StringBuilder();
        if(tableDescriptor == null){
            // could happen with mock table instance
            return compressionConfigValue.toString();
        }
        Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
        int i = 0;
        for (HColumnDescriptor familyDescriptor : families) {
            if (i++ > 0) {
                compressionConfigValue.append('&');
            }
            compressionConfigValue.append(URLEncoder.encode(
                    familyDescriptor.getNameAsString(), "UTF-8"));
            compressionConfigValue.append('=');
            compressionConfigValue.append(URLEncoder.encode(
                    familyDescriptor.getCompression().getName(), "UTF-8"));
        }
        return compressionConfigValue.toString();
    }

    /**
     * Serialize column family to block size map to configuration.
     * Invoked while configuring the MR job for incremental load.
     * @param tableDescriptor to read the properties from
     * @param conf to persist serialized values into
     *
     * @throws IOException
     *           on failure to read column family descriptors
     */
    @VisibleForTesting
    static String configureBlockSize(HTableDescriptor tableDescriptor)
        throws UnsupportedEncodingException {
        StringBuilder blockSizeConfigValue = new StringBuilder();
        if (tableDescriptor == null) {
            // could happen with mock table instance
            return blockSizeConfigValue.toString();
        }
        Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
        int i = 0;
        for (HColumnDescriptor familyDescriptor : families) {
            if (i++ > 0) {
                blockSizeConfigValue.append('&');
            }
            blockSizeConfigValue.append(URLEncoder.encode(
                    familyDescriptor.getNameAsString(), "UTF-8"));
            blockSizeConfigValue.append('=');
            blockSizeConfigValue.append(URLEncoder.encode(
                    String.valueOf(familyDescriptor.getBlocksize()), "UTF-8"));
        }
        return  blockSizeConfigValue.toString();
    }

    /**
     * Serialize column family to bloom type map to configuration.
     * Invoked while configuring the MR job for incremental load.
     * @param tableDescriptor to read the properties from
     * @param conf to persist serialized values into
     *
     * @throws IOException
     *           on failure to read column family descriptors
     */
    static String configureBloomType(HTableDescriptor tableDescriptor)
        throws UnsupportedEncodingException {
        
        StringBuilder bloomTypeConfigValue = new StringBuilder();
        
        if (tableDescriptor == null) {
            // could happen with mock table instance
            return bloomTypeConfigValue.toString();
        }
        Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
        int i = 0;
        for (HColumnDescriptor familyDescriptor : families) {
            if (i++ > 0) {
                bloomTypeConfigValue.append('&');
            }
            bloomTypeConfigValue.append(URLEncoder.encode(
                    familyDescriptor.getNameAsString(), "UTF-8"));
            bloomTypeConfigValue.append('=');
            String bloomType = familyDescriptor.getBloomFilterType().toString();
            if (bloomType == null) {
                bloomType = HColumnDescriptor.DEFAULT_BLOOMFILTER;
            }
            bloomTypeConfigValue.append(URLEncoder.encode(bloomType, "UTF-8"));
        }
        return bloomTypeConfigValue.toString();
     }

    /**
     * Serialize column family to data block encoding map to configuration.
     * Invoked while configuring the MR job for incremental load.
     *
     * @param table to read the properties from
     * @param conf to persist serialized values into
     * @throws IOException
     *           on failure to read column family descriptors
     */
    static String configureDataBlockEncoding(HTableDescriptor tableDescriptor) throws UnsupportedEncodingException {
      
        StringBuilder dataBlockEncodingConfigValue = new StringBuilder();
        
        if (tableDescriptor == null) {
            // could happen with mock table instance
            return dataBlockEncodingConfigValue.toString();
        }
        Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
        int i = 0;
        for (HColumnDescriptor familyDescriptor : families) {
            if (i++ > 0) {
                dataBlockEncodingConfigValue.append('&');
            }
            dataBlockEncodingConfigValue.append(
                    URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
            dataBlockEncodingConfigValue.append('=');
            DataBlockEncoding encoding = familyDescriptor.getDataBlockEncoding();
            if (encoding == null) {
                encoding = DataBlockEncoding.NONE;
            }
            dataBlockEncodingConfigValue.append(URLEncoder.encode(encoding.toString(),
                    "UTF-8"));
        }
        return dataBlockEncodingConfigValue.toString();
    }

    /**
     * Configures the job for MultiHfileOutputFormat.
     * @param job
     * @param tablesToBeLoaded
     * @throws IOException
     */
    public static void configureIncrementalLoad(Job job, List<TargetTableRef> tablesToBeLoaded) throws IOException {
        
        Configuration conf = job.getConfiguration();
        job.setOutputFormatClass(MultiHfileOutputFormat.class);
        conf.setStrings("io.serializations", conf.get("io.serializations"),
                MutationSerialization.class.getName(), ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());

        // tableStartKeys for all tables.
        Set<TableRowkeyPair> tablesStartKeys = Sets.newTreeSet();
        for(TargetTableRef table : tablesToBeLoaded) {
           final String tableName = table.getPhysicalName();
           try(HTable htable = new HTable(conf,tableName);){
               Set<TableRowkeyPair> startKeys = getRegionStartKeys(tableName , htable.getRegionLocator());
               tablesStartKeys.addAll(startKeys);
               String compressionConfig = configureCompression(htable.getTableDescriptor());
               String bloomTypeConfig = configureBloomType(htable.getTableDescriptor());
               String blockSizeConfig = configureBlockSize(htable.getTableDescriptor());
               String blockEncodingConfig = configureDataBlockEncoding(htable.getTableDescriptor());
               Map<String,String> tableConfigs = Maps.newHashMap();
               if(StringUtils.isNotBlank(compressionConfig)) {
                   tableConfigs.put(COMPRESSION_FAMILIES_CONF_KEY, compressionConfig);
               }
               if(StringUtils.isNotBlank(bloomTypeConfig)) {
                   tableConfigs.put(BLOOM_TYPE_FAMILIES_CONF_KEY,bloomTypeConfig);
               }
               if(StringUtils.isNotBlank(blockSizeConfig)) {
                   tableConfigs.put(BLOCK_SIZE_FAMILIES_CONF_KEY,blockSizeConfig);
               }
               if(StringUtils.isNotBlank(blockEncodingConfig)) {
                   tableConfigs.put(DATABLOCK_ENCODING_FAMILIES_CONF_KEY,blockEncodingConfig);
               }
               table.setConfiguration(tableConfigs);
               final String tableDefns = TargetTableRefFunctions.TO_JSON.apply(table);
               // set the table definition in the config to be used during the RecordWriter..
               conf.set(tableName, tableDefns);
               
               TargetTableRef tbl = TargetTableRefFunctions.FROM_JSON.apply(tableDefns);
               LOG.info(" the table logical name is "+ tbl.getLogicalName());
           }
       }
    
       LOG.info("Configuring " + tablesStartKeys.size() + " reduce partitions to match current region count");
       job.setNumReduceTasks(tablesStartKeys.size());

       configurePartitioner(job, tablesStartKeys);
       TableMapReduceUtil.addDependencyJars(job);
       TableMapReduceUtil.initCredentials(job);
        
    }
    
    /**
     * Return the start keys of all of the regions in this table,
     * as a list of ImmutableBytesWritable.
     */
    private static Set<TableRowkeyPair> getRegionStartKeys(String tableName , RegionLocator table) throws IOException {
      byte[][] byteKeys = table.getStartKeys();
      Set<TableRowkeyPair> ret = new TreeSet<TableRowkeyPair>();
      for (byte[] byteKey : byteKeys) {
          // phoenix-2216: start : passing the table name and startkey  
        ret.add(new TableRowkeyPair(tableName, new ImmutableBytesWritable(byteKey)));
      }
      return ret;
    }
}
