/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.index.IndexScrutinyMapper;
import org.apache.phoenix.mapreduce.index.IndexScrutinyMapperForTest;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.OutputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link IndexScrutinyTool}
 *
 * Note that this class is never run directly by Junit/Maven, as it has no @Tests, but
 * cannot be made abstract
 */
public class IndexScrutinyToolBaseIT extends BaseTest {
    protected String outputDir;

    protected static String indexRegionObserverEnabled = Boolean.FALSE.toString();
    private static String previousIndexRegionObserverEnabled = indexRegionObserverEnabled;

    @BeforeClass public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMap();
        //disable major compactions
        serverProps.put(HConstants.MAJOR_COMPACTION_PERIOD, "0");
        Map<String, String> clientProps = Maps.newHashMap();

        clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, indexRegionObserverEnabled);

        if (!previousIndexRegionObserverEnabled.equals(indexRegionObserverEnabled)) {
           driver = null;
        }
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
        previousIndexRegionObserverEnabled = indexRegionObserverEnabled;
    }

    protected List<Job> runScrutiny(Class<? extends IndexScrutinyMapper> mapperClass,
                                    String[] cmdArgs) throws Exception {
        IndexScrutinyTool scrutiny = new IndexScrutinyTool(mapperClass);
        Configuration conf = new Configuration(getUtility().getConfiguration());
        scrutiny.setConf(conf);
        int status = scrutiny.run(cmdArgs);
        assertEquals(0, status);
        for (Job job : scrutiny.getJobs()) {
            assertTrue(job.waitForCompletion(true));
        }
        return scrutiny.getJobs();
    }

    public String[] getArgValues(String schemaName, String dataTable, String indxTable, Long batchSize,
            SourceTable sourceTable, boolean outputInvalidRows, OutputFormat outputFormat, Long maxOutputRows, String tenantId, Long scrutinyTs) {
        final List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("-s");
            args.add(schemaName);
        }
        args.add("-dt");
        args.add(dataTable);
        args.add("-it");
        args.add(indxTable);

        // TODO test snapshot reads
        // if(useSnapshot) {
        // args.add("-snap");
        // }

        if (OutputFormat.FILE.equals(outputFormat)) {
            args.add("-op");
            outputDir = "/tmp/" + UUID.randomUUID().toString();
            args.add(outputDir);
        }

        args.add("-t");
        args.add(String.valueOf(scrutinyTs));
        args.add("-run-foreground");
        if (batchSize != null) {
            args.add("-b");
            args.add(String.valueOf(batchSize));
        }

        // default to using data table as the source table
        args.add("-src");
        if (sourceTable == null) {
            args.add(SourceTable.DATA_TABLE_SOURCE.name());
        } else {
            args.add(sourceTable.name());
        }
        if (outputInvalidRows) {
            args.add("-o");
        }
        if (outputFormat != null) {
            args.add("-of");
            args.add(outputFormat.name());
        }
        if (maxOutputRows != null) {
            args.add("-om");
            args.add(maxOutputRows.toString());
        }
        if (tenantId != null) {
            args.add("-tenant");
            args.add(tenantId);
        }
        return args.toArray(new String[0]);
    }

    public static List<Job> runScrutinyTool(String schemaName, String dataTableName, String indexTableName,
            Long batchSize, SourceTable sourceTable) throws Exception {
        IndexScrutinyToolBaseIT it = new IndexScrutinyToolBaseIT();
        final String[]
                cmdArgs =
                it.getArgValues(schemaName, dataTableName, indexTableName, batchSize, sourceTable,
                        false, null, null, null, Long.MAX_VALUE);
        return it.runScrutiny(IndexScrutinyMapperForTest.class, cmdArgs);
    }

    protected long getCounterValue(Counters counters, Enum<PhoenixScrutinyJobCounters> counter) {
        return counters.findCounter(counter).getValue();
    }

    protected int countRows(Connection conn, String tableFullName) throws SQLException {
        ResultSet count = conn.createStatement().executeQuery("select count(*) from " + tableFullName);
        count.next();
        int numRows = count.getInt(1);
        return numRows;
    }

}


