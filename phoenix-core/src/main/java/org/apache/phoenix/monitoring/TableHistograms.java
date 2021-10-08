/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import java.util.List;
import org.apache.hadoop.conf.Configuration;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

public class TableHistograms {
    private String tableName;
    private LatencyHistogram queryLatencyHisto;
    private SizeHistogram querySizeHisto;
    private LatencyHistogram upsertLatencyHisto;
    private SizeHistogram upsertSizeHisto;
    private LatencyHistogram deleteLatencyHisto;
    private SizeHistogram deleteSizeHisto;
    private LatencyHistogram pointLookupLatencyHisto;
    private SizeHistogram pointLookupSizeHisto;
    private LatencyHistogram rangeScanLatencyHisto;
    private SizeHistogram rangeScanSizeHisto;

    public TableHistograms(String tableName, Configuration conf) {
        this.tableName = tableName;
        queryLatencyHisto = new LatencyHistogram("QueryTime", "Query time latency", conf);
        querySizeHisto = new SizeHistogram("QuerySize", "Query size", conf);

        upsertLatencyHisto = new LatencyHistogram("UpsertTime", "Upsert time latency", conf);
        upsertSizeHisto = new SizeHistogram("UpsertSize", "Upsert size", conf);

        deleteLatencyHisto = new LatencyHistogram("DeleteTime", "Delete time latency", conf);
        deleteSizeHisto = new SizeHistogram("DeleteSize", "Delete size", conf);

        pointLookupLatencyHisto = new LatencyHistogram("PointLookupTime",
                "Point Lookup Query time latency", conf);
        pointLookupSizeHisto = new SizeHistogram("PointLookupSize",
                "Point Lookup Query Size", conf);

        rangeScanLatencyHisto = new LatencyHistogram("RangeScanTime",
                "Range Scan Query time latency", conf);
        rangeScanSizeHisto = new SizeHistogram("RangeScanSize",
                "Range Scan Query size", conf);
    }

    public String getTableName() {
        return tableName;
    }

    public LatencyHistogram getQueryLatencyHisto() {
        return queryLatencyHisto;
    }

    public SizeHistogram getQuerySizeHisto() {
        return querySizeHisto;
    }


    public LatencyHistogram getPointLookupLatencyHisto() {
        return pointLookupLatencyHisto;
    }

    public SizeHistogram getPointLookupSizeHisto() {
        return pointLookupSizeHisto;
    }

    public LatencyHistogram getRangeScanLatencyHisto() {
        return rangeScanLatencyHisto;
    }

    public SizeHistogram getRangeScanSizeHisto() {
        return rangeScanSizeHisto;
    }

    public LatencyHistogram getUpsertLatencyHisto() {
        return upsertLatencyHisto;
    }

    public SizeHistogram getUpsertSizeHisto() {
        return upsertSizeHisto;
    }

    public LatencyHistogram getDeleteLatencyHisto() {
        return deleteLatencyHisto;
    }

    public SizeHistogram getDeleteSizeHisto() {
        return deleteSizeHisto;
    }

    public List<HistogramDistribution> getTableLatencyHistogramsDistribution() {
        return ImmutableList.of(queryLatencyHisto.getRangeHistogramDistribution(),
                upsertLatencyHisto.getRangeHistogramDistribution(),
                deleteLatencyHisto.getRangeHistogramDistribution(),
                pointLookupLatencyHisto.getRangeHistogramDistribution(),
                rangeScanLatencyHisto.getRangeHistogramDistribution());
    }

    public List<HistogramDistribution> getTableSizeHistogramsDistribution() {
        return ImmutableList.of(querySizeHisto.getRangeHistogramDistribution(),
                upsertSizeHisto.getRangeHistogramDistribution(),
                deleteSizeHisto.getRangeHistogramDistribution(),
                pointLookupSizeHisto.getRangeHistogramDistribution(),
                rangeScanSizeHisto.getRangeHistogramDistribution());
    }

}