/*
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.text.Format;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.BindableStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.ScanUtil;


/**
 *
 * Class that keeps common state used across processing the various clauses in a
 * top level JDBC statement such as SELECT, UPSERT, DELETE, etc.
 *
 * @author jtaylor
 * @since 0.1
 */
public class StatementContext {
    private final ColumnResolver resolver;
    private final BindManager binds;
    private final Scan scan;
    private final ExpressionManager expressions;
    private final AggregationManager aggregates;
    private final String dateFormat;
    private final Format dateFormatter;
    private final Format dateParser;
    private final String numberFormat;
    private final ImmutableBytesWritable tempPtr;
    private final PhoenixConnection connection;
    
    private long currentTime = QueryConstants.UNSET_TIMESTAMP;
    private ScanRanges scanRanges = ScanRanges.EVERYTHING;
    private KeyRange minMaxRange = null;

    public StatementContext(BindableStatement statement, PhoenixConnection connection, ColumnResolver resolver, List<Object> binds, Scan scan) {
        this.connection = connection;
        this.resolver = resolver;
        this.scan = scan;
        this.binds = new BindManager(binds, statement.getBindCount());
        this.aggregates = new AggregationManager();
        this.expressions = new ExpressionManager();
        this.dateFormat = connection.getQueryServices().getProps().get(QueryServices.DATE_FORMAT_ATTRIB, DateUtil.DEFAULT_DATE_FORMAT);
        this.dateFormatter = DateUtil.getDateFormatter(dateFormat);
        this.dateParser = DateUtil.getDateParser(dateFormat);
        this.numberFormat = connection.getQueryServices().getProps().get(QueryServices.NUMBER_FORMAT_ATTRIB, NumberUtil.DEFAULT_NUMBER_FORMAT);
        this.tempPtr = new ImmutableBytesWritable();
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public Format getDateFormatter() {
        return dateFormatter;
    }

    public Format getDateParser() {
        return dateParser;
    }
    
    public String getNumberFormat() {
        return numberFormat;
    }
    
    public Scan getScan() {
        return scan;
    }

    public BindManager getBindManager() {
        return binds;
    }

    public AggregationManager getAggregationManager() {
        return aggregates;
    }

    public ColumnResolver getResolver() {
        return resolver;
    }

    public ExpressionManager getExpressionManager() {
        return expressions;
    }


    public ImmutableBytesWritable getTempPtr() {
        return tempPtr;
    }

    public ScanRanges getScanRanges() {
        return this.scanRanges;
    }
    
    public void setScanRanges(ScanRanges scanRanges) {
        setScanRanges(scanRanges, null);
    }

    public void setScanRanges(ScanRanges scanRanges, KeyRange minMaxRange) {
        this.scanRanges = scanRanges;
        this.scanRanges.setScanStartStopRow(scan);
        PTable table = this.getResolver().getTables().get(0).getTable();
        if (minMaxRange != null) {
            // Ensure minMaxRange is lower inclusive and upper exclusive, as that's
            // what we need to intersect against for the HBase scan.
            byte[] lowerRange = minMaxRange.getLowerRange();
            if (!minMaxRange.lowerUnbound()) {
                if (!minMaxRange.isLowerInclusive()) {
                    lowerRange = ScanUtil.nextKey(lowerRange, table, tempPtr);
                }
            }
            
            byte[] upperRange = minMaxRange.getUpperRange();
            if (!minMaxRange.upperUnbound()) {
                if (minMaxRange.isUpperInclusive()) {
                    upperRange = ScanUtil.nextKey(upperRange, table, tempPtr);
                }
            }
            if (minMaxRange.getLowerRange() != lowerRange || minMaxRange.getUpperRange() != upperRange) {
                minMaxRange = KeyRange.getKeyRange(lowerRange, true, upperRange, false);
            }
            // If we're not salting, we can intersect this now with the scan range.
            // Otherwise, we have to wait to do this when we chunk up the scan.
            if (table.getBucketNum() == null) {
                minMaxRange = minMaxRange.intersect(KeyRange.getKeyRange(scan.getStartRow(), scan.getStopRow()));
                scan.setStartRow(minMaxRange.getLowerRange());
                scan.setStopRow(minMaxRange.getUpperRange());
            }
            this.minMaxRange = minMaxRange;
        }
    }
    
    public PhoenixConnection getConnection() {
        return connection;
    }

    public long getCurrentTime() throws SQLException {
        long ts = this.getResolver().getTables().get(0).getTimeStamp();
        if (ts != QueryConstants.UNSET_TIMESTAMP) {
            return ts;
        }
        if (currentTime != QueryConstants.UNSET_TIMESTAMP) {
            return currentTime;
        }
        /*
         * For an UPSERT VALUES where autocommit off, we won't hit the server until the commit.
         * However, if the statement has a CURRENT_DATE() call as a value, we need to know the
         * current time at execution time. In that case, we'll call MetaDataClient.updateCache
         * purely to bind the current time based on the server time.
         */
        PTable table = this.getResolver().getTables().get(0).getTable();
        MetaDataClient client = new MetaDataClient(connection);
        currentTime = Math.abs(client.updateCache(table.getSchemaName().getString(), table.getTableName().getString()));
        return currentTime;
    }

    /**
     * Get the key range derived from row value constructor usage in where clause. These are orthogonal to the ScanRanges
     * and form a range for which each scan is intersected against.
     */
    public KeyRange getMinMaxRange () {
        return minMaxRange;
    }
    
    public boolean isSingleRowScan() {
        return this.getScanRanges().isSingleRowScan() && ! (this.getScan().getFilter() instanceof FilterList);
    }
}
