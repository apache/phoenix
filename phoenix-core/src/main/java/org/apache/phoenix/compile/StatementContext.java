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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.join.TupleProjector;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.collect.Maps;


/**
 *
 * Class that keeps common state used across processing the various clauses in a
 * top level JDBC statement such as SELECT, UPSERT, DELETE, etc.
 *
 * 
 * @since 0.1
 */
public class StatementContext {
    private ColumnResolver resolver;
    private final BindManager binds;
    private final Scan scan;
    private final ExpressionManager expressions;
    private final AggregationManager aggregates;
    private final String dateFormat;
    private final Format dateFormatter;
    private final Format dateParser;
    private final String numberFormat;
    private final ImmutableBytesWritable tempPtr;
    private final PhoenixStatement statement;
    private final Map<PColumn, Integer> dataColumns;
    
    private long currentTime = QueryConstants.UNSET_TIMESTAMP;
    private ScanRanges scanRanges = ScanRanges.EVERYTHING;
    private KeyRange minMaxRange = null;
    private final SequenceManager sequences; 

    private TableRef currentTable;
    private List<Pair<byte[], byte[]>> whereConditionColumns;
    private TupleProjector clientTupleProjector;
    
    public StatementContext(PhoenixStatement statement) {
        this(statement, FromCompiler.EMPTY_TABLE_RESOLVER, new Scan(), new SequenceManager(statement));
    }
    
    public StatementContext(PhoenixStatement statement, ColumnResolver resolver, Scan scan, SequenceManager seqManager) {
        this.statement = statement;
        this.resolver = resolver;
        this.scan = scan;
        this.sequences = seqManager;
        this.binds = new BindManager(statement.getParameters());
        this.aggregates = new AggregationManager();
        this.expressions = new ExpressionManager();
        PhoenixConnection connection = statement.getConnection();
        this.dateFormat = connection.getQueryServices().getProps().get(QueryServices.DATE_FORMAT_ATTRIB, DateUtil.DEFAULT_DATE_FORMAT);
        this.dateFormatter = DateUtil.getDateFormatter(dateFormat);
        this.dateParser = DateUtil.getDateParser(dateFormat);
        this.numberFormat = connection.getQueryServices().getProps().get(QueryServices.NUMBER_FORMAT_ATTRIB, NumberUtil.DEFAULT_NUMBER_FORMAT);
        this.tempPtr = new ImmutableBytesWritable();
        this.currentTable = resolver != null && !resolver.getTables().isEmpty() ? resolver.getTables().get(0) : null;
        this.whereConditionColumns = new ArrayList<Pair<byte[],byte[]>>();
        this.dataColumns = this.currentTable == null ? Collections.<PColumn, Integer>emptyMap() : Maps.<PColumn, Integer>newLinkedHashMap();
    }

    /**
     * Copy constructor where an altered scan can be set.
     *
     * @param stmtContext the {@code StatementContext} to be copied
     * @param scan the customized scan
     */
    public StatementContext(StatementContext stmtContext, Scan scan) {
        this.statement = stmtContext.statement;
        this.resolver = stmtContext.resolver;
        this.scan = scan;
        this.sequences = stmtContext.sequences;
        this.binds = stmtContext.binds;
        this.aggregates = stmtContext.aggregates;
        this.expressions = stmtContext.expressions;
        this.dateFormat = stmtContext.dateFormat;
        this.dateFormatter = stmtContext.dateFormatter;
        this.dateParser = stmtContext.dateParser;
        this.numberFormat = stmtContext.numberFormat;
        this.tempPtr = new ImmutableBytesWritable();
        this.currentTable = stmtContext.currentTable;
        this.whereConditionColumns = stmtContext.whereConditionColumns;
        this.dataColumns = stmtContext.getDataColumnsMap();
    }

    /**
     * build map from dataColumn to what will be its position in single KeyValue value bytes
     * returned from the coprocessor that joins from the index row back to the data row.
     * @param column
     * @return
     */
    public int getDataColumnPosition(PColumn column) {
        Integer pos = dataColumns.get(column);
        if (pos == null) {
            pos = dataColumns.size();
            dataColumns.put(column, pos);
        }
        return pos;
    }

    /**
     * @return return set of data columns.
     */
    public Set<PColumn> getDataColumns() {
        return dataColumns.keySet();
    }

    /**
     * @return map of data columns and their positions. 
     */
    public Map<PColumn, Integer> getDataColumnsMap() {
        return dataColumns;
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
    
    public TableRef getCurrentTable() {
        return currentTable;
    }
    
    public void setCurrentTable(TableRef table) {
        this.currentTable = table;
    }

    public AggregationManager getAggregationManager() {
        return aggregates;
    }

    public ColumnResolver getResolver() {
        return resolver;
    }

    public void setResolver(ColumnResolver resolver) {
        this.resolver = resolver;
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
        PTable table = this.getCurrentTable().getTable();
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
        return statement.getConnection();
    }

    public PhoenixStatement getStatement() {
        return statement;
    }

    public long getCurrentTime() throws SQLException {
        long ts = this.getCurrentTable().getTimeStamp();
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
        PTable table = this.getCurrentTable().getTable();
        PhoenixConnection connection = getConnection();
        MetaDataClient client = new MetaDataClient(connection);
        currentTime = client.getCurrentTime(table.getSchemaName().getString(), table.getTableName().getString());
        return currentTime;
    }

    /**
     * Get the key range derived from row value constructor usage in where clause. These are orthogonal to the ScanRanges
     * and form a range for which each scan is intersected against.
     */
    public KeyRange getMinMaxRange () {
        return minMaxRange;
    }
    
    public SequenceManager getSequenceManager(){
        return sequences;
    }

    public void addWhereCoditionColumn(byte[] cf, byte[] q) {
        whereConditionColumns.add(new Pair<byte[], byte[]>(cf, q));
    }

    public List<Pair<byte[], byte[]>> getWhereCoditionColumns() {
        return whereConditionColumns;
    }
    
    public TupleProjector getClientTupleProjector() {
        return clientTupleProjector;
    }
    
    public void setClientTupleProjector(TupleProjector projector) {
        this.clientTupleProjector = projector;
    }
}
