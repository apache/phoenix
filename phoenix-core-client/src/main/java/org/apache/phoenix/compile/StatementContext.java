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
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.log.QueryLogger;
import org.apache.phoenix.monitoring.OverAllQueryMetrics;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.ReadOnlyProps;


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
    private final PhoenixConnection connection;
    private final BindManager binds;
    private final Scan scan;
    private final ExpressionManager expressions;
    private final AggregationManager aggregates;
    private final String numberFormat;
    private final ImmutableBytesWritable tempPtr;
    private final PhoenixStatement statement;
    private final Map<PColumn, Integer> dataColumns;
    private Map<Long, Boolean> retryingPersistentCache;

    private long currentTime = QueryConstants.UNSET_TIMESTAMP;
    private ScanRanges scanRanges = ScanRanges.EVERYTHING;
    private final SequenceManager sequences;

    private TableRef currentTable;
    private List<Pair<byte[], byte[]>> whereConditionColumns;
    private Map<SelectStatement, Object> subqueryResults;
    private final ReadMetricQueue readMetricsQueue;
    private final OverAllQueryMetrics overAllQueryMetrics;
    private QueryLogger queryLogger;
    private boolean isClientSideUpsertSelect;
    private boolean isUncoveredIndex;
    private AtomicBoolean hasFirstValidResult;
    
    public StatementContext(PhoenixStatement statement) {
        this(statement, new Scan());
    }
    public StatementContext(StatementContext context) {
        this.resolver = context.resolver;
        this.connection = context.connection;
        this.binds = context.binds;
        this.scan = context.scan;
        this.expressions = context.expressions;
        this.aggregates = context.aggregates;
        this.numberFormat = context.numberFormat;
        this.tempPtr = context.tempPtr;
        this.statement = context.statement;
        this.dataColumns = context.dataColumns;
        this.retryingPersistentCache = context.retryingPersistentCache;
        this.currentTime = context.currentTime;
        this.scanRanges = context.scanRanges;
        this.sequences = context.sequences;
        this.currentTable = context.currentTable;
        this.whereConditionColumns = context.whereConditionColumns;
        this.subqueryResults = context.subqueryResults;
        this.readMetricsQueue = context.readMetricsQueue;
        this.overAllQueryMetrics = context.overAllQueryMetrics;
        this.queryLogger = context.queryLogger;
        this.isClientSideUpsertSelect = context.isClientSideUpsertSelect;
        this.isUncoveredIndex = context.isUncoveredIndex;
        this.hasFirstValidResult = new AtomicBoolean(context.getHasFirstValidResult());
    }
    /**
     *  Constructor that lets you override whether or not to collect request level metrics.
     */
    public StatementContext(PhoenixStatement statement, boolean collectRequestLevelMetrics) {
        this(statement, FromCompiler.EMPTY_TABLE_RESOLVER, new BindManager(statement.getParameters()), new Scan(), new SequenceManager(statement), collectRequestLevelMetrics);
    }

    public StatementContext(PhoenixStatement statement, Scan scan) {
        this(statement, FromCompiler.EMPTY_TABLE_RESOLVER, new BindManager(statement.getParameters()), scan, new SequenceManager(statement));
    }

    public StatementContext(PhoenixStatement statement, ColumnResolver resolver) {
        this(statement, resolver, new BindManager(statement.getParameters()), new Scan(), new SequenceManager(statement));
    }

    public StatementContext(PhoenixStatement statement, ColumnResolver resolver, Scan scan, SequenceManager seqManager) {
        this(statement, resolver, new BindManager(statement.getParameters()), scan, seqManager);
    }

    public StatementContext(PhoenixStatement statement, ColumnResolver resolver, BindManager binds, Scan scan, SequenceManager seqManager) {
        this(statement, resolver, binds, scan, seqManager, statement.getConnection().isRequestLevelMetricsEnabled());
    }

    public StatementContext(PhoenixStatement statement, ColumnResolver resolver, BindManager binds, Scan scan, SequenceManager seqManager, boolean isRequestMetricsEnabled) {
        this.statement = statement;
        this.resolver = resolver;
        this.scan = scan;
        this.sequences = seqManager;
        this.binds = binds;
        this.aggregates = new AggregationManager();
        this.expressions = new ExpressionManager();
        this.connection = statement.getConnection();
        ReadOnlyProps props = connection.getQueryServices().getProps();
        this.numberFormat = props.get(QueryServices.NUMBER_FORMAT_ATTRIB, NumberUtil.DEFAULT_NUMBER_FORMAT);
        this.tempPtr = new ImmutableBytesWritable();
        this.currentTable = resolver != null && !resolver.getTables().isEmpty() ? resolver.getTables().get(0) : null;
        this.whereConditionColumns = new ArrayList<Pair<byte[], byte[]>>();
        this.dataColumns = this.currentTable == null ? Collections.<PColumn, Integer> emptyMap() : Maps
                .<PColumn, Integer> newLinkedHashMap();
        this.subqueryResults = Maps.<SelectStatement, Object> newConcurrentMap();
        this.readMetricsQueue = new ReadMetricQueue(isRequestMetricsEnabled,connection.getLogLevel());
        this.overAllQueryMetrics = new OverAllQueryMetrics(isRequestMetricsEnabled,connection.getLogLevel());
        this.retryingPersistentCache = Maps.<Long, Boolean> newHashMap();
        this.hasFirstValidResult = new AtomicBoolean(false);
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

    public String getDateFormatTimeZoneId() {
        return connection.getDateFormatTimeZoneId();
    }

    public String getDateFormat() {
        return connection.getDatePattern();
    }

    public Format getDateFormatter() {
        return connection.getFormatter(PDate.INSTANCE);
    }

    public String getTimeFormat() {
        return connection.getTimePattern();
    }

    public Format getTimeFormatter() {
        return connection.getFormatter(PTime.INSTANCE);
    }

    public String getTimestampFormat() {
        return connection.getTimestampPattern();
    }

    public Format getTimestampFormatter() {
        return connection.getFormatter(PTimestamp.INSTANCE);
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

    public boolean getHasFirstValidResult() {
        return hasFirstValidResult.get();
    }

    public void setHasFirstValidResult(boolean hasValidResult) {
        hasFirstValidResult.set(hasValidResult);
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
        this.scanRanges = scanRanges;
        scanRanges.initializeScan(scan);
    }

    public PhoenixConnection getConnection() {
        return connection;
    }

    public PhoenixStatement getStatement() {
        return statement;
    }

    public long getCurrentTime() throws SQLException {
        long ts = this.getCurrentTable().getCurrentTime();
        // if the table is transactional then it is only resolved once per query, so we can't use the table timestamp
        if (this.getCurrentTable().getTable().getType() != PTableType.SUBQUERY
                && this.getCurrentTable().getTable().getType() != PTableType.PROJECTED
                && !this.getCurrentTable().getTable().isTransactional()
                && ts != QueryConstants.UNSET_TIMESTAMP) {
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
        MetaDataClient client = new MetaDataClient(connection);
        currentTime = client.getCurrentTime(table.getSchemaName().getString(), table.getTableName().getString());
        return currentTime;
    }

    public long getCurrentTimeWithDisplacement() throws SQLException {
        if (connection.isApplyTimeZoneDisplacement()) {
            return DateUtil.applyInputDisplacement(new java.sql.Date(getCurrentTime()),
                statement.getLocalCalendar().getTimeZone()).getTime();
        } else {
            return getCurrentTime();
        }
    }

    public SequenceManager getSequenceManager(){
        return sequences;
    }

    public void addWhereConditionColumn(byte[] cf, byte[] q) {
        whereConditionColumns.add(new Pair<byte[], byte[]>(cf, q));
    }

    public List<Pair<byte[], byte[]>> getWhereConditionColumns() {
        return whereConditionColumns;
    }

    public boolean isSubqueryResultAvailable(SelectStatement select) {
        return subqueryResults.containsKey(select);
    }

    public Object getSubqueryResult(SelectStatement select) {
        return subqueryResults.get(select);
    }

    public void setSubqueryResult(SelectStatement select, Object result) {
        subqueryResults.put(select, result);
    }
    
    public ReadMetricQueue getReadMetricsQueue() {
        return readMetricsQueue;
    }
    
    public OverAllQueryMetrics getOverallQueryMetrics() {
        return overAllQueryMetrics;
    }

    public void setQueryLogger(QueryLogger queryLogger) {
       this.queryLogger=queryLogger;
    }

    public QueryLogger getQueryLogger() {
        return queryLogger;
    }

    public boolean isClientSideUpsertSelect() {
        return isClientSideUpsertSelect;
    }

    public void setClientSideUpsertSelect(boolean isClientSideUpsertSelect) {
        this.isClientSideUpsertSelect = isClientSideUpsertSelect;
    }

    public boolean isUncoveredIndex() {
        return isUncoveredIndex;
    }

    public void setUncoveredIndex(boolean isUncoveredIndex) {
        this.isUncoveredIndex = isUncoveredIndex;
    }

    /*
     * setRetryingPersistentCache can be used to override the USE_PERSISTENT_CACHE hint and disable the use of the
     * persistent cache for a specific cache ID. This can be used to retry queries that failed when using the persistent
     * cache.
     */
    public void setRetryingPersistentCache(long cacheId) {
        retryingPersistentCache.put(cacheId, true);
    }

    public boolean getRetryingPersistentCache(long cacheId) {
        Boolean retrying = retryingPersistentCache.get(cacheId);
        if (retrying == null) {
            return false;
        } else {
            return retrying;
        }
    }
}
