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

import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.BindTableNode;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DMLStatement;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SingleTableStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TableNodeVisitor;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.parse.UDFParseNode;
import org.apache.phoenix.parse.UseSchemaStatement;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.AmbiguousTableException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PColumnFamilyImpl;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

/**
 * Validates FROM clause and builds a ColumnResolver for resolving column references
 *
 *
 * @since 0.1
 */
public class FromCompiler {
    private static final Logger logger = LoggerFactory.getLogger(FromCompiler.class);

    public static final ColumnResolver EMPTY_TABLE_RESOLVER = new ColumnResolver() {

        @Override
        public List<TableRef> getTables() {
            return Collections.singletonList(TableRef.EMPTY_TABLE_REF);
        }

        @Override
        public List<PFunction> getFunctions() {
            return Collections.emptyList();
        }

        @Override
        public TableRef resolveTable(String schemaName, String tableName)
                throws SQLException {
            throw new TableNotFoundException(schemaName, tableName);
        }

        @Override
        public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException {
            throw new ColumnNotFoundException(schemaName, tableName, null, colName);
        }
        
        @Override
        public PFunction resolveFunction(String functionName) throws SQLException {
            throw new FunctionNotFoundException(functionName);
        }

        @Override
        public boolean hasUDFs() {
            return false;
        }

        @Override
        public PSchema resolveSchema(String schemaName) throws SQLException {
            throw new SchemaNotFoundException(schemaName);
        }

        @Override
        public List<PSchema> getSchemas() {
            return Collections.emptyList();
        }

    };

    public static ColumnResolver getResolverForCreation(final CreateTableStatement statement, final PhoenixConnection connection)
            throws SQLException {
    	
        TableName baseTable = statement.getBaseTableName();
        String schemaName;
        if (baseTable == null) {
            if (SchemaUtil.isSchemaCheckRequired(statement.getTableType(), connection.getQueryServices().getProps())) {
                schemaName = statement.getTableName().getSchemaName();
                if (schemaName != null) {
                    new SchemaResolver(connection, statement.getTableName().getSchemaName(), true);
                } else if (connection.getSchema() != null) {
                    // To ensure schema set through properties or connection string exists before creating table
                    new SchemaResolver(connection, connection.getSchema(), true);
                }
            }
            return EMPTY_TABLE_RESOLVER;
        }
        NamedTableNode tableNode = NamedTableNode.create(null, baseTable, Collections.<ColumnDef>emptyList());
        // Always use non-tenant-specific connection here
        try {
            SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, tableNode, true);
            return visitor;
        } catch (TableNotFoundException e) {
            // Used for mapped VIEW, since we won't be able to resolve that.
            // Instead, we create a table with just the dynamic columns.
            // A tenant-specific connection may not create a mapped VIEW.
            if (connection.getTenantId() == null && statement.getTableType() == PTableType.VIEW) {
                ConnectionQueryServices services = connection.getQueryServices();
                byte[] fullTableName = SchemaUtil.getPhysicalName(
                        SchemaUtil.getTableNameAsBytes(baseTable.getSchemaName(), baseTable.getTableName()),
                        connection.getQueryServices().getProps()).getName();
                HTableInterface htable = null;
                try {
                    htable = services.getTable(fullTableName);
                } catch (UnsupportedOperationException ignore) {
                    throw e; // For Connectionless
                } finally {
                    if (htable != null) Closeables.closeQuietly(htable);
                }
                tableNode = NamedTableNode.create(null, baseTable, statement.getColumnDefs());
                return new SingleTableColumnResolver(connection, tableNode, e.getTimeStamp(), new HashMap<String, UDFParseNode>(1), false);
            }
            throw e;
        }
    }

    /**
     * Iterate through the nodes in the FROM clause to build a column resolver used to lookup a column given the name
     * and alias.
     *
     * @param statement
     *            the select statement
     * @return the column resolver
     * @throws SQLException
     * @throws SQLFeatureNotSupportedException
     *             if unsupported constructs appear in the FROM clause
     * @throws TableNotFoundException
     *             if table name not found in schema
     */
    public static ColumnResolver getResolverForQuery(SelectStatement statement, PhoenixConnection connection)
    		throws SQLException {
    	TableNode fromNode = statement.getFrom();
    	if (fromNode == null)
    	    return EMPTY_TABLE_RESOLVER;
        if (fromNode instanceof NamedTableNode)
            return new SingleTableColumnResolver(connection, (NamedTableNode) fromNode, true, 1, statement.getUdfParseNodes());

        MultiTableColumnResolver visitor = new MultiTableColumnResolver(connection, 1, statement.getUdfParseNodes());
        fromNode.accept(visitor);
        return visitor;
    }

    public static ColumnResolver getResolverForSchema(UseSchemaStatement statement, PhoenixConnection connection)
            throws SQLException {
        return new SchemaResolver(connection, SchemaUtil.normalizeIdentifier(statement.getSchemaName()), true);
    }

    public static ColumnResolver getResolverForSchema(String schema, PhoenixConnection connection) throws SQLException {
        return new SchemaResolver(connection, SchemaUtil.normalizeIdentifier(schema), true);
    }

    public static ColumnResolver getResolver(NamedTableNode tableNode, PhoenixConnection connection) throws SQLException {
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, tableNode, true);
        return visitor;
    }

    public static ColumnResolver getResolver(NamedTableNode tableNode, PhoenixConnection connection, Map<String, UDFParseNode> udfParseNodes) throws SQLException {
        SingleTableColumnResolver visitor =
                new SingleTableColumnResolver(connection, tableNode, true, 0, udfParseNodes);
        return visitor;
    }

    public static ColumnResolver getResolver(SingleTableStatement statement, PhoenixConnection connection)
            throws SQLException {
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, statement.getTable(), true);
        return visitor;
    }

    public static ColumnResolver getResolver(SingleTableStatement statement, PhoenixConnection connection, Map<String, UDFParseNode> udfParseNodes)
            throws SQLException {
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, statement.getTable(), true, 0, udfParseNodes);
        return visitor;
    }

    public static ColumnResolver getResolverForCompiledDerivedTable(PhoenixConnection connection, TableRef tableRef, RowProjector projector)
            throws SQLException {
        List<PColumn> projectedColumns = new ArrayList<PColumn>();
        PTable table = tableRef.getTable();
        for (PColumn column : table.getColumns()) {
            Expression sourceExpression = projector.getColumnProjector(column.getPosition()).getExpression();
            PColumnImpl projectedColumn = new PColumnImpl(column.getName(), column.getFamilyName(),
                    sourceExpression.getDataType(), sourceExpression.getMaxLength(), sourceExpression.getScale(), sourceExpression.isNullable(),
                    column.getPosition(), sourceExpression.getSortOrder(), column.getArraySize(), column.getViewConstant(), column.isViewReferenced(), column.getExpressionStr(), column.isRowTimestamp(), column.isDynamic(), column.getColumnQualifierBytes());
            projectedColumns.add(projectedColumn);
        }
        PTable t = PTableImpl.makePTable(table, projectedColumns);
        return new SingleTableColumnResolver(connection, new TableRef(tableRef.getTableAlias(), t, tableRef.getLowerBoundTimeStamp(), tableRef.hasDynamicCols()));
    }
    
    public static ColumnResolver getResolver(TableRef tableRef)
            throws SQLException {
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(tableRef);
        return visitor;
    }

    public static ColumnResolver getResolver(PhoenixConnection connection, TableRef tableRef, Map<String, UDFParseNode> udfParseNodes)
            throws SQLException {
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, tableRef, udfParseNodes);
        return visitor;
    }

    public static ColumnResolver getResolverForMutation(DMLStatement statement, PhoenixConnection connection)
            throws SQLException {
        /*
         * We validate the meta data at commit time for mutations, as this allows us to do many UPSERT VALUES calls
         * without hitting the server each time to check if the meta data is up-to-date.
         */
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, statement.getTable(), false, 0,  statement.getUdfParseNodes());
        return visitor;
    }
    
    public static ColumnResolver getResolverForProjectedTable(PTable projectedTable, PhoenixConnection connection, Map<String, UDFParseNode> udfParseNodes) throws SQLException {
        return new ProjectedTableColumnResolver(projectedTable, connection, udfParseNodes);
    }

    private static class SchemaResolver extends BaseColumnResolver {
        private final List<PSchema> schemas;

        public SchemaResolver(PhoenixConnection conn, String schemaName, boolean updateCacheImmediately)
                throws SQLException {
            super(conn, 0);
            schemaName = connection.getSchema() != null && schemaName == null ? connection.getSchema() : schemaName;
            schemas = ImmutableList.of(createSchemaRef(schemaName, updateCacheImmediately));
        }

        @Override
        public List<TableRef> getTables() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableRef resolveTable(String schemaName, String tableName) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PSchema resolveSchema(String schemaName) throws SQLException {
            return schemas.get(0);
        }

        @Override
        public List<PSchema> getSchemas() {
            return schemas;
        }

    }

    private static class SingleTableColumnResolver extends BaseColumnResolver {
    	private final List<TableRef> tableRefs;
    	private final String alias;
        private final List<PSchema> schemas;

        public SingleTableColumnResolver(PhoenixConnection connection, NamedTableNode table, long timeStamp, Map<String, UDFParseNode> udfParseNodes, boolean isNamespaceMapped) throws SQLException {
            super(connection, 0, false, udfParseNodes);
            List<PColumnFamily> families = Lists.newArrayListWithExpectedSize(table.getDynamicColumns().size());
            for (ColumnDef def : table.getDynamicColumns()) {
                if (def.getColumnDefName().getFamilyName() != null) {
                    families.add(new PColumnFamilyImpl(PNameFactory.newName(def.getColumnDefName().getFamilyName()),Collections.<PColumn>emptyList()));//, NON_ENCODED_QUALIFIERS));
                }
            }
            Long scn = connection.getSCN();
            String schema = table.getName().getSchemaName();
            if (connection.getSchema() != null) {
                schema = schema != null ? schema : connection.getSchema();
            }
            // Storage scheme and encoding scheme don't matter here since the PTable is being used only for the purposes of create table.
            // The actual values of these two will be determined by the metadata client.
            PTable theTable = new PTableImpl(connection.getTenantId(), schema, table.getName().getTableName(),
                    scn == null ? HConstants.LATEST_TIMESTAMP : scn, families, isNamespaceMapped);
            theTable = this.addDynamicColumns(table.getDynamicColumns(), theTable);
            alias = null;
            tableRefs = ImmutableList.of(new TableRef(alias, theTable, timeStamp, !table.getDynamicColumns().isEmpty()));
            schemas = ImmutableList.of(new PSchema(theTable.getSchemaName().toString(), timeStamp));
        }

        public SingleTableColumnResolver(PhoenixConnection connection, NamedTableNode tableNode, boolean updateCacheImmediately) throws SQLException {
            this(connection, tableNode, updateCacheImmediately, 0, new HashMap<String,UDFParseNode>(1));
        }

        public SingleTableColumnResolver(PhoenixConnection connection, NamedTableNode tableNode,
                boolean updateCacheImmediately, int tsAddition,
                Map<String, UDFParseNode> udfParseNodes) throws SQLException {
            super(connection, tsAddition, updateCacheImmediately, udfParseNodes);
            alias = tableNode.getAlias();
            TableRef tableRef = createTableRef(tableNode.getName().getSchemaName(), tableNode, updateCacheImmediately);
			PSchema schema = new PSchema(tableRef.getTable().getSchemaName().toString());
            tableRefs = ImmutableList.of(tableRef);
            schemas = ImmutableList.of(schema);
        }

        public SingleTableColumnResolver(PhoenixConnection connection, TableRef tableRef) {
            super(connection, 0);
            alias = tableRef.getTableAlias();
            tableRefs = ImmutableList.of(tableRef);
            schemas = ImmutableList.of(new PSchema(tableRef.getTable().getSchemaName().toString()));
        }

        public SingleTableColumnResolver(PhoenixConnection connection, TableRef tableRef, Map<String, UDFParseNode> udfParseNodes) throws SQLException {
            super(connection, 0, false, udfParseNodes);
            alias = tableRef.getTableAlias();
            tableRefs = ImmutableList.of(tableRef);
            schemas = ImmutableList.of(new PSchema(tableRef.getTable().getSchemaName().toString()));
        }

        public SingleTableColumnResolver(TableRef tableRef) throws SQLException {
            super(null, 0);
            alias = tableRef.getTableAlias();
            tableRefs = ImmutableList.of(tableRef);
            schemas = ImmutableList.of(new PSchema(tableRef.getTable().getSchemaName().toString()));
        }

		

		@Override
		public List<TableRef> getTables() {
			return tableRefs;
		}

        @Override
        public List<PFunction> getFunctions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableRef resolveTable(String schemaName, String tableName)
                throws SQLException {
            TableRef tableRef = tableRefs.get(0);
            /*
             * The only case we can definitely verify is when both a schemaName and a tableName
             * are provided. Otherwise, the tableName might be a column family. In this case,
             * this will be validated by resolveColumn.
             */
            if (schemaName != null || tableName != null) {
                String resolvedTableName = tableRef.getTable().getTableName().getString();
                String resolvedSchemaName = tableRef.getTable().getSchemaName().getString();
                if (schemaName != null && tableName != null) {
                    if ( ! ( schemaName.equals(resolvedSchemaName)  &&
                             tableName.equals(resolvedTableName) )  &&
                             ! schemaName.equals(alias) ) {
                        throw new TableNotFoundException(schemaName, tableName);
                    }
                }
            }
            return tableRef;
        }

		@Override
		public ColumnRef resolveColumn(String schemaName, String tableName,
				String colName) throws SQLException {
			TableRef tableRef = tableRefs.get(0);
			boolean resolveCF = false;
			if (schemaName != null || tableName != null) {
			    String resolvedTableName = tableRef.getTable().getTableName().getString();
			    String resolvedSchemaName = tableRef.getTable().getSchemaName().getString();
			    if (schemaName != null && tableName != null) {
                    if ( ! ( schemaName.equals(resolvedSchemaName)  &&
                             tableName.equals(resolvedTableName) )) {
                        if (!(resolveCF = schemaName.equals(alias))) {
                            throw new ColumnNotFoundException(schemaName, tableName, null, colName);
                        }
                    }
			    } else { // schemaName == null && tableName != null
                    if (tableName != null && !tableName.equals(alias) && (!tableName.equals(resolvedTableName) || !resolvedSchemaName.equals(""))) {
                        resolveCF = true;
                   }
			    }

			}
        	PColumn column = resolveCF
        	        ? tableRef.getTable().getColumnFamily(tableName).getPColumnForColumnName(colName)
        			: tableRef.getTable().getColumnForColumnName(colName);
            return new ColumnRef(tableRef, column.getPosition());
		}

        @Override
        public PSchema resolveSchema(String schemaName) throws SQLException {
            return schemas.get(0);
        }

        @Override
        public List<PSchema> getSchemas() {
            return schemas;
        }
    }

    private static abstract class BaseColumnResolver implements ColumnResolver {
        protected final PhoenixConnection connection;
        protected final MetaDataClient client;
        // Fudge factor to add to current time we calculate. We need this when we do a SELECT
        // on Windows because the millis timestamp granularity is so bad we sometimes won't
        // get the data back that we just upsert.
        private final int tsAddition;
        protected final Map<String, PFunction> functionMap;
        protected List<PFunction> functions;

        private BaseColumnResolver(PhoenixConnection connection, int tsAddition) {
            this.connection = connection;
            this.client = connection == null ? null : new MetaDataClient(connection);
            this.tsAddition = tsAddition;
            functionMap = new HashMap<String, PFunction>(1);
            this.functions = Collections.<PFunction>emptyList();
        }

        private BaseColumnResolver(PhoenixConnection connection, int tsAddition, boolean updateCacheImmediately, Map<String, UDFParseNode> udfParseNodes) throws SQLException {
        	this.connection = connection;
            this.client = connection == null ? null : new MetaDataClient(connection);
            this.tsAddition = tsAddition;
            functionMap = new HashMap<String, PFunction>(1);
            if (udfParseNodes.isEmpty()) {
                functions = Collections.<PFunction> emptyList();
            } else {
                functions = createFunctionRef(new ArrayList<String>(udfParseNodes.keySet()), updateCacheImmediately);
                for (PFunction function : functions) {
                    functionMap.put(function.getFunctionName(), function);
                }
            }
        }

        protected PSchema createSchemaRef(String schemaName, boolean updateCacheImmediately) throws SQLException {
            long timeStamp = QueryConstants.UNSET_TIMESTAMP;
            PSchema theSchema = null;
            MetaDataClient client = new MetaDataClient(connection);
            if (updateCacheImmediately) {
                MetaDataMutationResult result = client.updateCache(schemaName, true);
                timeStamp = TransactionUtil.getResolvedTimestamp(connection, result);
                theSchema = result.getSchema();
                if (theSchema == null) { throw new SchemaNotFoundException(schemaName, timeStamp); }
            } else {
                try {
                    theSchema = connection.getSchema(new PTableKey(null, schemaName));
                } catch (SchemaNotFoundException e1) {}
                // We always attempt to update the cache in the event of a
                // SchemaNotFoundException
                if (theSchema == null) {
                    MetaDataMutationResult result = client.updateCache(schemaName, true);
                    if (result.wasUpdated()) {
                        timeStamp = TransactionUtil.getResolvedTimestamp(connection, result);
                        theSchema = result.getSchema();
                    }
                }
                if (theSchema == null) { throw new SchemaNotFoundException(schemaName, timeStamp); }
            }
            return theSchema;
        }

        protected TableRef createTableRef(String connectionSchemaName, NamedTableNode tableNode, boolean updateCacheImmediately) throws SQLException {
            String tableName = tableNode.getName().getTableName();
            String schemaName = tableNode.getName().getSchemaName();
            schemaName = connection.getSchema() != null && schemaName == null ? connection.getSchema() : schemaName;
            long timeStamp = QueryConstants.UNSET_TIMESTAMP;
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            PName tenantId = connection.getTenantId();
            PTable theTable = null;
            if (updateCacheImmediately) {
                MetaDataMutationResult result = client.updateCache(schemaName, tableName);
                timeStamp = TransactionUtil.getResolvedTimestamp(connection, result);
                theTable = result.getTable();
                if (theTable == null) {
                    throw new TableNotFoundException(schemaName, tableName, timeStamp);
                }
            } else {
                try {
                    theTable = connection.getTable(new PTableKey(tenantId, fullTableName));
                } catch (TableNotFoundException e1) {
                    if (tenantId != null) { // Check with null tenantId next
                        try {
                            theTable = connection.getTable(new PTableKey(null, fullTableName));
                        } catch (TableNotFoundException e2) {
                        }
                    }
                }
                // We always attempt to update the cache in the event of a TableNotFoundException
                if (theTable == null) {
                    MetaDataMutationResult result = client.updateCache(schemaName, tableName);
                    if (result.wasUpdated()) {
                    	timeStamp = TransactionUtil.getResolvedTimestamp(connection, result);
                        theTable = result.getTable();
                    }
                }
                if (theTable == null) {
                    throw new TableNotFoundException(schemaName, tableName, timeStamp);
                }
            }
            // Add any dynamic columns to the table declaration
            List<ColumnDef> dynamicColumns = tableNode.getDynamicColumns();
            theTable = addDynamicColumns(dynamicColumns, theTable);
            if (timeStamp != QueryConstants.UNSET_TIMESTAMP) {
                timeStamp += tsAddition;
            }
            TableRef tableRef = new TableRef(tableNode.getAlias(), theTable, timeStamp, !dynamicColumns.isEmpty());
            if (logger.isDebugEnabled() && timeStamp != QueryConstants.UNSET_TIMESTAMP) {
                logger.debug(LogUtil.addCustomAnnotations("Re-resolved stale table " + fullTableName + " with seqNum " + tableRef.getTable().getSequenceNumber() + " at timestamp " + tableRef.getTable().getTimeStamp() + " with " + tableRef.getTable().getColumns().size() + " columns: " + tableRef.getTable().getColumns(), connection));
            }
            return tableRef;
        }

        @Override
        public List<PFunction> getFunctions() {
            return functions;
        }

        private List<PFunction> createFunctionRef(List<String> functionNames, boolean updateCacheImmediately) throws SQLException {
            long timeStamp = QueryConstants.UNSET_TIMESTAMP;
            int numFunctions = functionNames.size();
            List<PFunction> functionsFound = new ArrayList<PFunction>(functionNames.size());
            if (updateCacheImmediately || connection.getAutoCommit()) {
                getFunctionFromCache(functionNames, functionsFound, true);
                if(functionNames.isEmpty()) {
                    return functionsFound;
                }
                MetaDataMutationResult result = client.updateCache(functionNames);
                timeStamp = result.getMutationTime();
                functionsFound = result.getFunctions();
                if(functionNames.size() != functionsFound.size()){
                    throw new FunctionNotFoundException("Some of the functions in "+functionNames.toString()+" are not found");
                }
            } else {
                getFunctionFromCache(functionNames, functionsFound, false);
                // We always attempt to update the cache in the event of a FunctionNotFoundException
                MetaDataMutationResult result = null;
                if (!functionNames.isEmpty()) {
                    result = client.updateCache(functionNames);
                }
                if(result!=null) {
                    if (!result.getFunctions().isEmpty()) {
                        functionsFound.addAll(result.getFunctions());
                    }
                    if(result.wasUpdated()) {
                        timeStamp = result.getMutationTime();
                    }
                }
                if (functionsFound.size()!=numFunctions) {
                    throw new FunctionNotFoundException("Some of the functions in "+functionNames.toString()+" are not found", timeStamp);
                }
            }
            if (timeStamp != QueryConstants.UNSET_TIMESTAMP) {
                timeStamp += tsAddition;
            }
            
            if (logger.isDebugEnabled() && timeStamp != QueryConstants.UNSET_TIMESTAMP) {
                logger.debug(LogUtil.addCustomAnnotations("Re-resolved stale function " + functionNames.toString() + "at timestamp " + timeStamp, connection));
            }
            return functionsFound;
        }

        private void getFunctionFromCache(List<String> functionNames,
                List<PFunction> functionsFound, boolean getOnlyTemporyFunctions) {
            Iterator<String> iterator = functionNames.iterator();
            while(iterator.hasNext()) {
                PFunction function = null;
                String functionName = iterator.next();
                try {
                    function = connection.getMetaDataCache().getFunction(new PTableKey(connection.getTenantId(), functionName));
                } catch (FunctionNotFoundException e1) {
                    if (connection.getTenantId() != null) { // Check with null tenantId next
                        try {
                            function = connection.getMetaDataCache().getFunction(new PTableKey(null, functionName));
                        } catch (FunctionNotFoundException e2) {
                        }
                    }
                }
                if (function != null) {
                    if (getOnlyTemporyFunctions) {
                        if (function.isTemporaryFunction()) {
                            functionsFound.add(function);
                            iterator.remove();
                        }
                    } else {
                        functionsFound.add(function);
                        iterator.remove();
                    }
                }
            }
        }

        protected PTable addDynamicColumns(List<ColumnDef> dynColumns, PTable theTable)
                throws SQLException {
            if (!dynColumns.isEmpty()) {
                List<PColumn> allcolumns = new ArrayList<PColumn>();
                List<PColumn> existingColumns = theTable.getColumns();
                // Need to skip the salting column, as it's added in the makePTable call below
                allcolumns.addAll(theTable.getBucketNum() == null ? existingColumns : existingColumns.subList(1, existingColumns.size()));
                // Position still based on with the salting columns
                int position = existingColumns.size();
                PName defaultFamilyName = PNameFactory.newName(SchemaUtil.getEmptyColumnFamily(theTable));
                for (ColumnDef dynColumn : dynColumns) {
                    PName familyName = defaultFamilyName;
                    PName name = PNameFactory.newName(dynColumn.getColumnDefName().getColumnName());
                    String family = dynColumn.getColumnDefName().getFamilyName();
                    if (family != null) {
                        theTable.getColumnFamily(family); // Verifies that column family exists
                        familyName = PNameFactory.newName(family);
                    }
                    allcolumns.add(new PColumnImpl(name, familyName, dynColumn.getDataType(), dynColumn.getMaxLength(),
                            dynColumn.getScale(), dynColumn.isNull(), position, dynColumn.getSortOrder(), dynColumn.getArraySize(), null, false, dynColumn.getExpression(), false, true, Bytes.toBytes(dynColumn.getColumnDefName().getColumnName())));
                    position++;
                }
                theTable = PTableImpl.makePTable(theTable, allcolumns);
            }
            return theTable;
        }
        
        @Override
        public PFunction resolveFunction(String functionName) throws SQLException {
            PFunction function = functionMap.get(functionName);
            if(function == null) {
                throw new FunctionNotFoundException(functionName);
            }
            return function;
        }

        @Override
        public boolean hasUDFs() {
            return !functions.isEmpty();
        }
    }

    private static class MultiTableColumnResolver extends BaseColumnResolver implements TableNodeVisitor<Void> {
        protected final ListMultimap<String, TableRef> tableMap;
        protected final List<TableRef> tables;
        private String connectionSchemaName;

        private MultiTableColumnResolver(PhoenixConnection connection, int tsAddition) {
        	super(connection, tsAddition);
            tableMap = ArrayListMultimap.<String, TableRef> create();
            tables = Lists.newArrayList();
            try {
                connectionSchemaName = connection.getSchema();
            } catch (SQLException e) {
                // ignore
            }
        }

        private MultiTableColumnResolver(PhoenixConnection connection, int tsAddition, Map<String, UDFParseNode> udfParseNodes) throws SQLException {
            super(connection, tsAddition, false, udfParseNodes);
            tableMap = ArrayListMultimap.<String, TableRef> create();
            tables = Lists.newArrayList();
        }

        @Override
        public List<TableRef> getTables() {
            return tables;
        }

        @Override
        public Void visit(BindTableNode boundTableNode) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public Void visit(JoinTableNode joinNode) throws SQLException {
            joinNode.getLHS().accept(this);
            joinNode.getRHS().accept(this);
            return null;
        }

        @Override
        public Void visit(NamedTableNode tableNode) throws SQLException {
            String alias = tableNode.getAlias();
            TableRef tableRef = createTableRef(connectionSchemaName, tableNode, true);
            PTable theTable = tableRef.getTable();

            if (alias != null) {
                tableMap.put(alias, tableRef);
            }

            String name = theTable.getName().getString();
            //avoid having one name mapped to two identical TableRef.
            if (alias == null || !alias.equals(name)) {
            	tableMap.put(name, tableRef);
            }
            tables.add(tableRef);
            return null;
        }

        @Override
        public Void visit(DerivedTableNode subselectNode) throws SQLException {
            List<AliasedNode> selectNodes = subselectNode.getSelect().getSelect();
            List<PColumn> columns = new ArrayList<PColumn>();
            int position = 0;
            for (AliasedNode aliasedNode : selectNodes) {
                String alias = aliasedNode.getAlias();
                if (alias == null) {
                    ParseNode node = aliasedNode.getNode();
                    if (node instanceof WildcardParseNode
                            || node instanceof TableWildcardParseNode
                            || node instanceof FamilyWildcardParseNode)
                        throw new SQLFeatureNotSupportedException("Wildcard in subqueries not supported.");

                    alias = SchemaUtil.normalizeIdentifier(node.getAlias());
                }
                if (alias == null) {
                    // Use position as column name for anonymous columns, which can be
                    // referenced by an outer wild-card select.
                    alias = String.valueOf(position);
                }
                PName name = PNameFactory.newName(alias);
                PColumnImpl column = new PColumnImpl(PNameFactory.newName(alias),
                        PNameFactory.newName(QueryConstants.DEFAULT_COLUMN_FAMILY),
                        null, 0, 0, true, position++, SortOrder.ASC, null, null, false, null, false, false, name.getBytes());
                columns.add(column);
            }
            PTable t = PTableImpl.makePTable(null, PName.EMPTY_NAME, PName.EMPTY_NAME, PTableType.SUBQUERY, null,
                    MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM, null, null, columns, null, null,
                    Collections.<PTable> emptyList(), false, Collections.<PName> emptyList(), null, null, false, false,
                    false, null, null, null, false, false, 0, 0L, SchemaUtil
                            .isNamespaceMappingEnabled(PTableType.SUBQUERY, connection.getQueryServices().getProps()), null, false, ImmutableStorageScheme.ONE_CELL_PER_COLUMN, QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, PTable.EncodedCQCounter.NULL_COUNTER);

            String alias = subselectNode.getAlias();
            TableRef tableRef = new TableRef(alias, t, MetaDataProtocol.MIN_TABLE_TIMESTAMP, false);
            tableMap.put(alias, tableRef);
            tables.add(tableRef);
            return null;
        }

        private static class ColumnFamilyRef {
            private final TableRef tableRef;
            private final PColumnFamily family;

            ColumnFamilyRef(TableRef tableRef, PColumnFamily family) {
                this.tableRef = tableRef;
                this.family = family;
            }

            public TableRef getTableRef() {
                return tableRef;
            }

            public PColumnFamily getFamily() {
                return family;
            }
        }

        @Override
        public TableRef resolveTable(String schemaName, String tableName) throws SQLException {
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            List<TableRef> tableRefs = tableMap.get(fullTableName);
            if (tableRefs.size() == 0) {
                throw new TableNotFoundException(fullTableName);
            } else if (tableRefs.size() > 1) {
                throw new AmbiguousTableException(tableName);
            } else {
                return tableRefs.get(0);
            }
        }

        private ColumnFamilyRef resolveColumnFamily(String tableName, String cfName) throws SQLException {
            if (tableName == null) {
                ColumnFamilyRef theColumnFamilyRef = null;
                Iterator<TableRef> iterator = tables.iterator();
                while (iterator.hasNext()) {
                    TableRef tableRef = iterator.next();
                    try {
                        PColumnFamily columnFamily = tableRef.getTable().getColumnFamily(cfName);
                        if (theColumnFamilyRef != null) { throw new TableNotFoundException(cfName); }
                        theColumnFamilyRef = new ColumnFamilyRef(tableRef, columnFamily);
                    } catch (ColumnFamilyNotFoundException e) {}
                }
                if (theColumnFamilyRef != null) { return theColumnFamilyRef; }
                throw new TableNotFoundException(cfName);
            } else {
                TableRef tableRef = null;
                try {
                    tableRef = resolveTable(null, tableName);
                } catch (TableNotFoundException e) {
                    return resolveColumnFamily(null, cfName);
                }
                PColumnFamily columnFamily = tableRef.getTable().getColumnFamily(cfName);
                return new ColumnFamilyRef(tableRef, columnFamily);
            }
        }

        @Override
        public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException {
            if (tableName == null) {
                int theColumnPosition = -1;
                TableRef theTableRef = null;
                Iterator<TableRef> iterator = tables.iterator();
                while (iterator.hasNext()) {
                    TableRef tableRef = iterator.next();
                    try {
                        PColumn column = tableRef.getTable().getColumnForColumnName(colName);
                        if (theTableRef != null) { throw new AmbiguousColumnException(colName); }
                        theTableRef = tableRef;
                        theColumnPosition = column.getPosition();
                    } catch (ColumnNotFoundException e) {

                    }
                }
                if (theTableRef != null) { return new ColumnRef(theTableRef, theColumnPosition); }
                throw new ColumnNotFoundException(colName);
            } else {
                try {
                    TableRef tableRef = resolveTable(schemaName, tableName);
                    PColumn column = tableRef.getTable().getColumnForColumnName(colName);
                    return new ColumnRef(tableRef, column.getPosition());
                } catch (TableNotFoundException e) {
                    // Try using the tableName as a columnFamily reference instead
                    ColumnFamilyRef cfRef = resolveColumnFamily(schemaName, tableName);
                    PColumn column = cfRef.getFamily().getPColumnForColumnName(colName);
                    return new ColumnRef(cfRef.getTableRef(), column.getPosition());
                }
            }
        }

        @Override
        public PSchema resolveSchema(String schemaName) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List<PSchema> getSchemas() {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    private static class ProjectedTableColumnResolver extends MultiTableColumnResolver {
        private final boolean isLocalIndex;
        private final List<TableRef> theTableRefs;
        private final Map<ColumnRef, Integer> columnRefMap;
        private ProjectedTableColumnResolver(PTable projectedTable, PhoenixConnection conn, Map<String, UDFParseNode> udfParseNodes) throws SQLException {
            super(conn, 0, udfParseNodes);
            Preconditions.checkArgument(projectedTable.getType() == PTableType.PROJECTED);
            this.isLocalIndex = projectedTable.getIndexType() == IndexType.LOCAL;
            this.columnRefMap = new HashMap<ColumnRef, Integer>();
            long ts = Long.MAX_VALUE;
            for (int i = projectedTable.getBucketNum() == null ? 0 : 1; i < projectedTable.getColumns().size(); i++) {
                PColumn column = projectedTable.getColumns().get(i);
                ColumnRef colRef = ((ProjectedColumn) column).getSourceColumnRef();
                TableRef tableRef = colRef.getTableRef();
                if (!tables.contains(tableRef)) {
                    String alias = tableRef.getTableAlias();
                    if (alias != null) {
                        this.tableMap.put(alias, tableRef);
                    }
                    String name = tableRef.getTable().getName().getString();
                    if (alias == null || !alias.equals(name)) {
                        tableMap.put(name, tableRef);
                    }
                    tables.add(tableRef);
                    if (tableRef.getLowerBoundTimeStamp() < ts) {
                        ts = tableRef.getLowerBoundTimeStamp();
                    }
                }
                this.columnRefMap.put(new ColumnRef(tableRef, colRef.getColumnPosition()), column.getPosition());
            }
            this.theTableRefs = ImmutableList.of(new TableRef(ParseNodeFactory.createTempAlias(), projectedTable, ts, false));
            
        }
        
        @Override
        public List<TableRef> getTables() {
            return theTableRefs;
        }
        
        @Override
        public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException {
            ColumnRef colRef;
            try {
                colRef = super.resolveColumn(schemaName, tableName, colName);
            } catch (ColumnNotFoundException e) {
                // This could be a ColumnRef for local index data column.
                TableRef tableRef = isLocalIndex ? super.getTables().get(0) : super.resolveTable(schemaName, tableName);
                if (tableRef.getTable().getIndexType() == IndexType.LOCAL) {
                    try {
                        TableRef parentTableRef = super.resolveTable(
                                tableRef.getTable().getSchemaName().getString(),
                                tableRef.getTable().getParentTableName().getString());
                        colRef = new ColumnRef(parentTableRef,
                                IndexUtil.getDataColumnFamilyName(colName),
                                IndexUtil.getDataColumnName(colName));
                    } catch (TableNotFoundException te) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
            Integer position = columnRefMap.get(colRef);
            if (position == null)
                throw new ColumnNotFoundException(colName);
            
            return new ColumnRef(theTableRefs.get(0), position);
        }
    }
}
