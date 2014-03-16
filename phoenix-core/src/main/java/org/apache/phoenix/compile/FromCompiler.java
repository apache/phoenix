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
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.BindTableNode;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SingleTableStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TableNodeVisitor;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.AmbiguousTableException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PColumnFamilyImpl;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

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
            return Collections.emptyList();
        }

        @Override
        public TableRef resolveTable(String schemaName, String tableName)
                throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException {
            throw new UnsupportedOperationException();
        }
    };

    public static ColumnResolver getResolverForCreation(final CreateTableStatement statement, final PhoenixConnection connection)
            throws SQLException {
        TableName baseTable = statement.getBaseTableName();
        if (baseTable == null) {
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
                byte[] fullTableName = SchemaUtil.getTableNameAsBytes(baseTable.getSchemaName(), baseTable.getTableName());
                HTableInterface htable = null;
                try {
                    htable = services.getTable(fullTableName);
                } catch (UnsupportedOperationException ignore) {
                    throw e; // For Connectionless
                } finally {
                    if (htable != null) Closeables.closeQuietly(htable);
                }
                tableNode = NamedTableNode.create(null, baseTable, statement.getColumnDefs());
                return new SingleTableColumnResolver(connection, tableNode, e.getTimeStamp());
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
     *             if unsupported constructs appear in the FROM clause. Currently only a single table name is supported.
     * @throws TableNotFoundException
     *             if table name not found in schema
     */
    public static ColumnResolver getResolverForQuery(SelectStatement statement, PhoenixConnection connection)
    		throws SQLException {
    	List<TableNode> fromNodes = statement.getFrom();
        if (fromNodes.size() == 1)
            return new SingleTableColumnResolver(connection, (NamedTableNode)fromNodes.get(0), true);

        MultiTableColumnResolver visitor = new MultiTableColumnResolver(connection);
        for (TableNode node : fromNodes) {
            node.accept(visitor);
        }
        return visitor;
    }

    public static ColumnResolver getResolver(NamedTableNode tableNode, PhoenixConnection connection) throws SQLException {
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, tableNode, true);
        return visitor;
    }
    
    public static ColumnResolver getResolverForMutation(SingleTableStatement statement, PhoenixConnection connection)
            throws SQLException {
        SingleTableColumnResolver visitor = new SingleTableColumnResolver(connection, statement.getTable(), false);
        return visitor;
    }
    

    private static class SingleTableColumnResolver extends BaseColumnResolver {
        	private final List<TableRef> tableRefs;
        	private final String alias;
    	
       public SingleTableColumnResolver(PhoenixConnection connection, NamedTableNode table, long timeStamp) throws SQLException  {
           super(connection);
           List<PColumnFamily> families = Lists.newArrayListWithExpectedSize(table.getDynamicColumns().size());
           for (ColumnDef def : table.getDynamicColumns()) {
               if (def.getColumnDefName().getFamilyName() != null) {
                   families.add(new PColumnFamilyImpl(PNameFactory.newName(def.getColumnDefName().getFamilyName()),Collections.<PColumn>emptyList()));
               }
           }
           Long scn = connection.getSCN();
           PTable theTable = new PTableImpl(connection.getTenantId(), table.getName().getSchemaName(), table.getName().getTableName(), scn == null ? HConstants.LATEST_TIMESTAMP : scn, families);
           theTable = this.addDynamicColumns(table.getDynamicColumns(), theTable);
           alias = null;
           tableRefs = ImmutableList.of(new TableRef(alias, theTable, timeStamp, !table.getDynamicColumns().isEmpty()));
       }
       
        public SingleTableColumnResolver(PhoenixConnection connection, NamedTableNode tableNode, boolean updateCacheImmediately) throws SQLException {
            super(connection);
            alias = tableNode.getAlias();
            TableRef tableRef = createTableRef(tableNode, updateCacheImmediately);
            tableRefs = ImmutableList.of(tableRef);
        }

		@Override
		public List<TableRef> getTables() {
			return tableRefs;
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
        	        ? tableRef.getTable().getColumnFamily(tableName).getColumn(colName)
        			: tableRef.getTable().getColumn(colName);
            return new ColumnRef(tableRef, column.getPosition());
		}

    }

    private static abstract class BaseColumnResolver implements ColumnResolver {
        protected final PhoenixConnection connection;
        protected final MetaDataClient client;
        
        private BaseColumnResolver(PhoenixConnection connection) {
        	this.connection = connection;
            this.client = new MetaDataClient(connection);
        }

        protected TableRef createTableRef(NamedTableNode tableNode, boolean updateCacheImmediately) throws SQLException {
            String tableName = tableNode.getName().getTableName();
            String schemaName = tableNode.getName().getSchemaName();
            long timeStamp = QueryConstants.UNSET_TIMESTAMP;
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            PName tenantId = connection.getTenantId();
            PTable theTable = null;
            if (updateCacheImmediately || connection.getAutoCommit()) {
                MetaDataMutationResult result = client.updateCache(schemaName, tableName);
                timeStamp = result.getMutationTime();
                theTable = result.getTable();
                if (theTable == null) {
                    throw new TableNotFoundException(schemaName, tableName, timeStamp);
                }
            } else {
                try {
                    theTable = connection.getMetaDataCache().getTable(new PTableKey(tenantId, fullTableName));
                } catch (TableNotFoundException e1) {
                    if (tenantId != null) { // Check with null tenantId next
                        try {
                            theTable = connection.getMetaDataCache().getTable(new PTableKey(null, fullTableName));
                        } catch (TableNotFoundException e2) {
                        }
                    }
                }
                // We always attempt to update the cache in the event of a TableNotFoundException
                if (theTable == null) {
                    MetaDataMutationResult result = client.updateCache(schemaName, tableName);
                    if (result.wasUpdated()) {
                        timeStamp = result.getMutationTime();
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
            TableRef tableRef = new TableRef(tableNode.getAlias(), theTable, timeStamp, !dynamicColumns.isEmpty());
            if (logger.isDebugEnabled() && timeStamp != QueryConstants.UNSET_TIMESTAMP) {
                logger.debug("Re-resolved stale table " + fullTableName + " with seqNum " + tableRef.getTable().getSequenceNumber() + " at timestamp " + tableRef.getTable().getTimeStamp() + " with " + tableRef.getTable().getColumns().size() + " columns: " + tableRef.getTable().getColumns());
            }
            return tableRef;
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
                            dynColumn.getScale(), dynColumn.isNull(), position, dynColumn.getSortOrder(), dynColumn.getArraySize(), null, false));
                    position++;
                }
                theTable = PTableImpl.makePTable(theTable, allcolumns);
            }
            return theTable;
        }
    }
    
    // TODO: unused, but should be used for joins - make private once used
    public static class MultiTableColumnResolver extends BaseColumnResolver implements TableNodeVisitor {
        private final ListMultimap<String, TableRef> tableMap;
        private final List<TableRef> tables;

        private MultiTableColumnResolver(PhoenixConnection connection) {
        	super(connection);
            tableMap = ArrayListMultimap.<String, TableRef> create();
            tables = Lists.newArrayList();
        }

        @Override
        public List<TableRef> getTables() {
            return tables;
        }

        @Override
        public void visit(BindTableNode boundTableNode) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public void visit(JoinTableNode joinNode) throws SQLException {
            joinNode.getTable().accept(this);
        }

        @Override
        public void visit(NamedTableNode tableNode) throws SQLException {
            String alias = tableNode.getAlias();
            TableRef tableRef = createTableRef(tableNode, true);
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
        }

        @Override
        public void visit(DerivedTableNode subselectNode) throws SQLException {
            throw new SQLFeatureNotSupportedException();
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
                TableRef tableRef = resolveTable(null, tableName);
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
                        PColumn column = tableRef.getTable().getColumn(colName);
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
                    PColumn column = tableRef.getTable().getColumn(colName);
                    return new ColumnRef(tableRef, column.getPosition());
                } catch (TableNotFoundException e) {
                    // Try using the tableName as a columnFamily reference instead
                    ColumnFamilyRef cfRef = resolveColumnFamily(schemaName, tableName);
                    PColumn column = cfRef.getFamily().getColumn(colName);
                    return new ColumnRef(cfRef.getTableRef(), column.getPosition());
                }
            }
        }

    }
}
