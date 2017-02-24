package org.apache.phoenix.calcite;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.TupleProjectionCompiler;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ArgumentTypeMismatchException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TableMapping {
    private final TableRef tableRef;
    private final TableRef dataTableRef;
    private final List<PColumn> mappedColumns;
    private final int extendedColumnsOffset;
    private final TableRef extendedTableRef;
    // For column resolving
    private final List<String> names = Lists.newArrayList();
    private final Map<String, Map<String, Integer>> groupMap = Maps.newHashMap();

    public TableMapping(PTable table) {
        this.tableRef = new TableRef(table);
        this.dataTableRef = null;
        this.mappedColumns = getMappedColumns(table);
        this.extendedColumnsOffset = mappedColumns.size();
        this.extendedTableRef = null;
        init();
    }

    public TableMapping(TableRef tableRef, TableRef dataTableRef, boolean extend) throws SQLException {
        this.tableRef = tableRef;
        this.dataTableRef = dataTableRef;
        if (!extend) {
            this.mappedColumns = getMappedColumns(tableRef.getTable());
            this.extendedColumnsOffset = mappedColumns.size();
            this.extendedTableRef = null;            
        } else {
            this.mappedColumns = Lists.newArrayList();
            this.mappedColumns.addAll(getMappedColumns(tableRef.getTable()));
            this.extendedColumnsOffset = mappedColumns.size();
            final PTable dataTable = dataTableRef.getTable();
            final List<PColumn> projectedColumns = getDataTableMappedColumns(dataTableRef, mappedColumns);
            this.mappedColumns.addAll(projectedColumns);
            PTable extendedTable = PTableImpl.makePTable(dataTable.getTenantId(),
                    TupleProjectionCompiler.PROJECTED_TABLE_SCHEMA, dataTable.getName(),
                    PTableType.PROJECTED, null, dataTable.getTimeStamp(),
                    dataTable.getSequenceNumber(), dataTable.getPKName(), null,
                    projectedColumns, null, null, Collections.<PTable>emptyList(),
                    dataTable.isImmutableRows(), Collections.<PName>emptyList(), null, null,
                    dataTable.isWALDisabled(), false, dataTable.getStoreNulls(),
                    dataTable.getViewType(), null, null, dataTable.rowKeyOrderOptimizable(),
                    dataTable.isTransactional(), dataTable.getUpdateCacheFrequency(),
                    dataTable.getIndexDisableTimestamp(), dataTable.isNamespaceMapped(),
                    dataTable.getAutoPartitionSeqName(), dataTable.isAppendOnlySchema());
            this.extendedTableRef = new TableRef(extendedTable);
        }
        init();
    }

    private void init() {
        Set<String> nameSet = Sets.newHashSet();
        boolean dup = false;
        for (int i = 0; i < mappedColumns.size(); i++) {
            PColumn column = mappedColumns.get(i);
            String familyName = column.getFamilyName() == null ? "" : column.getFamilyName().getString();
            String name = column.getName().getString();
            Map<String, Integer> subMap = groupMap.get(familyName);
            if (subMap == null) {
              subMap = Maps.newHashMap();
              groupMap.put(familyName, subMap);
            }
            subMap.put(name, i);
            dup = dup || !nameSet.add(name);
        }
        for (int i = 0; i < mappedColumns.size(); i++) {
            PColumn column = mappedColumns.get(i);
            String familyName = column.getFamilyName() == null ? "" : column.getFamilyName().getString();
            String name = column.getName().getString();
            String translatedName = !dup ? name
                    : SchemaUtil.getCaseSensitiveColumnDisplayName(familyName, column.getName().getString());
            names.add(translatedName);
        }
    }

    public TableRef getTableRef() {
        return tableRef;
    }
    
    public PTable getPTable() {
        return tableRef.getTable();
    }
    
    public TableRef getDataTableRef() {
        return dataTableRef;
    }

    public List<String> getColumnNames() {
        return names;
    }

    public List<PColumn> getMappedColumns() {
        return mappedColumns;
    }
    
    public boolean hasExtendedColumns() {
        return extendedTableRef != null;
    }

    public List<Pair<RelDataTypeField, List<String>>> resolveColumn(
            RelDataType rowType, RelDataTypeFactory typeFactory, List<String> names) {
        List<Pair<RelDataTypeField, List<String>>> ret = new ArrayList<>();
        if (names.size() >= 2) {
            Map<String, Integer> subMap = groupMap.get(names.get(0));
            if (subMap != null) {
                Integer index = subMap.get(names.get(1));
                if (index != null) {
                    ret.add(
                            new Pair<RelDataTypeField, List<String>>(
                                    rowType.getFieldList().get(index),
                                    names.subList(2, names.size())));
                }
            }
        }

        final String columnName = names.get(0);
        final List<String> remainder = names.subList(1, names.size());
        for (int i = 0; i < this.names.size(); i++) {
            if (columnName.equals(this.names.get(i))) {
                ret.add(
                        new Pair<RelDataTypeField, List<String>>(
                                rowType.getFieldList().get(i), remainder));
                return ret;
            }
        }

        final List<String> priorityGroups = Arrays.asList("", QueryConstants.DEFAULT_COLUMN_FAMILY);
        for (String group : priorityGroups) {
            Map<String, Integer> subMap = groupMap.get(group);
            if (subMap != null) {
                Integer index = subMap.get(columnName);
                if (index != null) {
                    ret.add(
                            new Pair<RelDataTypeField, List<String>>(
                                    rowType.getFieldList().get(index), remainder));
                    return ret;
                }
            }
        }
        for (Map.Entry<String, Map<String, Integer>> entry : groupMap.entrySet()) {
            if (priorityGroups.contains(entry.getKey())) {
                continue;
            }
            Integer index = entry.getValue().get(columnName);
            if (index != null) {
                ret.add(
                        new Pair<RelDataTypeField, List<String>>(
                                rowType.getFieldList().get(index), remainder));
            }
        }

        if (ret.isEmpty() && names.size() == 1) {
            Map<String, Integer> subMap = groupMap.get(columnName);
            if (subMap != null) {
                List<Map.Entry<String, Integer>> entries =
                        new ArrayList<>(subMap.entrySet());
                Collections.sort(
                        entries,
                        new Comparator<Map.Entry<String, Integer>>() {
                            @Override public int compare(
                                    Entry<String, Integer> o1, Entry<String, Integer> o2) {
                                return o1.getValue() - o2.getValue();
                            }
                        });
                ret.add(
                        new Pair<RelDataTypeField, List<String>>(
                                new RelDataTypeFieldImpl(
                                        columnName, -1,
                                        createStructType(
                                                rowType,
                                                typeFactory,
                                                entries)),
                                remainder));
            }
        }

        return ret;
    }

    private static RelDataType createStructType(
            final RelDataType rowType,
            RelDataTypeFactory typeFactory,
            final List<Map.Entry<String, Integer>> entries) {
        return typeFactory.createStructType(
                StructKind.PEEK_FIELDS,
                new AbstractList<RelDataType>() {
                    @Override public RelDataType get(int index) {
                        final int i = entries.get(index).getValue();
                        return rowType.getFieldList().get(i).getType();
                    }
                    @Override public int size() {
                        return entries.size();
                    }
                },
                new AbstractList<String>() {
                    @Override public String get(int index) {
                        return entries.get(index).getKey();
                    }
                    @Override public int size() {
                        return entries.size();
                    }
                });
    }
    
    public Expression newColumnExpression(int index) {
        ColumnRef colRef = new ColumnRef(
                index < extendedColumnsOffset ? tableRef : extendedTableRef,
                this.mappedColumns.get(index).getPosition());
        try {
            return colRef.newColumnExpression();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    public ImmutableBitSet getDefaultExtendedColumnRef() {
        return ImmutableBitSet.range(extendedColumnsOffset, mappedColumns.size());
    }
    
    public ImmutableBitSet getExtendedColumnRef(List<RexNode> exprs) {
        if (!hasExtendedColumns()) {
            return ImmutableBitSet.of();
        }
        
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (RexNode expr : exprs) {
            builder.addAll(InputFinder.analyze(expr).inputBitSet.build());
        }
        for (int i = 0; i < extendedColumnsOffset; i++) {
            builder.clear(i);
        }
        return builder.build();
    }
    
    public org.apache.hadoop.hbase.util.Pair<Integer, Integer> getExtendedColumnReferenceCount(ImmutableBitSet columnRef) {
        Set<String> cf = Sets.newHashSet();
        int columnCount = 0;
        for (int i = extendedColumnsOffset; i < mappedColumns.size(); i++) {
            if (columnRef.get(i)) {
                PColumn dataColumn = ((ProjectedColumn) mappedColumns.get(i))
                        .getSourceColumnRef().getColumn();
                cf.add(dataColumn.getFamilyName().getString());
                columnCount++;
            }
        }
        return new org.apache.hadoop.hbase.util.Pair<Integer, Integer>(cf.size(), columnCount);
    }
    
    public PTable createProjectedTable(boolean retainPKColumns) {
        List<ColumnRef> sourceColumnRefs = Lists.<ColumnRef> newArrayList();
        List<PColumn> columns = retainPKColumns ?
                  tableRef.getTable().getColumns() : mappedColumns.subList(0, extendedColumnsOffset);
        for (PColumn column : columns) {
            sourceColumnRefs.add(new ColumnRef(tableRef, column.getPosition()));
        }
        if (extendedColumnsOffset < mappedColumns.size()) {
            for (PColumn column : mappedColumns.subList(extendedColumnsOffset, mappedColumns.size())) {
                sourceColumnRefs.add(new ColumnRef(extendedTableRef, column.getPosition()));
            }
        }
        
        try {
            return TupleProjectionCompiler.createProjectedTable(tableRef, sourceColumnRefs, retainPKColumns);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    public TupleProjector createTupleProjector(boolean retainPKColumns) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        List<Expression> exprs = Lists.<Expression> newArrayList();
        for (int i = 0; i < mappedColumns.size(); i++) {
            if (!SchemaUtil.isPKColumn(mappedColumns.get(i)) || !retainPKColumns) {
                Expression expr = newColumnExpression(i);
                exprs.add(expr);
                builder.addField(expr);
            }
        }
        
        return new TupleProjector(builder.build(), exprs.toArray(new Expression[exprs.size()]));
    }

    public RowProjector createRowProjector() throws SQLException {
        return createRowProjector(null);
    }

    public RowProjector createRowProjector(List<PColumn> targetColumns) throws SQLException {
        List<ColumnProjector> columnProjectors = Lists.<ColumnProjector>newArrayList();
        for (int i = 0; i < mappedColumns.size(); i++) {
            PColumn column = mappedColumns.get(i);
            Expression expr = newColumnExpression(i); // Do not use column.position() here.
            if (targetColumns != null) {
                PDatum targetColumn = targetColumns.get(i);
                if (targetColumn.getDataType() != expr.getDataType()) {
                    PDataType<?> targetType = targetColumn.getDataType();
                    assert expr.getDataType() == null || expr.getDataType().isCastableTo(targetType);
                    expr = CoerceExpression.create(expr, targetType, targetColumn.getSortOrder(), targetColumn.getMaxLength());
                }
            }
            columnProjectors.add(new ExpressionProjector(column.getName().getString(), tableRef.getTable().getName().getString(), expr, false));
        }
        // TODO get estimate row size
        return new RowProjector(columnProjectors, 0, false);        
    }
    
    public void setupScanForExtendedTable(Scan scan, ImmutableBitSet extendedColumnRef,
            PhoenixConnection connection) throws SQLException {
        if (extendedTableRef == null || extendedColumnRef.isEmpty()) {
            return;
        }
        
        TableRef dataTableRef = null;
        List<PColumn> dataColumns = Lists.newArrayList();
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        List<Expression> exprs = Lists.<Expression> newArrayList();
        for (int i = extendedColumnsOffset; i < mappedColumns.size(); i++) {
            ProjectedColumn column = (ProjectedColumn) mappedColumns.get(i);
            builder.addField(column);
            if (extendedColumnRef.get(i)) {
                dataColumns.add(column.getSourceColumnRef().getColumn());
                exprs.add(column.getSourceColumnRef().newColumnExpression());
                if (dataTableRef == null) {
                    dataTableRef = column.getSourceColumnRef().getTableRef();
                }
            } else {
                exprs.add(LiteralExpression.newConstant(null));
            }
        }
        if (dataColumns.isEmpty()) {
            return;
        }
        
        // Set data columns to be join back from data table.
        serializeDataTableColumnsToJoin(scan, dataColumns);
        // Set tuple projector of the data columns.
        TupleProjector projector = new TupleProjector(builder.build(), exprs.toArray(new Expression[exprs.size()]));
        TupleProjector.serializeProjectorIntoScan(scan, projector, IndexUtil.INDEX_PROJECTOR);
        PTable dataTable = dataTableRef.getTable();
        // Set index maintainer of the local index.
        serializeIndexMaintainerIntoScan(scan, dataTable, connection);
        // Set view constants if exists.
        serializeViewConstantsIntoScan(scan, dataTable);
    }

    private static void serializeDataTableColumnsToJoin(Scan scan, List<PColumn> dataColumns) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, dataColumns.size());
            for (PColumn column : dataColumns) {
                Bytes.writeByteArray(output, column.getFamilyName().getBytes());
                Bytes.writeByteArray(output, column.getName().getBytes());
            }
            scan.setAttribute(BaseScannerRegionObserver.DATA_TABLE_COLUMNS_TO_JOIN, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void serializeIndexMaintainerIntoScan(Scan scan, PTable dataTable, PhoenixConnection connection) throws SQLException {
        PName name = getPTable().getName();
        List<PTable> indexes = Lists.newArrayListWithExpectedSize(1);
        for (PTable index : dataTable.getIndexes()) {
            if (index.getName().equals(name) && index.getIndexType() == IndexType.LOCAL) {
                indexes.add(index);
                break;
            }
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        IndexMaintainer.serialize(dataTable, ptr, indexes, connection);
        scan.setAttribute(BaseScannerRegionObserver.LOCAL_INDEX_BUILD, ByteUtil.copyKeyBytesIfNecessary(ptr));
        if (dataTable.isTransactional()) {
            scan.setAttribute(BaseScannerRegionObserver.TX_STATE, connection.getMutationState().encodeTransaction());
        }
    }

    private static void serializeViewConstantsIntoScan(Scan scan, PTable dataTable) {
        int dataPosOffset = (dataTable.getBucketNum() != null ? 1 : 0) + (dataTable.isMultiTenant() ? 1 : 0);
        int nViewConstants = 0;
        if (dataTable.getType() == PTableType.VIEW) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            List<PColumn> dataPkColumns = dataTable.getPKColumns();
            for (int i = dataPosOffset; i < dataPkColumns.size(); i++) {
                PColumn dataPKColumn = dataPkColumns.get(i);
                if (dataPKColumn.getViewConstant() != null) {
                    nViewConstants++;
                }
            }
            if (nViewConstants > 0) {
                byte[][] viewConstants = new byte[nViewConstants][];
                int j = 0;
                for (int i = dataPosOffset; i < dataPkColumns.size(); i++) {
                    PColumn dataPkColumn = dataPkColumns.get(i);
                    if (dataPkColumn.getViewConstant() != null) {
                        if (IndexUtil.getViewConstantValue(dataPkColumn, ptr)) {
                            viewConstants[j++] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                }
                serializeViewConstantsIntoScan(viewConstants, scan);
            }
        }
    }

    private static void serializeViewConstantsIntoScan(byte[][] viewConstants, Scan scan) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, viewConstants.length);
            for (byte[] viewConstant : viewConstants) {
                Bytes.writeByteArray(output, viewConstant);
            }
            scan.setAttribute(BaseScannerRegionObserver.VIEW_CONSTANTS, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private static List<PColumn> getMappedColumns(PTable pTable) {
        int initPosition =
                  (pTable.getBucketNum() ==null ? 0 : 1)
                + (pTable.isMultiTenant() ? 1 : 0)
                + (pTable.getViewIndexId() == null ? 0 : 1);
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(pTable.getColumns().size() - initPosition);
        for (int i = initPosition; i < pTable.getPKColumns().size(); i++) {
            columns.add(pTable.getPKColumns().get(i));
        }
        for (PColumnFamily family : pTable.getColumnFamilies()) {
            for (PColumn column : family.getColumns()) {
                columns.add(column);
            }
        }
        
        return columns;
    }
    
    private static List<PColumn> getDataTableMappedColumns(TableRef dataTableRef, List<PColumn> mappedColumns) {
        Set<String> names = Sets.newHashSet();
        for (PColumn column : mappedColumns) {
            names.add(column.getName().getString());
        }
        List<PColumn> projectedColumns = new ArrayList<PColumn>();
        for (PColumnFamily cf : dataTableRef.getTable().getColumnFamilies()) {
            for (PColumn sourceColumn : cf.getColumns()) {
                String colName = IndexUtil.getIndexColumnName(sourceColumn);
                if (!names.contains(colName)) {
                    ColumnRef sourceColumnRef =
                            new ColumnRef(dataTableRef, sourceColumn.getPosition());
                    PColumn column = new ProjectedColumn(PNameFactory.newName(colName),
                            cf.getName(), projectedColumns.size(),
                            sourceColumn.isNullable(), sourceColumnRef);
                    projectedColumns.add(column);
                }
            }            
        }
        
        return projectedColumns;
    }
}
