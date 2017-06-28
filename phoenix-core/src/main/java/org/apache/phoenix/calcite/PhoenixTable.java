package org.apache.phoenix.calcite;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Implementation of Calcite {@link org.apache.calcite.schema.Table} SPI for
 * Phoenix.
 */
public class PhoenixTable extends AbstractTable
    implements ExtensibleTable, TranslatableTable, CustomColumnResolvingTable, Wrapper {
  public final TableMapping tableMapping;
  public final ImmutableBitSet pkBitSet;
  public final RelCollation collation;
  public final long byteCount;
  public final long rowCount;
  public final PhoenixConnection pc;
  public final RelDataTypeFactory typeFactory;
  public final InitializerExpressionFactory initializerExpressionFactory;

    private PhoenixTable(TableMapping tableMapping, ImmutableBitSet pkBitSet,
            RelCollation collation, long byteCount, long rowCount, PhoenixConnection pc,
            InitializerExpressionFactory initializerExpressionFactory,
            RelDataTypeFactory typeFactory) {
    this.tableMapping = tableMapping;
    this.pkBitSet = pkBitSet;
    this.collation = collation;
    this.byteCount = byteCount;
    this.rowCount = rowCount;
    this.pc = pc;
    this.initializerExpressionFactory = initializerExpressionFactory;
    this.typeFactory = typeFactory;
  }

  public PhoenixTable(PhoenixConnection pc, TableRef tableRef, final RelDataTypeFactory typeFactory) throws SQLException {
      this.pc = Preconditions.checkNotNull(pc);
      PTable pTable = tableRef.getTable();
      TableRef dataTable = null;
      if (pTable.getType() == PTableType.INDEX) {
          ColumnResolver x = FromCompiler.getResolver(
                  NamedTableNode.create(null,
                          TableName.create(pTable.getParentSchemaName().getString(),
                                  pTable.getParentTableName().getString()),
                          ImmutableList.<ColumnDef>of()), pc);
          final List<TableRef> tables = x.getTables();
          assert tables.size() == 1;
          dataTable = tables.get(0);          
      }
      this.tableMapping = new TableMapping(tableRef, dataTable, pTable.getIndexType() == IndexType.LOCAL);
      List<Integer> pkPositions = Lists.<Integer> newArrayList();
      List<RelFieldCollation> fieldCollations = Lists.<RelFieldCollation> newArrayList();
      final List<PColumn> columns = tableMapping.getMappedColumns();
      for (int i = 0; i < columns.size(); i++) {
          PColumn column = columns.get(i);
          if (SchemaUtil.isPKColumn(column)) {
              SortOrder sortOrder = column.getSortOrder();
              pkPositions.add(i);
              fieldCollations.add(new RelFieldCollation(i, sortOrder == SortOrder.ASC ? Direction.ASCENDING : Direction.DESCENDING));
          }
      }
      this.pkBitSet = ImmutableBitSet.of(pkPositions);
      this.collation = RelCollationTraitDef.INSTANCE.canonize(RelCollations.of(fieldCollations));
      try {
          PhoenixStatement stmt = new PhoenixStatement(pc);
          ColumnResolver resolver = FromCompiler.getResolver(tableRef);
          StatementContext context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
          Pair<Long, Long> estimatedCount = BaseResultIterators.getEstimatedCount(context, pTable);
          if (estimatedCount.getFirst() != null) {
              // FIXME Right now the row count for local index is not correct.
              if (dataTable == null) {
                  rowCount = estimatedCount.getFirst();
                  byteCount = estimatedCount.getSecond();
              } else {
                  Pair<Long, Long> dataTableEstimatedCount =
                          BaseResultIterators.getEstimatedCount(context, dataTable.getTable());
                  rowCount = dataTableEstimatedCount.getFirst();
                  byteCount = dataTableEstimatedCount.getSecond();
              }
          } else {
              if(pTable.getType() == PTableType.INDEX) {
                  rowCount = 1;
                  byteCount = 1;
              } else {
                  // TODO The props might not be the same as server props.
                  int guidepostPerRegion = pc.getQueryServices().getProps().getInt(
                          QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB,
                          QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_PER_REGION);
                  long guidepostWidth = pc.getQueryServices().getProps().getLong(
                          QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB,
                          QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES);
                  HTableDescriptor desc = null;
                  if (guidepostPerRegion > 0) {
                        desc =
                                pc.getQueryServices()
                                        .getAdmin()
                                        .getTableDescriptor(
                                            dataTable != null ? dataTable.getTable().getPhysicalName()
                                                    .getBytes() : pTable.getPhysicalName().getBytes());
                  }
                  byteCount = StatisticsUtil.getGuidePostDepth(
                      guidepostPerRegion, guidepostWidth, desc) / 2;
                  long rowSize = SchemaUtil.estimateRowSize(dataTable != null ? dataTable.getTable():pTable);
                  rowCount = byteCount / rowSize;
              }
          }
      } catch (SQLException | IOException e) {
          throw new RuntimeException(e);
      }
      this.typeFactory = typeFactory;
      this.initializerExpressionFactory =
              this.typeFactory == null ? null : new PhoenixTableInitializerExpressionFactory(
                  typeFactory, pc, tableMapping);
    }
  
    public List<PColumn> getColumns() {
        return tableMapping.getMappedColumns();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
        final List<PColumn> columns = tableMapping.getMappedColumns();
        for (int i = 0; i < columns.size(); i++) {
            PColumn pColumn = columns.get(i);
            RelDataType type = CalciteUtils.pDataTypeToRelDataType(
                    typeFactory, pColumn.getDataType(), pColumn.getMaxLength(),
                    pColumn.getScale(), pColumn.getArraySize());
            builder.add(tableMapping.getColumnNames().get(i), type);
            builder.nullable(pColumn.isNullable());
        }
        return builder.build();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return PhoenixTableScan.create(context.getCluster(), relOptTable);
    }

    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            @Override
            public Double getRowCount() {
                return (double) rowCount;
            }

            @Override
            public boolean isKey(ImmutableBitSet immutableBitSet) {
                return immutableBitSet.contains(pkBitSet);
            }

            @Override
            public List<RelCollation> getCollations() {
                return ImmutableList.<RelCollation>of(collation);
            }

            @Override
            public RelDistribution getDistribution() {
                return RelDistributions.RANDOM_DISTRIBUTED;
            }

            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                // TODO: As we don't support any referential constraints in Phoenix
                return ImmutableList.<RelReferentialConstraint> of();
            }
        };
    }

    @Override
    public List<org.apache.calcite.util.Pair<RelDataTypeField, List<String>>> resolveColumn(
            RelDataType rowType, RelDataTypeFactory typeFactory, List<String> names) {
        return tableMapping.resolveColumn(rowType, typeFactory, names);
    }

    @Override public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(initializerExpressionFactory)) {
          return aClass.cast(initializerExpressionFactory);
        }
        return null;
    }
    
    static class PhoenixTableInitializerExpressionFactory extends
            NullInitializerExpressionFactory {
        private final RexBuilder rexBuilder;
        private final PhoenixConnection pc;
        private final TableMapping tableMapping;

        PhoenixTableInitializerExpressionFactory(RelDataTypeFactory typeFactory,
                PhoenixConnection pc, TableMapping tableMapping) {
            super();
            this.rexBuilder = new RexBuilder(typeFactory);
            this.pc = pc;
            this.tableMapping = tableMapping;
        }
        
        public RexNode newColumnDefaultValue(RelOptTable table, int iColumn,
                InitializerContext context) {
            PColumn column = tableMapping.getMappedColumns().get(iColumn);
            String expressionStr = column.getExpressionStr();
            if(expressionStr == null) {
                RelDataType pDataTypeToRelDataType =
                        CalciteUtils.pDataTypeToRelDataType(rexBuilder.getTypeFactory(),
                            column.getDataType(), column.getMaxLength(),
                            column.getScale(), column.getArraySize());
                return new ImplicitNullLiteral(this.rexBuilder.makeNullLiteral(pDataTypeToRelDataType));
            }
            Expression defaultExpression = CalciteUtils.parseExpressionFromStr(expressionStr, pc);
            return CalciteUtils.convertColumnExpressionToLiteral(column, defaultExpression,
                rexBuilder);
        }
    }

    @Override
    public Table extend(List<RelDataTypeField> fields) {
        final ImmutableList.Builder<PColumn> extendedColumns = ImmutableList.builder();
        PTable table = tableMapping.getTableRef().getTable();
        for (RelDataTypeField field : fields) {
            final RelDataType colType = field.getType();
            int index = field.getName().indexOf(QueryConstants.NAME_SEPARATOR);
            String columnFamily = null;
            String columnName = field.getName();
            if (index > 0) {
                columnFamily = field.getName().substring(0, index);
                try {
                    table.getColumnFamily(columnFamily);
                } catch (ColumnFamilyNotFoundException e) {
                    throw new RuntimeException(e);
                }
                columnName = field.getName().substring(index + 1);
            } else {
                columnFamily =
                        tableMapping.getTableRef().getTable().getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY
                                : tableMapping.getTableRef().getTable().getDefaultFamilyName()
                                        .getString();
            }
            final PColumn column =
                    new PColumnImpl(PNameFactory.newName(columnName),
                            PNameFactory.newName(columnFamily), PDataType.fromSqlTypeName(field
                                    .getType().getSqlTypeName().getName()),
                            colType.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ? null
                                    : colType.getPrecision(),
                            colType.getScale() == RelDataType.SCALE_NOT_SPECIFIED ? null : colType
                                    .getScale(), colType.isNullable(),
                            (table.getBucketNum() != null ? (field.getIndex() + 1) : field
                                    .getIndex()),
                            SortOrder.ASC,// TODO: get this from metastore? test if specifying ASC
                                          // or DESC is allowed. use Docker image
                            colType.isStruct() ? field.getType().getFieldCount() : 0, null, false,
                            null, false, true, null);
            extendedColumns.add(column);
        }

        List<PColumn> tableColumns = table.getColumns();
        if(table.getBucketNum()!=null) {
            tableColumns = new ArrayList<PColumn>(table.getColumns().size()-1);
            for(int i = 1; i < table.getColumns().size() ; i++) {
                tableColumns.add(table.getColumns().get(i));
            }
        }
        final List<PColumn> allColumns =
                ImmutableList.copyOf(Iterables.concat(tableColumns, extendedColumns.build()));
        try {
            final PTable extendedTable =
                    PTableImpl.makePTable(tableMapping.getPTable(), allColumns);
            final TableMapping newMapping =
                    new TableMapping(extendedTable, tableMapping.getExtendedColumnsOffset());
            return new PhoenixTable(newMapping, pkBitSet, collation, byteCount, rowCount, pc,
                    initializerExpressionFactory, typeFactory);
        } catch (SQLException e) {
            throw new RuntimeException("Could not create extended table", e);
        }
    }

    @Override
    public int getExtendedColumnOffset() {
        return tableMapping.getExtendedColumnsOffset();
    }

}
