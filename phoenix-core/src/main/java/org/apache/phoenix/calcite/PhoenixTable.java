package org.apache.phoenix.calcite;

import java.io.IOException;
import java.sql.SQLException;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Implementation of Calcite {@link org.apache.calcite.schema.Table} SPI for
 * Phoenix.
 */
public class PhoenixTable extends AbstractTable implements TranslatableTable {
  public final TableMapping tableMapping;
  public final ImmutableBitSet pkBitSet;
  public final RelCollation collation;
  public final long byteCount;
  public final long rowCount;
  public final PhoenixConnection pc;

  public PhoenixTable(PhoenixConnection pc, TableRef tableRef) throws SQLException {
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
              rowCount = estimatedCount.getFirst();
              byteCount = estimatedCount.getSecond();
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
                  desc = pc.getQueryServices().getAdmin().getTableDescriptor(
                          pTable.getPhysicalName().getBytes());
              }
              byteCount = StatisticsUtil.getGuidePostDepth(
                      guidepostPerRegion, guidepostWidth, desc) / 2;
              long rowSize = SchemaUtil.estimateRowSize(pTable);
              rowCount = byteCount / rowSize;
          }
      } catch (SQLException | IOException e) {
          throw new RuntimeException(e);
      }
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
            builder.add(pColumn.getName().getString(), type);
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
        };
    }
}
