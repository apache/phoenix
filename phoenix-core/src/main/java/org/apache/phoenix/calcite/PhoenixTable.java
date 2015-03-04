package org.apache.phoenix.calcite;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.calcite.util.ImmutableBitSet;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;

/**
 * Implementation of Calcite {@link org.apache.calcite.schema.Table} SPI for
 * Phoenix.
 */
public class PhoenixTable extends AbstractTable implements TranslatableTable {
  public final PTable pTable;
  public final PhoenixConnection pc;

  public PhoenixTable(PhoenixConnection pc, PTable pTable) {
      this.pc = Preconditions.checkNotNull(pc);
      this.pTable = Preconditions.checkNotNull(pTable);
    }
    
    public PTable getTable() {
    	return pTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
        for (PColumn pColumn : pTable.getColumns()) {
            final int sqlTypeId = pColumn.getDataType().getResultSetSqlType();
            final PDataType pDataType = PDataType.fromTypeId(sqlTypeId);
            final SqlTypeName sqlTypeName1 = SqlTypeName.valueOf(pDataType.getSqlTypeName());
            final Integer maxLength = pColumn.getMaxLength();
            final Integer scale = pColumn.getScale();
            if (maxLength != null && scale != null) {
                builder.add(pColumn.getName().getString(), sqlTypeName1, maxLength, scale);
            } else if (maxLength != null) {
                builder.add(pColumn.getName().getString(), sqlTypeName1, maxLength);
            } else {
                builder.add(pColumn.getName().getString(), sqlTypeName1);
            }
        }
        return builder.build();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new PhoenixTableScan(cluster, cluster.traitSetOf(PhoenixRel.CONVENTION), relOptTable, null);
    }

    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            @Override
            public Double getRowCount() {
                return 100d;
            }

            @Override
            public boolean isKey(ImmutableBitSet immutableBitSet) {
                return false;
            }
        };
    }
}
