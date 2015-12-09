package org.apache.phoenix.calcite;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.phoenix.jdbc.PhoenixConnection;

public class PhoenixSequence extends AbstractTable implements TranslatableTable {
    public final String schemaName;
    public final String sequenceName;
    public final PhoenixConnection pc;
    
    public PhoenixSequence(String schemaName, String sequenceName, PhoenixConnection pc) {
        this.schemaName = schemaName;
        this.sequenceName = sequenceName;
        this.pc = pc;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
        builder.add("CURRENT_VALUE", typeFactory.createSqlType(SqlTypeName.BIGINT));
        return builder.build();
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        return null;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.SEQUENCE;
    }

}