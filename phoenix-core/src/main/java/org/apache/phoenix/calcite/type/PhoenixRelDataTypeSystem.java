package org.apache.phoenix.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class PhoenixRelDataTypeSystem extends RelDataTypeSystemImpl {
    
    public PhoenixRelDataTypeSystem() {
        super();
    }

    @Override
    public RelDataType deriveSumType(
        RelDataTypeFactory typeFactory, RelDataType argumentType) {
        RelDataType type;
        if (argumentType.getSqlTypeName() == SqlTypeName.DECIMAL) {
            type = typeFactory.createSqlType(SqlTypeName.DECIMAL);
        } else if (argumentType.getSqlTypeName() == SqlTypeName.FLOAT
                || argumentType.getSqlTypeName() == SqlTypeName.DOUBLE) {
            type = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        } else {
            type = typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
        return typeFactory.createTypeWithNullability(type, argumentType.isNullable());
    }
}
