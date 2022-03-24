package org.apache.phoenix.parse;

import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.schema.PTableType;

public class TruncateTableStatement extends MutableStatement {
    private final TableName tableName;
    private final PTableType tableType;

    public TruncateTableStatement(TruncateTableStatement truncateTableStatement) {
      this.tableName = truncateTableStatement.tableName;
      this.tableType = truncateTableStatement.tableType;
    }

    protected TruncateTableStatement(TableName tableName, PTableType tableType) {
        this.tableName = tableName;
        this.tableType = PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA.equals(tableName.getSchemaName()) ? PTableType.SYSTEM : tableType;
    }

    public TableName getTableName() {return tableName;}

    public PTableType getTableType() {return tableType;}

    @Override
    public int getBindCount() {return 0;}
}
