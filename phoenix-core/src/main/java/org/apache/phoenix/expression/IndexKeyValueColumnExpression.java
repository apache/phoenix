package org.apache.phoenix.expression;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

public class IndexKeyValueColumnExpression extends KeyValueColumnExpression {
    public IndexKeyValueColumnExpression() {
    }

    public IndexKeyValueColumnExpression(PColumn column) {
        super(column);
    }
    
    @Override
    public String toString() {
        // Translate to the data table column name
        String indexColumnName = Bytes.toString(this.getColumnName());
        String dataFamilyName = IndexUtil.getDataColumnFamilyName(indexColumnName);
        String dataColumnName = IndexUtil.getDataColumnName(indexColumnName);
        return SchemaUtil.getColumnDisplayName(dataFamilyName, dataColumnName);
    }

}
